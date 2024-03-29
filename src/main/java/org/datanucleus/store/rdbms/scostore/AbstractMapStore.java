/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved. 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 

Contributors:
2003 Erik Bengtson - removed unused import
2003 Andy Jefferson - coding standards
2003 Andy Jefferson - addition of getGetStatement for inherited values
2004 Andy Jefferson - addition of query methods
2004 Andy Jefferson - moved statements from subclasses
2005 Andy Jefferson - allow for embedded keys/values
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.NoSuchElementException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.scostore.MapStore;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Abstract representation of the backing store for a Map.
 */
public abstract class AbstractMapStore<K, V> extends BaseContainerStore implements MapStore<K, V>
{
    /** Flag to set whether the iterator statement will use a discriminator or not. */
    protected boolean iterateUsingDiscriminator = false;

    /** Metadata for the keys (if persistable). */
    protected AbstractClassMetaData keyCmd;

    /** Metadata for the values (if persistable). */
    protected AbstractClassMetaData valueCmd;

    /** Mapping to the key from the mapTable. */
    protected JavaTypeMapping keyMapping;

    /** Mapping to the value from the mapTable. */
    protected JavaTypeMapping valueMapping;

    /** Type of the key. */
    protected String keyType;

    /** Type of the value. */
    protected String valueType;

    /** Whether the keys are embedded. */
    protected boolean keysAreEmbedded;

    /** Whether the keys are serialised. */
    protected boolean keysAreSerialised;

    /** Whether the values are embedded. */
    protected boolean valuesAreEmbedded;

    /** Whether the values are serialised. */
    protected boolean valuesAreSerialised;

    protected String containsValueStmt;

    public AbstractMapStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    public JavaTypeMapping getValueMapping()
    {
        return valueMapping;
    }

    public JavaTypeMapping getKeyMapping()
    {
        return keyMapping;
    }

    public AbstractClassMetaData getKeyClassMetaData()
    {
        return keyCmd;
    }

    public AbstractClassMetaData getValueClassMetaData()
    {
        return valueCmd;
    }

    /**
     * Accessor for whether the keys are embedded or not.
     * If they are PC instances then returns false;
     * @return Whether the keys are embedded
     */
    @Override
    public boolean keysAreEmbedded()
    {
        return keysAreEmbedded;
    }

    /**
     * Accessor for whether the keys are serialised or not.
     * If they are PC instances then returns false;
     * @return Whether the keys are serialised
     */
    @Override
    public boolean keysAreSerialised()
    {
        return keysAreSerialised;
    }

    /**
     * Accessor for whether the values are embedded or not.
     * If they are PC instances then returns false;
     * @return Whether the values are embedded
     */
    @Override
    public boolean valuesAreEmbedded()
    {
        return valuesAreEmbedded;
    }

    /**
     * Accessor for whether the values are serialised or not.
     * If they are PC instances then returns false;
     * @return Whether the values are serialised
     */
    @Override
    public boolean valuesAreSerialised()
    {
        return valuesAreSerialised;
    }

    @Override
    public boolean containsKey(DNStateManager sm, Object key)
    {
        if (key == null)
        {
            // nulls not allowed
            return false;
        }
        try
        {
            getValue(sm, key);
            return true;
        }
        catch (NoSuchElementException e)
        {
            return false;
        }
    }

    @Override
    public boolean containsValue(DNStateManager sm, Object value)
    {
        if (value == null)
        {
            // nulls not allowed
            return false;
        }
        if (!validateValueForReading(sm, value))
        {
            return false;
        }

        boolean exists = false;
        try
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, containsValueStmt);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                    BackingStoreHelper.populateValueInStatement(ec, ps, value, jdbcPosition, getValueMapping());

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, containsValueStmt, ps);
                    try
                    {
                        if (rs.next())
                        {
                            exists = true;
                        }

                        JDBCUtils.logWarnings(rs);
                    }
                    finally
                    {
                        rs.close();
                    }
                }
                finally
                {
                    sqlControl.closeStatement(mconn, ps);
                }
            }
            finally
            {
                mconn.release();
            }
        }
        catch (SQLException e)
        {
            NucleusLogger.DATASTORE_RETRIEVE.warn("Exception during backing store select", e);
            throw new NucleusDataStoreException(Localiser.msg("056019",containsValueStmt), e);
        }

        return exists;
    }

    @Override
    public V get(DNStateManager sm, Object key)
    {
        try
        {
            return getValue(sm, key);
        }
        catch (NoSuchElementException e)
        {
            return null;
        }
    }

    @Override
    public void putAll(DNStateManager sm, Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
        {
            put(sm, entry.getKey(), entry.getValue());
        }
    }

    // --------------------------- Utility Methods -----------------------------
 
    /**
     * Utility to validate the type of a key for storing in the Map.
     * @param clr The ClassLoaderResolver
     * @param key The key to check.
     */
    protected void validateKeyType(ClassLoaderResolver clr, Object key)
    {
        if (key == null && !allowNulls)
        {
            // Nulls not allowed and key is null
            throw new NullPointerException(Localiser.msg("056062"));
        }

        if (key != null && !clr.isAssignableFrom(keyType, key.getClass()))
        {
            throw new ClassCastException(Localiser.msg("056064", key.getClass().getName(), keyType));
        }
    }

    /**
     * Utility to validate the type of a value for storing in the Map.
     * @param clr The ClassLoaderResolver
     * @param value The value to check.
     */
    protected void validateValueType(ClassLoaderResolver clr, Object value)
    {
        if (value == null && !allowNulls)
        {
            // Nulls not allowed and value is null
            throw new NullPointerException(Localiser.msg("056063"));
        }
        
        if (value != null && !clr.isAssignableFrom(valueType, value.getClass()))
        {
            throw new ClassCastException(Localiser.msg("056065", value.getClass().getName(), valueType));
        }
    }

    /**
     * Utility to validate a key is ok for reading.
     * @param sm StateManager for the map.
     * @param key The key to check.
     * @return Whether it is validated. 
     */
    protected boolean validateKeyForReading(DNStateManager sm, Object key)
    {
        validateKeyType(sm.getExecutionContext().getClassLoaderResolver(), key);

        if (!keysAreEmbedded && !keysAreSerialised)
        {
            ExecutionContext ec = sm.getExecutionContext();
            if (key!=null && (!ec.getApiAdapter().isPersistent(key) ||
                ec != ec.getApiAdapter().getExecutionContext(key)) && !ec.getApiAdapter().isDetached(key))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Utility to validate a value is ok for reading.
     * @param sm StateManager for the map.
     * @param value The value to check.
     * @return Whether it is validated.
     */
    protected boolean validateValueForReading(DNStateManager sm, Object value)
    {
        validateValueType(sm.getExecutionContext().getClassLoaderResolver(), value);

        if (!valuesAreEmbedded && !valuesAreSerialised)
        {
            ExecutionContext ec = sm.getExecutionContext();
            if (value != null && (!ec.getApiAdapter().isPersistent(value) ||
                ec != ec.getApiAdapter().getExecutionContext(value)) && !ec.getApiAdapter().isDetached(value))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Utility to validate a key is ok for writing (present in the datastore).
     * @param ownerSM StateManager for the owner of the map
     * @param key The key to check.
     */
    protected void validateKeyForWriting(DNStateManager ownerSM, Object key)
    {
        // TODO Pass in cascade flag and if key not present then throw exception
        ExecutionContext ec = ownerSM.getExecutionContext();
        validateKeyType(ec.getClassLoaderResolver(), key);
        if (!keysAreEmbedded && !keysAreSerialised)
        {
            SCOUtils.validateObjectForWriting(ec, key, null);
        }
    }

    /**
     * Utility to validate a value is ok for writing (present in the datastore).
     * @param ownerSM StateManager for the owner of the map
     * @param value The value to check.
     */
    protected void validateValueForWriting(DNStateManager ownerSM, Object value)
    {
        // TODO Pass in cascade flag and if value not present then throw exception
        ExecutionContext ec = ownerSM.getExecutionContext();
        validateValueType(ec.getClassLoaderResolver(), value);
        if (!valuesAreEmbedded && !valuesAreSerialised)
        {
            SCOUtils.validateObjectForWriting(ec, value, null);
        }
    }

    /**
     * Method to retrieve a value from the Map given the key.
     * @param sm StateManager for the map.
     * @param key The key to retrieve the value for.
     * @return The value for this key
     * @throws NoSuchElementException if the value for the key was not found
     */
    protected abstract V getValue(DNStateManager sm, Object key)
    throws NoSuchElementException;

    /**
     * Generate statement to check if a value is contained in the Map.
     * <PRE>
     * SELECT OWNERCOL
     * FROM MAPTABLE
     * WHERE OWNERCOL=? AND VALUECOL = ?
     * </PRE>
     * @param ownerMapping the owner mapping
     * @param valueMapping the value mapping
     * @param mapTable the map table
     * @return Statement to check if a value is contained in the Map.
     */
    protected static String getContainsValueStmt(JavaTypeMapping ownerMapping, JavaTypeMapping valueMapping, Table mapTable)
    {
        StringBuilder stmt = new StringBuilder("SELECT ");
        for (int i=0; i<ownerMapping.getNumberOfColumnMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(ownerMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
        }
        stmt.append(" FROM ");
        stmt.append(mapTable.toString());
        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForMapping(stmt, valueMapping, null, false);

        return stmt.toString();
    }
}