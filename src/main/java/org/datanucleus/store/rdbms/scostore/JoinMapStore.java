/**********************************************************************
Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.Transaction;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.MapMetaData;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedKeyPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.query.StatementParameterMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SelectStatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.types.scostore.CollectionStore;
import org.datanucleus.store.types.scostore.MapStore;
import org.datanucleus.store.types.scostore.SetStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * RDBMS-specific implementation of a {@link MapStore} using join table.
 */
public class JoinMapStore<K, V> extends AbstractMapStore<K, V>
{
    private String putStmt;
    private String updateStmt;
    private String removeStmt;
    private String clearStmt;

    /** JDBC statement to use for retrieving keys of the map (locking). */
    private String getStmtLocked = null;

    /** JDBC statement to use for retrieving keys of the map (not locking). */
    private String getStmtUnlocked = null;

    private StatementClassMapping getMappingDef = null;
    private StatementParameterMapping getMappingParams = null;

    private SetStore keySetStore = null;
    private CollectionStore valueSetStore = null;
    private SetStore entrySetStore = null;
    
    /**
     * when the element mappings columns can't be part of the primary key
     * by datastore limitations like BLOB types. An adapter mapping is used to be a kind of "index"
     */
    protected final JavaTypeMapping adapterMapping;    

    /**
     * Constructor for the backing store of a join map for RDBMS.
     * @param mapTable Join table for the Map
     * @param clr The ClassLoaderResolver
     */
    public JoinMapStore(MapTable mapTable, ClassLoaderResolver clr)
    {
        super(mapTable.getStoreManager(), clr);

        this.mapTable = mapTable;
        setOwner(mapTable.getOwnerMemberMetaData());

        this.ownerMapping = mapTable.getOwnerMapping();
        this.keyMapping = mapTable.getKeyMapping();
        this.valueMapping = mapTable.getValueMapping();
        this.adapterMapping = mapTable.getOrderMapping();

        this.keyType = mapTable.getKeyType();
        this.keysAreEmbedded = mapTable.isEmbeddedKey();
        this.keysAreSerialised = mapTable.isSerialisedKey();
        this.valueType = mapTable.getValueType();
        this.valuesAreEmbedded = mapTable.isEmbeddedValue();
        this.valuesAreSerialised = mapTable.isSerialisedValue();

        keyCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(clr.classForName(keyType), clr);

        Class value_class=clr.classForName(valueType);
        if (ClassUtils.isReferenceType(value_class))
        {
            // Map of reference value types (interfaces/Objects)
            NucleusLogger.PERSISTENCE.warn(Localiser.msg("056066", ownerMemberMetaData.getFullFieldName(), value_class.getName()));
            valueCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForImplementationOfReference(value_class,null,clr);
            if (valueCmd != null)
            {
                this.valueType = value_class.getName();
                // TODO This currently just grabs the cmd of the first implementation. It needs to
                // get the cmds for all implementations, so we can have a handle to all possible elements.
                // This would mean changing the SCO classes to have multiple valueTable/valueMapping etc.
                valueTable = storeMgr.getDatastoreClass(valueCmd.getFullClassName(), clr);
            }
        }
        else
        {
            valueCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(value_class, clr);
            if (valueCmd != null)
            {
                this.valueType = valueCmd.getFullClassName();
                if (valuesAreEmbedded)
                {
                    valueTable = null;
                }
                else
                {
                    valueTable = storeMgr.getDatastoreClass(valueType, clr);
                }
            }
        }

        initialise();

        putStmt = getPutStmt();
        updateStmt = getUpdateStmt();
        removeStmt = getRemoveStmt();
        clearStmt = getClearStmt();
    }

    /**
     * Method to put all elements from a Map into our Map.
     * @param op ObjectProvider for the Map
     * @param m The Map to add
     */
    public void putAll(ObjectProvider op, Map<? extends K, ? extends V> m)
    {
        if (m == null || m.size() == 0)
        {
            return;
        }

        Set<Map.Entry> puts = new HashSet<>();
        Set<Map.Entry> updates = new HashSet<>();

        Iterator i = m.entrySet().iterator();
        while (i.hasNext())
        {
            Map.Entry e = (Map.Entry)i.next();
            Object key = e.getKey();
            Object value = e.getValue();

            // Make sure the related objects are persisted (persistence-by-reachability)
            validateKeyForWriting(op, key);
            validateValueForWriting(op, value);

            // Check if this is a new entry, or an update
            try
            {
                Object oldValue = getValue(op, key);
                if (oldValue != value)
                {
                    updates.add(e);
                }
            }
            catch (NoSuchElementException nsee)
            {
                puts.add(e);
            }
        }

        boolean batched = allowsBatching();

        // Put any new entries
        if (puts.size() > 0)
        {
            try
            {
                ExecutionContext ec = op.getExecutionContext();
                ManagedConnection mconn = storeMgr.getConnection(ec);
                try
                {
                    // Loop through all entries
                    Iterator<Map.Entry> iter = puts.iterator();
                    while (iter.hasNext())
                    {
                        // Add the row to the join table
                        Map.Entry entry = iter.next();
                        internalPut(op, mconn, batched, entry.getKey(), entry.getValue(), (!iter.hasNext()));
                    }
                }
                finally
                {
                    mconn.release();
                }
            }
            catch (MappedDatastoreException e)
            {
                throw new NucleusDataStoreException(Localiser.msg("056016", e.getMessage()), e);
            }
        }

        // Update any changed entries
        if (updates.size() > 0)
        {
            try
            {
                ExecutionContext ec = op.getExecutionContext();
                ManagedConnection mconn = storeMgr.getConnection(ec);
                try
                {
                    // Loop through all entries
                    Iterator<Map.Entry> iter = updates.iterator();
                    while (iter.hasNext())
                    {
                        // Update the row in the join table
                        Map.Entry entry = iter.next();
                        internalUpdate(op, mconn, batched, entry.getKey(), entry.getValue(), !iter.hasNext());
                    }
                }
                finally
                {
                    mconn.release();
                }
            }
            catch (MappedDatastoreException mde)
            {
                throw new NucleusDataStoreException(Localiser.msg("056016", mde.getMessage()), mde);
            }
        }
    }

    /**
     * Method to put an item in the Map.
     * @param op ObjectProvider for the map.
     * @param key The key to store the value against
     * @param value The value to store.
     * @return The value stored.
     **/
    public V put(ObjectProvider op, K key, V value)
    {
        validateKeyForWriting(op, key);
        validateValueForWriting(op, value);

        boolean exists = false;
        V oldValue;
        try
        {
            oldValue = getValue(op, key);
            exists = true;
        }
        catch (NoSuchElementException e)
        {
            oldValue = null;
            exists = false;
        }

        if (oldValue != value)
        {
            // Value changed so update the map
            try
            {
                ExecutionContext ec = op.getExecutionContext();
                ManagedConnection mconn = storeMgr.getConnection(ec);
                try
                {
                    if (exists)
                    {
                        internalUpdate(op, mconn, false, key, value, true);
                    }
                    else
                    {
                        internalPut(op, mconn, false, key, value, true);
                    }
                }
                finally
                {
                    mconn.release();
                }
            }
            catch (MappedDatastoreException e)
            {
                throw new NucleusDataStoreException(Localiser.msg("056016", e.getMessage()), e);
            }
        }

        MapMetaData mapmd = ownerMemberMetaData.getMap();
        if (mapmd.isDependentValue() && !mapmd.isEmbeddedValue() && oldValue != null)
        {
            // Delete the old value if it is no longer contained and is dependent
            if (!containsValue(op, oldValue))
            {
                op.getExecutionContext().deleteObjectInternal(oldValue);
            }
        }

        return oldValue;
    }

    /**
     * Method to remove an entry from the map.
     * @param op ObjectProvider for the map.
     * @param key Key of the entry to remove.
     * @return The value that was removed.
     */
    public V remove(ObjectProvider op, Object key)
    {
        if (!validateKeyForReading(op, key))
        {
            return null;
        }

        V oldValue;
        boolean exists;
        try
        {
            oldValue = getValue(op, key);
            exists = true;
        }
        catch (NoSuchElementException e)
        {
            oldValue = null;
            exists = false;
        }

        ExecutionContext ec = op.getExecutionContext();
        if (exists)
        {
            removeInternal(op, key);
        }

        MapMetaData mapmd = ownerMemberMetaData.getMap();
        ApiAdapter api = ec.getApiAdapter();
        if (mapmd.isDependentKey() && !mapmd.isEmbeddedKey() && api.isPersistable(key))
        {
            // Delete the key if it is dependent
            ec.deleteObjectInternal(key);
        }

        if (mapmd.isDependentValue() && !mapmd.isEmbeddedValue() && api.isPersistable(oldValue))
        {
            if (!containsValue(op, oldValue))
            {
                // Delete the value if it is dependent and is not keyed by another key
                ec.deleteObjectInternal(oldValue);
            }
        }

        return oldValue;
    }

    /**
     * Method to remove an item from the map.
     * @param op ObjectProvider for the map.
     * @param key Key of the item to remove.
     * @return The value that was removed.
     */
    public V remove(ObjectProvider op, Object key, Object oldValue)
    {
        if (!validateKeyForReading(op, key))
        {
            return null;
        }

        ExecutionContext ec = op.getExecutionContext();
        removeInternal(op, key);

        MapMetaData mapmd = ownerMemberMetaData.getMap();
        ApiAdapter api = ec.getApiAdapter();
        if (mapmd.isDependentKey() && !mapmd.isEmbeddedKey() && api.isPersistable(key))
        {
            // Delete the key if it is dependent
            ec.deleteObjectInternal(key);
        }

        if (mapmd.isDependentValue() && !mapmd.isEmbeddedValue() && api.isPersistable(oldValue))
        {
            if (!containsValue(op, oldValue))
            {
                // Delete the value if it is dependent and is not keyed by another key
                ec.deleteObjectInternal(oldValue);
            }
        }

        return (V) oldValue;
    }

    /**
     * Method to clear the map of all values.
     * @param ownerOP ObjectProvider for the map.
     */
    public void clear(ObjectProvider ownerOP)
    {
        Collection dependentElements = null;
        if (ownerMemberMetaData.getMap().isDependentKey() || ownerMemberMetaData.getMap().isDependentValue())
        {
            // Retain the PC dependent keys/values that need deleting after clearing
            dependentElements = new HashSet();
            ApiAdapter api = ownerOP.getExecutionContext().getApiAdapter();
            Iterator iter = entrySetStore().iterator(ownerOP);
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry)iter.next();
                MapMetaData mapmd = ownerMemberMetaData.getMap();
                if (api.isPersistable(entry.getKey()) && mapmd.isDependentKey() && !mapmd.isEmbeddedKey())
                {
                    dependentElements.add(entry.getKey());
                }
                if (api.isPersistable(entry.getValue()) && mapmd.isDependentValue() && !mapmd.isEmbeddedValue())
                {
                    dependentElements.add(entry.getValue());
                }
            }
        }
        clearInternal(ownerOP);

        if (dependentElements != null && dependentElements.size() > 0)
        {
            // Delete all dependent objects
            ownerOP.getExecutionContext().deleteObjects(dependentElements.toArray());
        }
    }

    /**
     * Accessor for the keys in the Map.
     * @return The keys
     **/
    public synchronized SetStore keySetStore()
    {
        if (keySetStore == null)
        {
            keySetStore = new MapKeySetStore((MapTable)mapTable, this, clr);
        }
        return keySetStore;
    }

    /**
     * Accessor for the values in the Map.
     * @return The values.
     */
    public synchronized CollectionStore valueCollectionStore()
    {
        if (valueSetStore == null)
        {
            valueSetStore = new MapValueCollectionStore((MapTable)mapTable, this, clr);
        }
        return valueSetStore;
    }

    /**
     * Accessor for the map entries in the Map.
     * @return The map entries.
     */
    public synchronized SetStore entrySetStore()
    {
        if (entrySetStore == null)
        {
            entrySetStore =  new MapEntrySetStore((MapTable)mapTable, this, clr);
        }
        return entrySetStore;
    }

    public JavaTypeMapping getAdapterMapping()
    {
        return adapterMapping;
    }

    /**
     * Generate statement to add an item to the Map.
     * Adds a row to the link table, linking container with value object.
     * <PRE>
     * INSERT INTO MAPTABLE (VALUECOL, OWNERCOL, KEYCOL)
     * VALUES (?, ?, ?)
     * </PRE>
     * @return Statement to add an item to the Map.
     */
    private String getPutStmt()
    {
        StringBuilder stmt = new StringBuilder("INSERT INTO ");
        stmt.append(mapTable.toString());
        stmt.append(" (");
        for (int i=0; i<valueMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(valueMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
        }

        for (int i=0; i<ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            stmt.append(",");
            stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
        }
        if (adapterMapping != null)
        {
            for (int i=0; i<adapterMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(adapterMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            }
        }

        for (int i=0; i<keyMapping.getNumberOfDatastoreMappings(); i++)
        {
            stmt.append(",");
            stmt.append(keyMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
        }

        stmt.append(") VALUES (");
        for (int i=0; i<valueMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(((AbstractDatastoreMapping)valueMapping.getDatastoreMapping(i)).getInsertionInputParameter());
        }

        for (int i=0; i<ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            stmt.append(",");
            stmt.append(((AbstractDatastoreMapping)ownerMapping.getDatastoreMapping(i)).getInsertionInputParameter());
        }
        if (adapterMapping != null)
        {
            for (int i=0; i<adapterMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(((AbstractDatastoreMapping)adapterMapping.getDatastoreMapping(i)).getInsertionInputParameter());
            }
        }
        for (int i=0; i<keyMapping.getNumberOfDatastoreMappings(); i++)
        {
            stmt.append(",");
            stmt.append(((AbstractDatastoreMapping)keyMapping.getDatastoreMapping(i)).getInsertionInputParameter());
        }
        stmt.append(") ");

        return stmt.toString();
    }

    /**
     * Generate statement to update an item in the Map.
     * Updates the link table row, changing the value object for this key.
     * <PRE>
     * UPDATE MAPTABLE
     * SET VALUECOL=?
     * WHERE OWNERCOL=?
     * AND KEYCOL=?
     * </PRE>
     * @return Statement to update an item in the Map.
     */
    private String getUpdateStmt()
    {
        StringBuilder stmt = new StringBuilder("UPDATE ");
        stmt.append(mapTable.toString());
        stmt.append(" SET ");
        for (int i=0; i<valueMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(valueMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            stmt.append(" = ");
            stmt.append(((AbstractDatastoreMapping)valueMapping.getDatastoreMapping(i)).getUpdateInputParameter());
        }
        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForMapping(stmt, keyMapping, null, false);

        return stmt.toString();
    }

    /**
     * Generate statement to remove an item from the Map.
     * Deletes the link from the join table, leaving the value object in its own table.
     * <PRE>
     * DELETE FROM MAPTABLE
     * WHERE OWNERCOL=?
     * AND KEYCOL=?
     * </PRE>
     * @return Return an item from the Map.
     */
    private String getRemoveStmt()
    {
        StringBuilder stmt = new StringBuilder("DELETE FROM ");
        stmt.append(mapTable.toString());
        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForMapping(stmt, keyMapping, null, false);

        return stmt.toString();
    }

    /**
     * Generate statement to clear the Map.
     * Deletes the links from the join table for this Map, leaving the value objects in their own table(s).
     * <PRE>
     * DELETE FROM MAPTABLE
     * WHERE OWNERCOL=?
     * </PRE>
     * @return Statement to clear the Map.
     */
    private String getClearStmt()
    {
        StringBuilder stmt = new StringBuilder("DELETE FROM ");
        stmt.append(mapTable.toString());
        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);

        return stmt.toString();
    }

    /**
     * Method to retrieve a value from the Map given the key.
     * @param ownerOP ObjectProvider for the owner of the map.
     * @param key The key to retrieve the value for.
     * @return The value for this key
     * @throws NoSuchElementException if the value for the key was not found
     */
    protected V getValue(ObjectProvider ownerOP, Object key)
    throws NoSuchElementException
    {
        if (!validateKeyForReading(ownerOP, key))
        {
            return null;
        }

        ExecutionContext ec = ownerOP.getExecutionContext();
        if (getStmtLocked == null)
        {
            synchronized (this) // Make sure this completes in case another thread needs the same info
            {
                // Generate the statement, and statement mapping/parameter information
                SQLStatement sqlStmt = getSQLStatementForGet(ownerOP);
                getStmtUnlocked = sqlStmt.getSQLText().toSQL();
                sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
                getStmtLocked = sqlStmt.getSQLText().toSQL();
            }
        }

        Transaction tx = ec.getTransaction();
        String stmt = (tx.getSerializeRead() != null && tx.getSerializeRead() ? getStmtLocked : getStmtUnlocked);
        Object value = null;
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Create the statement and supply owner/key params
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                StatementMappingIndex ownerIdx = getMappingParams.getMappingForParameter("owner");
                int numParams = ownerIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerIdx.getMapping().setObject(ec, ps, ownerIdx.getParameterPositionsForOccurrence(paramInstance), ownerOP.getObject());
                }
                StatementMappingIndex keyIdx = getMappingParams.getMappingForParameter("key");
                numParams = keyIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    keyIdx.getMapping().setObject(ec, ps, keyIdx.getParameterPositionsForOccurrence(paramInstance), key);
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        boolean found = rs.next();
                        if (!found)
                        {
                            throw new NoSuchElementException();
                        }

                        if (valuesAreEmbedded || valuesAreSerialised)
                        {
                            int param[] = new int[valueMapping.getNumberOfDatastoreMappings()];
                            for (int i = 0; i < param.length; ++i)
                            {
                                param[i] = i + 1;
                            }

                            if (valueMapping instanceof SerialisedPCMapping ||
                                valueMapping instanceof SerialisedReferenceMapping ||
                                valueMapping instanceof EmbeddedKeyPCMapping)
                            {
                                // Value = Serialised
                                int ownerFieldNumber = ((JoinTable)mapTable).getOwnerMemberMetaData().getAbsoluteFieldNumber();
                                value = valueMapping.getObject(ec, rs, param, ownerOP, ownerFieldNumber);
                            }
                            else
                            {
                                // Value = Non-PC
                                value = valueMapping.getObject(ec, rs, param);
                            }
                        }
                        else if (valueMapping instanceof ReferenceMapping)
                        {
                            // Value = Reference (Interface/Object)
                            int param[] = new int[valueMapping.getNumberOfDatastoreMappings()];
                            for (int i = 0; i < param.length; ++i)
                            {
                                param[i] = i + 1;
                            }
                            value = valueMapping.getObject(ec, rs, param);
                        }
                        else
                        {
                            // Value = PC
                            ResultObjectFactory rof = new PersistentClassROF(storeMgr, valueCmd, getMappingDef, false, null, clr.classForName(valueType));
                            value = rof.getObject(ec, rs);
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
            throw new NucleusDataStoreException(Localiser.msg("056014", stmt), e);
        }
        return (V) value;
    }

    /**
     * Method to return an SQLStatement for retrieving the value for a key.
     * Selects the join table and optionally joins to the value table if it has its own table.
     * @param ownerOP ObjectProvider for the owning object
     * @return The SQLStatement
     */
    protected SelectStatement getSQLStatementForGet(ObjectProvider ownerOP)
    {
        SelectStatement sqlStmt = null;
        ExecutionContext ec = ownerOP.getExecutionContext();

        final ClassLoaderResolver clr = ownerOP.getExecutionContext().getClassLoaderResolver();
        Class valueCls = clr.classForName(this.valueType);
        if (valuesAreEmbedded || valuesAreSerialised)
        {
            // Value is stored in join table
            sqlStmt = new SelectStatement(storeMgr, mapTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);
            sqlStmt.select(sqlStmt.getPrimaryTable(), valueMapping, null);
        }
        else
        {
            // Value is stored in own table
            getMappingDef = new StatementClassMapping();
            if (!valueCmd.getFullClassName().equals(valueCls.getName()))
            {
                valueCls = clr.classForName(valueCmd.getFullClassName());
            }
            UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, valueCls, true, null, null, mapTable, null, valueMapping);
            stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
            getMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
            sqlStmt = stmtGen.getStatement(ec);

            // Select the value field(s)
            SQLTable valueSqlTbl = sqlStmt.getTable(valueTable, sqlStmt.getPrimaryTable().getGroupName());
            if (valueSqlTbl == null)
            {
                // Root value candidate has no table, so try to find a value candidate with a table that exists in this statement
                Collection<String> valueSubclassNames = storeMgr.getSubClassesForClass(valueType, true, clr);
                if (valueSubclassNames != null && !valueSubclassNames.isEmpty())
                {
                    for (String valueSubclassName : valueSubclassNames)
                    {
                        DatastoreClass valueTbl = storeMgr.getDatastoreClass(valueSubclassName, clr);
                        if (valueTbl != null)
                        {
                            valueSqlTbl = sqlStmt.getTable(valueTbl, sqlStmt.getPrimaryTable().getGroupName());
                            if (valueSqlTbl != null)
                            {
                                break;
                            }
                        }
                    }
                }
            }
            SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, getMappingDef, ec.getFetchPlan(), valueSqlTbl, valueCmd, 0);
        }

        // Apply condition on owner field to filter by owner
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        SQLTable ownerSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), ownerMapping);
        SQLExpression ownerExpr = exprFactory.newExpression(sqlStmt, ownerSqlTbl, ownerMapping);
        SQLExpression ownerVal = exprFactory.newLiteralParameter(sqlStmt, ownerMapping, null, "OWNER");
        sqlStmt.whereAnd(ownerExpr.eq(ownerVal), true);

        // Apply condition on key
        if (keyMapping instanceof SerialisedMapping)
        {
            // if the keyMapping contains a BLOB column (or any other column not supported by the database
            // as primary key), uses like instead of the operator OP_EQ (=)
            // in future do not check if the keyMapping is of ObjectMapping, but use the database 
            // adapter to check the data types not supported as primary key
            // if object mapping (BLOB) use like
            SQLExpression keyExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), keyMapping);
            SQLExpression keyVal = exprFactory.newLiteralParameter(sqlStmt, keyMapping, null, "KEY");
            sqlStmt.whereAnd(new org.datanucleus.store.rdbms.sql.expression.BooleanExpression(keyExpr, Expression.OP_LIKE, keyVal), true);
        }
        else
        {
            SQLExpression keyExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), keyMapping);
            SQLExpression keyVal = exprFactory.newLiteralParameter(sqlStmt, keyMapping, null, "KEY");
            sqlStmt.whereAnd(keyExpr.eq(keyVal), true);
        }

        // Input parameter(s) - owner, key
        int inputParamNum = 1;
        StatementMappingIndex ownerIdx = new StatementMappingIndex(ownerMapping);
        StatementMappingIndex keyIdx = new StatementMappingIndex(keyMapping);
        if (sqlStmt.getNumberOfUnions() > 0)
        {
            // Add parameter occurrence for each union of statement
            for (int j=0;j<sqlStmt.getNumberOfUnions()+1;j++)
            {
                int[] ownerPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
                for (int k=0;k<ownerPositions.length;k++)
                {
                    ownerPositions[k] = inputParamNum++;
                }
                ownerIdx.addParameterOccurrence(ownerPositions);

                int[] keyPositions = new int[keyMapping.getNumberOfDatastoreMappings()];
                for (int k=0;k<keyPositions.length;k++)
                {
                    keyPositions[k] = inputParamNum++;
                }
                keyIdx.addParameterOccurrence(keyPositions);
            }
        }
        else
        {
            int[] ownerPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<ownerPositions.length;k++)
            {
                ownerPositions[k] = inputParamNum++;
            }
            ownerIdx.addParameterOccurrence(ownerPositions);

            int[] keyPositions = new int[keyMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<keyPositions.length;k++)
            {
                keyPositions[k] = inputParamNum++;
            }
            keyIdx.addParameterOccurrence(keyPositions);
        }
        getMappingParams = new StatementParameterMapping();
        getMappingParams.addMappingForParameter("owner", ownerIdx);
        getMappingParams.addMappingForParameter("key", keyIdx);

        return sqlStmt;
    }

    protected void clearInternal(ObjectProvider ownerOP)
    {
        try
        {
            ExecutionContext ec = ownerOP.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, clearStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                    sqlControl.executeStatementUpdate(ec, mconn, clearStmt, ps, true);
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
            throw new NucleusDataStoreException(Localiser.msg("056013",clearStmt),e);
        }
    }

    protected void removeInternal(ObjectProvider op, Object key)
    {
        ExecutionContext ec = op.getExecutionContext();
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, removeStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    BackingStoreHelper.populateKeyInStatement(ec, ps, key, jdbcPosition, keyMapping);
                    sqlControl.executeStatementUpdate(ec, mconn, removeStmt, ps, true);
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
            throw new NucleusDataStoreException(Localiser.msg("056012",removeStmt),e);
        }
    }

    /**
     * Method to process an "update" statement (where the key already has a value in the join table).
     * @param ownerOP ObjectProvider for the owner
     * @param conn The Connection
     * @param batched Whether we are batching it
     * @param key The key
     * @param value The new value
     * @param executeNow Whether to execute the statement now or wait til any batch
     * @throws MappedDatastoreException Thrown if an error occurs
     */
    protected void internalUpdate(ObjectProvider ownerOP, ManagedConnection conn, boolean batched, Object key, Object value, boolean executeNow) 
    throws MappedDatastoreException
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        try 
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, updateStmt, false);
            try
            {
                int jdbcPosition = 1;
                if (valueMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateValueInStatement(ec, ps, value, jdbcPosition, valueMapping);
                }
                else
                {
                    jdbcPosition = BackingStoreHelper.populateEmbeddedValueFieldsInStatement(ownerOP, value, ps, jdbcPosition, (JoinTable)mapTable, this);
                }
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateKeyInStatement(ec, ps, key, jdbcPosition, keyMapping);

                if (batched)
                {
                    ps.addBatch();
                }
                else
                {
                    sqlControl.executeStatementUpdate(ec, conn, updateStmt, ps, true);
                }
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
        catch (SQLException e)
        {
            throw new MappedDatastoreException(getUpdateStmt(), e);
        }
    }

    /**
     * Method to process a "put" statement (where the key has no value in the join table).
     * @param ownerOP ObjectProvider for the owner
     * @param conn The Connection
     * @param batched Whether we are batching it
     * @param key The key
     * @param value The value
     * @param executeNow Whether to execute the statement now or wait til batching
     * @return The return codes from any executed statement
     * @throws MappedDatastoreException Thrown if an error occurs
     */
    protected int[] internalPut(ObjectProvider ownerOP, ManagedConnection conn, boolean batched, Object key, Object value, boolean executeNow)
    throws MappedDatastoreException
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, putStmt, false);
            try
            {
                int jdbcPosition = 1;
                if (valueMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateValueInStatement(ec, ps, value, jdbcPosition, valueMapping);
                }
                else
                {
                    jdbcPosition = BackingStoreHelper.populateEmbeddedValueFieldsInStatement(ownerOP, value, ps, jdbcPosition, (JoinTable)mapTable, this);
                }
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                if (adapterMapping != null)
                {
                    // Only set the adapter mapping if we have a new object
                    long nextIDAdapter = getNextIDForAdapterColumn(ownerOP);
                    adapterMapping.setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, adapterMapping), Long.valueOf(nextIDAdapter));
                    jdbcPosition += adapterMapping.getNumberOfDatastoreMappings();
                }
                jdbcPosition = BackingStoreHelper.populateKeyInStatement(ec, ps, key, jdbcPosition, keyMapping);

                // Execute the statement
                return sqlControl.executeStatementUpdate(ec, conn, putStmt, ps, true);
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
        catch (SQLException e)
        {
            throw new MappedDatastoreException(getPutStmt(), e);
        }
    }

    /**
     * Accessor for the higher id when elements primary key can't be part of
     * the primary key by datastore limitations like BLOB types can't be primary keys.
     * @param op ObjectProvider for container
     * @return The next id
     */
    private int getNextIDForAdapterColumn(ObjectProvider op)
    {
        int nextID;
        try
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                String stmt = getMaxAdapterColumnIdStmt();
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);

                try
                {
                    int jdbcPosition = 1;
                    BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        if (!rs.next())
                        {
                            nextID = 1;
                        }
                        else
                        {
                            nextID = rs.getInt(1)+1;
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
            throw new NucleusDataStoreException(Localiser.msg("056020",getMaxAdapterColumnIdStmt()),e);
        }

        return nextID;
    }
    /**
     * Generate statement for obtaining the maximum id.
     * <PRE>
     * SELECT MAX(SCOID) FROM MAPTABLE
     * WHERE OWNERCOL=?
     * </PRE>
     * @return The Statement returning the higher id
     */
    private String getMaxAdapterColumnIdStmt()
    {
        StringBuilder stmt = new StringBuilder("SELECT MAX(" + adapterMapping.getDatastoreMapping(0).getColumn().getIdentifier().toString() + ")");
        stmt.append(" FROM ");
        stmt.append(mapTable.toString());
        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);

        return stmt.toString();
    }
}