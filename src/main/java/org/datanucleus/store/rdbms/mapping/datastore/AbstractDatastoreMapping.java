/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved. 
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
2004 Andy Jefferson - added getTypeInfo method
2005 Andy Jefferson - changed to use StoreManager instead of DatabaseAdapter
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.exceptions.UnsupportedDataTypeException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Implementation of the mapping of an RDBMS type.
 */
public abstract class AbstractDatastoreMapping implements DatastoreMapping
{
    /** Mapping of the Java type. */
    protected final JavaTypeMapping mapping;

    /** Store Manager to use for mapping. */
    protected final RDBMSStoreManager storeMgr;

    /** The RDBMS Column being persisted to. */
    protected Column column;

    /**
     * Create a new Mapping with the given DatabaseAdapter for the given type.
     * @param storeMgr The Store Manager that this Mapping should use.
     * @param mapping Mapping for the underlying java type. This can be null on an "unmapped column".
     */
    protected AbstractDatastoreMapping(RDBMSStoreManager storeMgr, JavaTypeMapping mapping)
    {
        this.mapping = mapping;
        if (mapping != null)
        {
            // Register this datastore mapping with the owning JavaTypeMapping
            mapping.addDatastoreMapping(this);
        }

        this.storeMgr = storeMgr;
    }

    /**
     * Accessor for the java type mapping
     * @return The java type mapping used
     */
    public JavaTypeMapping getJavaTypeMapping()
    {
        return mapping;
    }

    /**
     * Convenience to access the datastore adapter.
     * @return The adapter in use
     */
    protected DatastoreAdapter getDatastoreAdapter()
    {
        return storeMgr.getDatastoreAdapter();
    }

    /**
     * Accessor for the (SQL) type info for this datastore type.
     * @return The type info
     */
    public abstract SQLTypeInfo getTypeInfo();

    /**
     * Accessor for whether the mapping is nullable.
     * @return Whether it is nullable
     */
    public boolean isNullable()
    {
        if (column != null)
        {
            return column.isNullable();
        }
        return true;
    }

    /**
     * Whether this mapping is included in the fetch statement.
     * @return Whether to include in fetch statement
     */
    public boolean includeInFetchStatement()
    {
        return true;
    }

    /**
     * Accessor for whether this mapping requires values inserting on an INSERT.
     * @return Whether values are to be inserted into this mapping on an INSERT
     */
    public boolean insertValuesOnInsert()
    {
        return getInsertionInputParameter().indexOf('?') > -1;
    }

    /**
     * Accessor for the string to put in any retrieval datastore statement for this field. In RDBMS, this is
     * typically a ? to be used in JDBC statements.
     * @return The input parameter
     */
    public String getInsertionInputParameter()
    {
        return column.getWrapperFunction(Column.WRAPPER_FUNCTION_INSERT);
    }

    /**
     * Accessor for the string to put in any update datastore statements for this field. In RDBMS, this is
     * typically a ? to be used in JDBC statements.
     * @return The input parameter.
     */
    public String getUpdateInputParameter()
    {
        return column.getWrapperFunction(Column.WRAPPER_FUNCTION_UPDATE);
    }

    /**
     * Accessor for the column
     * @return The column
     */
    public Column getColumn()
    {
        return column;
    }

    /**
     * Sets the TypeInfo for the columns of the Mapping. Mappings using two or more columns using different
     * TypeInfo(s) should overwrite this method to appropriate set the TypeInfo (SQL type) for all the columns
     */
    protected void initTypeInfo()
    {
        SQLTypeInfo typeInfo = getTypeInfo();
        if (typeInfo == null)
        {
            throw new UnsupportedDataTypeException(Localiser.msg("055000", column));
        }

        if (column != null)
        {
            column.setTypeInfo(typeInfo);
        }
    }

    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof AbstractDatastoreMapping))
        {
            return false;
        }

        AbstractDatastoreMapping cm = (AbstractDatastoreMapping) obj;

        return getClass().equals(cm.getClass()) && storeMgr.equals(cm.storeMgr) && (column == null ? cm.column == null : column.equals(cm.column));
    }

    public int hashCode()
    {
        return storeMgr.hashCode() ^ (column == null ? 0 : column.hashCode());
    }

    /**
     * Utility to output any error message.
     * @param method The method that failed.
     * @param position The position of the column
     * @param e The exception
     * @return The localised failure message
     */
    protected String failureMessage(String method, int position, Exception e)
    {
        return Localiser.msg("041050", getClass().getName() + "." + method, position, column, e.getMessage());
    }

    /**
     * Utility to output any error message.
     * @param method The method that failed.
     * @param value Value at the position
     * @param e The exception
     * @return The localised failure message
     */
    protected String failureMessage(String method, Object value, Exception e)
    {
        return Localiser.msg("041050", getClass().getName() + "." + method, value, column, e.getMessage());
    }

    public void setBoolean(PreparedStatement ps, int exprIndex, boolean value)
    {
        throw new NucleusException(failureMessage("setBoolean")).setFatal();
    }

    public boolean getBoolean(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getBoolean")).setFatal();
    }

    public void setChar(PreparedStatement ps, int exprIndex, char value)
    {
        throw new NucleusException(failureMessage("setChar")).setFatal();
    }

    public char getChar(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getChar")).setFatal();
    }

    public void setByte(PreparedStatement ps, int exprIndex, byte value)
    {
        throw new NucleusException(failureMessage("setByte")).setFatal();
    }

    public byte getByte(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getByte")).setFatal();
    }

    public void setShort(PreparedStatement ps, int exprIndex, short value)
    {
        throw new NucleusException(failureMessage("setShort")).setFatal();
    }

    public short getShort(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getShort")).setFatal();
    }

    public void setInt(PreparedStatement ps, int exprIndex, int value)
    {
        throw new NucleusException(failureMessage("setInt")).setFatal();
    }

    public int getInt(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getInt")).setFatal();
    }

    public void setLong(PreparedStatement ps, int exprIndex, long value)
    {
        throw new NucleusException(failureMessage("setLong")).setFatal();
    }

    public long getLong(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getLong")).setFatal();
    }

    public void setFloat(PreparedStatement ps, int exprIndex, float value)
    {
        throw new NucleusException(failureMessage("setFloat")).setFatal();
    }

    public float getFloat(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getFloat")).setFatal();
    }

    public void setDouble(PreparedStatement ps, int exprIndex, double value)
    {
        throw new NucleusException(failureMessage("setDouble")).setFatal();
    }

    public double getDouble(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getDouble")).setFatal();
    }

    public void setString(PreparedStatement ps, int exprIndex, String value)
    {
        throw new NucleusException(failureMessage("setString")).setFatal();
    }

    public String getString(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getString")).setFatal();
    }

    public void setObject(PreparedStatement ps, int exprIndex, Object value)
    {
        throw new NucleusException(failureMessage("setObject")).setFatal();
    }

    public Object getObject(ResultSet resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getObject")).setFatal();
    }

    /**
     * Accessor for whether the mapping is decimal-based.
     * @return Whether the mapping is decimal based
     */
    public boolean isDecimalBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is integer-based.
     * @return Whether the mapping is integer based
     */
    public boolean isIntegerBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is string-based.
     * @return Whether the mapping is string based
     */
    public boolean isStringBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is bit-based.
     * @return Whether the mapping is bit based
     */
    public boolean isBitBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is boolean-based.
     * @return Whether the mapping is boolean based
     */
    public boolean isBooleanBased()
    {
        return false;
    }

    /**
     * Utility to output any error message.
     * @param method The method that failed.
     * @return The localised failure message
     **/
    protected String failureMessage(String method)
    {
        return Localiser.msg("041005", getClass().getName(), method, mapping.getMemberMetaData().getFullFieldName());
    }
}