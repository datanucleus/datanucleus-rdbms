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
2003 Andy Jefferson - coding standards
2005 Andy Jefferson - added "value" field for cases where a parameter is put in a query "result"
2006 Andy Jefferson - remove typeInfo
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;

/**
 * Simple mapping for a java field mapping to a single datastore field.
 */
public abstract class SingleFieldMapping extends JavaTypeMapping
{
    /**
     * Initialize this JavaTypeMapping with the given DatastoreAdapter for the given FieldMetaData.
     * @param table The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     * @param fmd FieldMetaData for the field to be mapped (if any)
     */
    public void initialize(AbstractMemberMetaData fmd, Table table, ClassLoaderResolver clr)
    {
		super.initialize(fmd, table, clr);
		prepareDatastoreMapping();
    }

    /**
     * Method to prepare a field mapping for use in the datastore.
     * This creates the column in the table.
     */
    protected void prepareDatastoreMapping()
    {
        MappingManager mmgr = storeMgr.getMappingManager();
        Column col = mmgr.createColumn(this, getJavaTypeForColumnMapping(0), 0);
        mmgr.createColumnMapping(this, mmd, 0, col);
    }

    /**
     * Accessor for the default length for this type in the datastore (if applicable).
     * @param index requested datastore field index.
     * @return Default length
     */
    public int getDefaultLength(int index)
    {
        return -1;
    }

    /**
     * Accessor for an array of valid values that this type can take.
     * This can be used at the datastore side for restricting the values to be inserted.
     * @param index requested datastore field index.
     * @return The valid values
     */
    public Object[] getValidValues(int index)
    {
        return null;
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForColumnMapping(int index)
    {
        if (getJavaType() == null)
        {
            return null;
        }
        return getJavaType().getName();
    }

    public void setBoolean(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, boolean value)
    {
        getColumnMapping(0).setBoolean(ps, exprIndex[0], value);
    }

    public boolean getBoolean(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getBoolean(resultSet, exprIndex[0]);
    }

    public void setChar(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, char value)
    {
        getColumnMapping(0).setChar(ps, exprIndex[0], value);
    }

    public char getChar(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getChar(resultSet, exprIndex[0]);
    }

    public void setByte(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, byte value)
    {
        getColumnMapping(0).setByte(ps, exprIndex[0], value);
    }

    public byte getByte(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getByte(resultSet, exprIndex[0]);
    }

    public void setShort(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, short value)
    {
        getColumnMapping(0).setShort(ps, exprIndex[0], value);
    }

    public short getShort(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getShort(resultSet, exprIndex[0]);
    }

    public void setInt(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, int value)
    {
        getColumnMapping(0).setInt(ps, exprIndex[0], value);
    }

    public int getInt(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getInt(resultSet, exprIndex[0]);
    }

    public void setLong(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, long value)
    {
        getColumnMapping(0).setLong(ps, exprIndex[0], value);
    }

    public long getLong(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getLong(resultSet, exprIndex[0]);
    }

    public void setFloat(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, float value)
    {
        getColumnMapping(0).setFloat(ps, exprIndex[0], value);
    }

    public float getFloat(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getFloat(resultSet, exprIndex[0]);
    }

    public void setDouble(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, double value)
    {
        getColumnMapping(0).setDouble(ps, exprIndex[0], value);
    }

    public double getDouble(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getDouble(resultSet, exprIndex[0]);
    }

    public void setString(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, String value)
    {
        getColumnMapping(0).setString(ps, exprIndex[0], value);
    }

    public String getString(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return getColumnMapping(0).getString(resultSet, exprIndex[0]);
    }

    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        getColumnMapping(0).setObject(ps, exprIndex[0], value);
    }

    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }
        return getColumnMapping(0).getObject(resultSet, exprIndex[0]);
    }
}