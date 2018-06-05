/**********************************************************************
Copyright (c) 2015 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.mapping.column;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.Localiser;

/**
 * Mapping of an ARRAY column.
 * Note that this is designed around how PostgreSQL handles arrays, and is largely limited to what is available for that datastore.
 */
public class ArrayColumnMapping extends AbstractColumnMapping
{
    /** SQL type for the element. */
    String arrayElemSqlType = null;

    public ArrayColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column column)
    {
		super(storeMgr, mapping);
		this.column = column;
		initialize();

		// PostgreSQL will have a type like "INT ARRAY" or "TEXT ARRAY", so this finds the element SQL type
		String arrayTypeName = column.getTypeName();
		if (arrayTypeName.indexOf("array") > 0)
		{
		    arrayElemSqlType = arrayTypeName.substring(0, arrayTypeName.indexOf("array")).trim();
		}
		else if (arrayTypeName.indexOf("ARRAY") > 0)
		{
            arrayElemSqlType = arrayTypeName.substring(0, arrayTypeName.indexOf("ARRAY")).trim();
		}
		else
		{
		    throw new NucleusUserException("Do not support handling of type=" + arrayTypeName + " for member=" + mapping.getMemberMetaData().getFullFieldName());
		}
	}

    private void initialize()
    {
		initTypeInfo();
    }

    public int getJDBCType()
    {
        return Types.ARRAY;
    }

    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getJDBCType());
            }
            else
            {
                Array array = null;
                if (value.getClass().isArray())
                {
                    // Convert the array into a java.sql.Array
                    int numElems = java.lang.reflect.Array.getLength(value);
                    Object[] elems = new Object[numElems];
                    for (int i=0;i<numElems;i++)
                    {
                        elems[i] = java.lang.reflect.Array.get(value, i);
                    }
                    array = ps.getConnection().createArrayOf(arrayElemSqlType, elems);
                }
                else if (value instanceof Collection)
                {
                    // Convert the collection into a java.sql.Array
                    Collection coll = (Collection)value;
                    Object[] elems = new Object[coll.size()];
                    int i = 0;
                    for (Object elem : coll)
                    {
                        elems[i++] = elem;
                    }
                    array = ps.getConnection().createArrayOf(arrayElemSqlType, elems);
                }
                else
                {
                    throw new NucleusUserException("We do not support persisting values of type " + value.getClass().getName() + " as an ARRAY." +
                        " Member=" + mapping.getMemberMetaData().getFullFieldName());
                }

                ps.setArray(param, array);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","Object","" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value = null;

        try
        {
            Array arr = rs.getArray(param);
            if (!rs.wasNull())
            {
                Object javaArray = arr.getArray();
                int length = java.lang.reflect.Array.getLength(javaArray);

                AbstractMemberMetaData mmd = mapping.getMemberMetaData();
                if (mmd.getType().isArray())
                {
                    // Copy in to an array of the same type as the member
                    value = java.lang.reflect.Array.newInstance(mmd.getType().getComponentType(), length);
                    for (int i=0;i<length;i++)
                    {
                        java.lang.reflect.Array.set(value, i, java.lang.reflect.Array.get(javaArray, i));
                    }
                }
                else if (Collection.class.isAssignableFrom(mmd.getType()))
                {
                    Collection<Object> coll;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                        coll = (Collection<Object>) instanceType.newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    for (int i=0;i<length;i++)
                    {
                        coll.add(java.lang.reflect.Array.get(javaArray, i));
                    }
                    value = coll;
                }
                else
                {
                    throw new NucleusUserException("We do not support retrieving values of type " + mmd.getTypeName() + " as an ARRAY." +
                            " Member=" + mapping.getMemberMetaData().getFullFieldName());
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Object","" + param, column, e.getMessage()), e);
        }

        return value;
    }
}