/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.java;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.Localiser;

/**
 * Mapping where the member has its value converted to/from some storable datastore type using a TypeConverter to multiple columns.
 */
public class TypeConverterMultiMapping extends SingleFieldMultiMapping
{
    TypeConverter converter;

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#initialize(org.datanucleus.store.rdbms.RDBMSStoreManager, java.lang.String)
     */
    @Override
    public void initialize(RDBMSStoreManager storeMgr, String type)
    {
        super.initialize(storeMgr, type);
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        Class fieldType = clr.classForName(type);
        converter = storeMgr.getNucleusContext().getTypeManager().getDefaultTypeConverterForType(fieldType);
        if (converter == null)
        {
            throw new NucleusUserException("Unable to find TypeConverter for converting " + fieldType + " to String");
        }

        if (!(converter instanceof MultiColumnConverter))
        {
            throw new NucleusUserException("Not able to use " + getClass().getName() + " for java type " + type + 
                " since provided TypeConverter " + converter + " does not implement MultiColumnConverter");
        }

        // Add columns for this converter
        MultiColumnConverter multiConv = (MultiColumnConverter)converter;
        Class[] colTypes = multiConv.getDatastoreColumnTypes();
        for (int i=0;i<colTypes.length;i++)
        {
            addColumns(colTypes[i].getName());
        }
    }

    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
        this.initialize(mmd, table, clr, null);
    }

    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr, TypeConverter conv)
    {
        super.initialize(mmd, table, clr);

        if (mmd.getTypeConverterName() != null)
        {
            // Use specified converter (if found)
            converter = table.getStoreManager().getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
            if (converter == null)
            {
                throw new NucleusUserException(Localiser.msg("044062", mmd.getFullFieldName(), mmd.getTypeConverterName()));
            }
        }
        else if (conv != null)
        {
            converter = conv;
        }
        else
        {
            throw new NucleusUserException("Unable to initialise mapping of type " + getClass().getName() + " for field " + mmd.getFullFieldName() + " since no TypeConverter was provided");
        }

        if (!(converter instanceof MultiColumnConverter))
        {
            throw new NucleusUserException("Not able to use " + getClass().getName() + " for field " + mmd.getFullFieldName() + 
                " since provided TypeConverter " + converter + " does not implement MultiColumnConverter");
        }

        // Add columns for this converter
        MultiColumnConverter multiConv = (MultiColumnConverter)converter;
        Class[] colTypes = multiConv.getDatastoreColumnTypes();
        for (int i=0;i<colTypes.length;i++)
        {
            addColumns(colTypes[i].getName());
        }
    }

    public TypeConverter getTypeConverter()
    {
        return converter;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.JavaTypeMapping#getJavaType()
     */
    @Override
    public Class getJavaType()
    {
        return mmd != null ? mmd.getType() : storeMgr.getNucleusContext().getClassLoaderResolver(null).classForName(type);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.SingleFieldMapping#setObject(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], java.lang.Object)
     */
    @Override
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        if (exprIndex == null)
        {
            return;
        }

        Object colArray = converter.toDatastoreType(value);
        if (colArray == null)
        {
            for (int i=0;i<exprIndex.length;i++)
            {
                getDatastoreMapping(i).setObject(ps, exprIndex[i], null);
            }
        }
        else
        {
            for (int i=0;i<exprIndex.length;i++)
            {
                Object colValue = Array.get(colArray, i);
                if (colValue == null)
                {
                    getDatastoreMapping(i).setObject(ps, exprIndex[i], null);
                }
                else
                {
                    Class colValCls = colValue.getClass();
                    if (colValCls == int.class || colValCls == Integer.class)
                    {
                        getDatastoreMapping(i).setInt(ps, exprIndex[i], (Integer)colValue);
                    }
                    else if (colValCls == long.class || colValCls == Long.class)
                    {
                        getDatastoreMapping(i).setLong(ps, exprIndex[i], (Long)colValue);
                    }
                    else if (colValCls == double.class || colValCls == Double.class)
                    {
                        getDatastoreMapping(i).setDouble(ps, exprIndex[i], (Double)colValue);
                    }
                    else if (colValCls == float.class || colValCls == Float.class)
                    {
                        getDatastoreMapping(i).setFloat(ps, exprIndex[i], (Float)colValue);
                    }
                    // TODO Support other types
                    else if (colValCls == String.class)
                    {
                        getDatastoreMapping(i).setString(ps, exprIndex[i], (String)colValue);
                    }
                    else
                    {
                        getDatastoreMapping(i).setObject(ps, exprIndex[i], colValue);
                    }
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.SingleFieldMapping#getObject(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        Object valuesArr = null;
        Class[] colTypes = ((MultiColumnConverter)converter).getDatastoreColumnTypes();
        if (colTypes[0] == int.class)
        {
            valuesArr = new int[exprIndex.length];
        }
        else if (colTypes[0] == long.class)
        {
            valuesArr = new long[exprIndex.length];
        }
        else if (colTypes[0] == double.class)
        {
            valuesArr = new double[exprIndex.length];
        }
        else if (colTypes[0] == float.class)
        {
            valuesArr = new double[exprIndex.length];
        }
        else if (colTypes[0] == String.class)
        {
            valuesArr = new String[exprIndex.length];
        }
        // TODO Support other types
        else
        {
            valuesArr = new Object[exprIndex.length];
        }
        boolean isNull = true;
        for (int i=0;i<exprIndex.length;i++)
        {
            String colJavaType = getJavaTypeForDatastoreMapping(i);
            if (colJavaType.equals("int") || colJavaType.equals("java.lang.Integer"))
            {
                Array.set(valuesArr, i, getDatastoreMapping(i).getInt(resultSet, exprIndex[i]));
            }
            else if (colJavaType.equals("long") || colJavaType.equals("java.lang.Long"))
            {
                Array.set(valuesArr, i, getDatastoreMapping(i).getLong(resultSet, exprIndex[i]));
            }
            else if (colJavaType.equals("double") || colJavaType.equals("java.lang.Double"))
            {
                Array.set(valuesArr, i, getDatastoreMapping(i).getDouble(resultSet, exprIndex[i]));
            }
            else if (colJavaType.equals("float") || colJavaType.equals("java.lang.Float"))
            {
                Array.set(valuesArr, i, getDatastoreMapping(i).getFloat(resultSet, exprIndex[i]));
            }
            // TODO Support other types
            else if (colJavaType.equals("java.lang.String"))
            {
                Array.set(valuesArr, i, getDatastoreMapping(i).getString(resultSet, exprIndex[i]));
            }
            else
            {
                Array.set(valuesArr, i, getDatastoreMapping(i).getObject(resultSet, exprIndex[i]));
            }
            if (isNull && Array.get(valuesArr, i) != null)
            {
                isNull = false;
            }
        }
        if (isNull)
        {
            return null;
        }
        return converter.toMemberType(valuesArr);
    }
}