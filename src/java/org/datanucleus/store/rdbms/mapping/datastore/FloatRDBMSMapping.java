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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Mapping of a Float RDBMS type.
 */
public class FloatRDBMSMapping extends DoubleRDBMSMapping
{
    /**
     * Constructor.
     * @param mapping The java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public FloatRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(mapping, storeMgr, col);
	}
    
    public SQLTypeInfo getTypeInfo()
    {
        if (column != null && column.getColumnMetaData().getSqlType() != null)
        {
            return storeMgr.getSQLTypeInfoForJDBCType(Types.FLOAT, column.getColumnMetaData().getSqlType());
        }
        return storeMgr.getSQLTypeInfoForJDBCType(Types.FLOAT);
    }

    public float getFloat(ResultSet rs, int param)
    {
        float value;

        try
        {
            value = rs.getFloat(param);
            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull())
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(LOCALISER_RDBMS.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001","float","" + param, column, e.getMessage()), e);
        }

        return value;
    }
    
    public void setFloat(PreparedStatement ps, int param, float value)
    {
        try
        {
            ps.setFloat(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055002","float","" + value, column, e.getMessage()), e);
        }
    }
    
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getTypeInfo().getDataType());
            }
            else if (value instanceof Integer)
            {
                ps.setFloat(param, ((Integer) value).floatValue());
            }
            else if (value instanceof Long)
            {
                ps.setFloat(param, ((Long) value).floatValue());
            }
            else if (value instanceof Short)
            {
                ps.setFloat(param, ((Short) value).floatValue());
            }
            else if (value instanceof BigInteger)
            {
                ps.setFloat(param, ((BigInteger) value).floatValue());
            }
            else if (value instanceof BigDecimal)
            {
                ps.setFloat(param, ((BigDecimal) value).floatValue());
            }
            else if (value instanceof Character)
            {
                String s = value.toString();
                ps.setFloat(param, s.charAt(0));                  
            }
            else if (value instanceof Float)
            {
                ps.setFloat(param, ((Float) value).floatValue());
            }
            else if (value instanceof Double)
            {
                ps.setDouble(param, ((Double) value).doubleValue());
            }
            else
            {
                ps.setFloat(param, ((Double) value).floatValue());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001", "Object", "" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            float d = rs.getFloat(param);
            if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_INTEGER))
            {
                value = rs.wasNull() ? null : Integer.valueOf((int) d);
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_LONG))
            {
                value = rs.wasNull() ? null : Long.valueOf((long) d);
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_FLOAT))
            {
                value = rs.wasNull() ? null : new Float(d);
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_DOUBLE))
            {
                double dbl = rs.getDouble(param);
                value = rs.wasNull() ? null : Double.valueOf(dbl);
            }
            else
            {
                value = rs.wasNull() ? null : Double.valueOf(d);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055002","Object","" + param, column, e.getMessage()), e);
        }

        return value;
    }    
}