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
2004 Andy Jefferson - localised messages
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
import org.datanucleus.util.Localiser;

/**
 * Mapping of a Decimal RDBMS type.
 */
public class DecimalRDBMSMapping extends AbstractDatastoreMapping
{
    private static final int INT_MAX_DECIMAL_DIGITS = 10;
    private static final int LONG_MAX_DECIMAL_DIGITS = 19;

    public DecimalRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(storeMgr, mapping);
		column = col;
		initialize();
	}

    /**
     * Initialise the mapping, setting any default precision.
     */
    private void initialize()
    {
        // If the column has no precision specified, set its size.
        // If the user has already set their precision we do nothing here since they
        // want to control it.
        if (column != null && column.getColumnMetaData().getLength() == null)
        {
            // In case the default DECIMAL precision is less than the number of
            // digits we need, set it manually
            if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_INTEGER))
            {
                column.getColumnMetaData().setLength(INT_MAX_DECIMAL_DIGITS);
                column.checkDecimal();
            }
            else
            {
                column.getColumnMetaData().setLength(Math.min(getTypeInfo().getPrecision(), LONG_MAX_DECIMAL_DIGITS));
                column.checkDecimal();
            }
        }
        initTypeInfo();
    }

    /**
     * Accessor for whether the mapping is decimal-based.
     * @return Whether the mapping is decimal based
     */
    public boolean isDecimalBased()
    {
        return true;
    }

    public SQLTypeInfo getTypeInfo()
    {
        if (column != null && column.getColumnMetaData().getSqlType() != null)
        {
            return storeMgr.getSQLTypeInfoForJDBCType(Types.DECIMAL, column.getColumnMetaData().getSqlType());
        }
        return storeMgr.getSQLTypeInfoForJDBCType(Types.DECIMAL);
    }

    public void setDouble(PreparedStatement ps, int param, double value)
    {
        try
        {
            ps.setDouble(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","double","" + value, column, e.getMessage()), e);
        }
    }
    
    public void setFloat(PreparedStatement ps, int param, float value)
    {
        try
        {
            ps.setDouble(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","float","" + value, column, e.getMessage()), e);
        }
    }    
    
    public void setInt(PreparedStatement ps, int param, int value)
    {
        try
        {
            ps.setInt(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","int","" + value, column, e.getMessage()), e);
        }
    }

    public double getDouble(ResultSet rs, int param)
    {
        double value;

        try
        {
            value = rs.getDouble(param);

            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","double","" + param, column, e.getMessage()), e);
        }

        return value;
    }
    
    public float getFloat(ResultSet rs, int param)
    {
        float value;

        try
        {
            value = (float) rs.getDouble(param);

            if( column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","float","" + param, column, e.getMessage()), e);
        }

        return value;
    }
    
    public int getInt(ResultSet rs, int param)
    {
        int value;

        try
        {
            value = rs.getInt(param);

            if( column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","int","" + param, column, e.getMessage()), e);
        }

        return value;
    }

    public void setLong(PreparedStatement ps, int param, long value)
    {
        try
        {
            ps.setLong(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","long","" + value, column,e.getMessage()), e);
        }
    }

    public long getLong(ResultSet rs, int param)
    {
        long value;

        try
        {
            value = rs.getLong(param);

            if( column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","long","" + param, column, e.getMessage()), e);
        }

        return value;
    }
    
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                if (column!= null && column.isDefaultable() && column.getDefaultValue() != null)
                {
                    ps.setInt(param, Integer.valueOf(column.getDefaultValue().toString()).intValue());
                }
                else
                {
                    ps.setNull(param, getTypeInfo().getDataType());
                }
            }
            else
            {
                if (value instanceof Integer) 
                {
                    ps.setBigDecimal(param, BigDecimal.valueOf(((Integer)value).longValue()));
                }
                else if (value instanceof Long)
                {
                    ps.setBigDecimal(param, new BigDecimal(((Long)value).longValue()));
                }
                else if (value instanceof BigDecimal) 
                {
                    ps.setBigDecimal(param, (BigDecimal)value);
                }
                else if (value instanceof Float)
                {
                    ps.setDouble(param, ((Float)value).doubleValue());
                }            
                else if (value instanceof Double)
                {
                    ps.setDouble(param, ((Double)value).doubleValue());
                }                
                else if (value instanceof BigInteger)
                {
                    ps.setBigDecimal(param, new BigDecimal((BigInteger)value));
                }                
                else
                {
                    ps.setInt(param, ((Integer)value).intValue());
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","Object","" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_INTEGER))
            {
                value = rs.getBigDecimal(param);                
	            value = value == null ? null : Integer.valueOf(((BigDecimal)value).toBigInteger().intValue());
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_LONG))
            {
                value = rs.getBigDecimal(param);                
	            value = value == null ? null : Long.valueOf(((BigDecimal)value).toBigInteger().longValue());
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_MATH_BIGINTEGER))
            {
                value = rs.getBigDecimal(param);                
	            value = value == null ? null : ((BigDecimal)value).toBigInteger();
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_MATH_BIGDECIMAL))
            {
                value = rs.getBigDecimal(param);                
            }            
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_FLOAT))
            {
                double d = rs.getDouble(param);
                value = rs.wasNull() ? null : (float)d;
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_DOUBLE))
            {
                double d = rs.getDouble(param);
                value = rs.wasNull() ? null : d;
            }            
            else
            {
	            int i = rs.getInt(param);
	            value = rs.wasNull() ? null : Integer.valueOf(i);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Object","" + param, column, e.getMessage()), e);
        }

        return value;
    }    
}