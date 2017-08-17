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
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a DOUBLE RDBMS type.
 */
public class DoubleRDBMSMapping extends AbstractDatastoreMapping
{
    /**
     * Constructor.
     * @param mapping The java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public DoubleRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(storeMgr, mapping);
		column = col;
		initialize();
	}

    private void initialize()
    {
        if (column != null)
        {
            column.checkPrimitive();
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

    public int getJDBCType()
    {
        return Types.DOUBLE;
    }

    public void setInt(PreparedStatement ps, int param, int value)
    {
        try
        {
            ps.setDouble(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","int","" + value, column, e.getMessage()), e);
        }
    }

    public int getInt(ResultSet rs, int param)
    {
        int value;

        try
        {
            value = (int)rs.getDouble(param);
            if ((column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull()) && rs.wasNull())
            {
                throw new NullValueException(Localiser.msg("055003",column));
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
            throw new NucleusDataStoreException(Localiser.msg("055001","long","" + value, column, e.getMessage()), e);
        }
    }

    public long getLong(ResultSet rs, int param)
    {
        long value;

        try
        {
            value = rs.getLong(param);
            if ((column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull()) && rs.wasNull())
            {
                throw new NullValueException(Localiser.msg("055003",column));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","long","" + param, column, e.getMessage()), e);
        }

        return value;
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

    public double getDouble(ResultSet rs, int param)
    {
        double value;

        try
        {
            value = rs.getDouble(param);
            if ((column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull()) && rs.wasNull())
            {
                throw new NullValueException(Localiser.msg("055003",column));
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
            if ((column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull()) && rs.wasNull())
            {
                throw new NullValueException(Localiser.msg("055003",column));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","float","" + param, column, e.getMessage()), e);
        }

        return value;
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
    
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getJDBCType());
            }
            else if (value instanceof Integer)
            {
                ps.setDouble(param, ((Integer)value).doubleValue());
            }
            else if (value instanceof Long)
            {
                ps.setDouble(param, ((Long)value).doubleValue());
            }
            else if (value instanceof Short)
            {
                ps.setDouble(param, ((Short)value).doubleValue());
            }
            else if (value instanceof Float)
            {
                ps.setDouble(param, ((Float)value).doubleValue());
            }
            else if (value instanceof Character)
            {
                ps.setDouble(param, value.toString().charAt(0));                  
            }
            else if (value instanceof BigInteger)
            {
                ps.setDouble(param, ((BigInteger)value).doubleValue());
            }
            else if (value instanceof BigDecimal)
            {
                ps.setDouble(param, ((BigDecimal)value).doubleValue());
            }
            else if (value instanceof String)
            {
                ps.setDouble(param, Double.parseDouble((String)value));
            }
            else
            {
                ps.setDouble(param, ((Double)value).doubleValue());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","Object","" + value,column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            double d = rs.getDouble(param);
            if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_INTEGER))
            {
                value = rs.wasNull() ? null : Integer.valueOf((int)d);
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_LONG))
            {
                value = rs.wasNull() ? null : Long.valueOf((long)d);
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_FLOAT))
            {
                value = rs.wasNull() ? null : (float)d;
            }
            else
            {
                value = rs.wasNull() ? null : Double.valueOf(d);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Object","" + param, column, e.getMessage()), e);
        }

        return value;
    }
}