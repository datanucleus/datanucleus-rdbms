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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.StringUtils;

/**
 * Mapping of a INTEGER RDBMS type.
 */
public class IntegerRDBMSMapping extends AbstractDatastoreMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public IntegerRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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

            // Valid Values
            if (getJavaTypeMapping() instanceof SingleFieldMapping)
            {
                Object[] validValues = ((SingleFieldMapping)getJavaTypeMapping()).getValidValues(0);
                if (validValues != null)
                {
                    String constraints = storeMgr.getDatastoreAdapter().getCheckConstraintForValues(column.getIdentifier(), validValues, column.isNullable());
                    column.setConstraints(constraints);
                }
            }

            if (getJavaTypeMapping().getJavaType() == Boolean.class)
            {
                // With a Boolean we'll store it as 1, 0 (see setBoolean/getBoolean methods)
                StringBuilder constraints = new StringBuilder("CHECK (" + column.getIdentifier() + " IN (0,1)");
                if (column.isNullable())
                {
                    constraints.append(" OR " + column.getIdentifier() + " IS NULL");
                }
                constraints.append(')');
                column.setConstraints(constraints.toString());
            }
        }
		initTypeInfo();
    }

    /**
     * Accessor for whether the mapping is integer-based.
     * @return Whether the mapping is integer based
     */
    public boolean isIntegerBased()
    {
        return true;
    }

    public SQLTypeInfo getTypeInfo()
    {
        if (column != null && column.getColumnMetaData().getSqlType() != null)
        {
            return storeMgr.getSQLTypeInfoForJDBCType(Types.INTEGER, column.getColumnMetaData().getSqlType());
        }
        return storeMgr.getSQLTypeInfoForJDBCType(Types.INTEGER);
    }

    public void setChar(PreparedStatement ps, int param, char value)
    {
        try
        {
            ps.setInt(param,value);                 
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001","char","" + value, column,e.getMessage()), e);
        }
    }

    public char getChar(ResultSet rs, int param)
    {
        char value;

        try
        {
            value = (char)rs.getInt(param);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055002","char","" + param, column, e.getMessage()), e);
        }
        return value;
    }
    
    public void setInt(PreparedStatement ps, int param, int value)
    {
        try
        {
            ps.setInt(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001","int","" + value, column, e.getMessage()), e);
        }
    }

    public int getInt(ResultSet rs, int param)
    {
        int value;

        try
        {
            value = rs.getInt(param);

            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(LOCALISER_RDBMS.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055002","int","" + param, column, e.getMessage()), e);
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
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001","long","" + value, column, e.getMessage()), e);
        }
    }

    public long getLong(ResultSet rs, int param)
    {
        long value;

        try
        {
            value = rs.getLong(param);

            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(LOCALISER_RDBMS.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055002","long","" + param, column, e.getMessage()), e);
        }

        return value;
    }    

    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                if (column != null && column.isDefaultable() && column.getDefaultValue() != null &&
                    !StringUtils.isWhitespace(column.getDefaultValue().toString()))
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
                if (value instanceof Character)
                {
                    String s = value.toString();
                    ps.setInt(param,s.charAt(0));                  
                }
                else if (value instanceof String)
                {
                    String s = (String)value;
                    ps.setInt(param,s.charAt(0));                  
                }
                else if (value instanceof Long)
                {
                    ps.setLong(param,((Long)value).longValue());                  
                }
                else
                {
                    ps.setLong(param, ((Number)value).longValue());
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001","Object","" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            long i = rs.getLong(param);
            if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_CHARACTER))
            {
            	value = rs.wasNull() ? null : Character.valueOf((char)i);
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_STRING))
            {
            	value = rs.wasNull() ? null : Character.valueOf((char)i).toString();
            }            
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_LONG))
            {
                value = rs.wasNull() ? null : Long.valueOf(i);
            }            
			else
            {
                value = rs.wasNull() ? null : Integer.valueOf((int)i);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055002","Object","" + param, column, e.getMessage()), e);
        }

        return value;
    }    
}