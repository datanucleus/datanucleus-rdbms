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
2006 Andy Jefferson - added support for boolean/Boolean
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

/**
 * Mapping of a TINYINT column.
 */
public class TinyIntColumnMapping extends AbstractColumnMapping
{
    public TinyIntColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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
                    column.setCheckConstraints(storeMgr.getDatastoreAdapter().getCheckConstraintForValues(column.getIdentifier(), validValues, column.isNullable()));
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
                column.setCheckConstraints(constraints.toString());
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

    public int getJDBCType()
    {
        return Types.TINYINT;
    }

    public SQLTypeInfo getTypeInfo()
    {
        if (column != null && column.getColumnMetaData().getSqlType() != null)
        {
            return storeMgr.getSQLTypeInfoForJDBCType(Types.TINYINT, column.getColumnMetaData().getSqlType());
        }
        return storeMgr.getSQLTypeInfoForJDBCType(Types.TINYINT);
    }

    /**
     * Setter for when we are storing a boolean field as a TINYINT.
     * @param ps Prepared Statement
     * @param param Number of the parameter in the statement
     * @param value The boolean value
     */
    public void setBoolean(PreparedStatement ps, int param, boolean value)
    {
        try
        {
            ps.setInt(param, value ? 1 : 0);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "boolean", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Getter for when we are storing a boolean field as a TINYINT.
     * @param rs Result Set from which to get the boolean
     * @param param Number of the parameter in the statement
     * @return The boolean value
     */
    public boolean getBoolean(ResultSet rs, int param)
    {
        boolean value;

        try
        {
            int intValue = rs.getInt(param);
            if (intValue == 0)
            {
                value = false;
            }
            else if (intValue == 1)
            {
                value = true;
            }
            else
            {
                throw new NucleusDataStoreException(Localiser.msg("055006", "Types.TINYINT", "" + intValue));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Boolean", "" + param, column, e.getMessage()), e);
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
            throw new NucleusDataStoreException(Localiser.msg("055001","int", "" + value, column, e.getMessage()), e);
        }
    }

    public int getInt(ResultSet rs, int param)
    {
        int value;

        try
        {
            value = rs.getInt(param);
            if ((column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull()) && rs.wasNull())
            {
                throw new NullValueException(Localiser.msg("055003",column));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","int", "" + param, column, e.getMessage()), e);
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
            throw new NucleusDataStoreException(Localiser.msg("055001","int", "" + value, column, e.getMessage()), e);
        }
    }

    public long getLong(ResultSet rs, int param)
    {
        int value;

        try
        {
            value = rs.getInt(param);
            if ((column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull()) && rs.wasNull())
            {
                throw new NullValueException(Localiser.msg("055003",column));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","int", "" + param, column, e.getMessage()), e);
        }

        return value;
    }

    public void setByte(PreparedStatement ps, int param, byte value)
    {
        try
        {
            //TODO bug with SQL SERVER DRIVER. It doesnt accept Byte -128
//          ps.setByte(param, value);
            ps.setInt(param, value);
            
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","byte", "" + value, column, e.getMessage()), e);
        }
    }

    public byte getByte(ResultSet rs, int param)
    {
        byte value;

        try
        {
            value = rs.getByte(param);
            if ((column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull()) && rs.wasNull())
            {
                throw new NullValueException(Localiser.msg("055003",column));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","byte", "" + param, column, e.getMessage()), e);
        }

        return value;
    }

    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                if (useDefaultWhenNull())
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
                if (value instanceof Byte)
                {
                    ps.setInt(param, ((Byte)value).shortValue());
                }
                else if (value instanceof BigInteger)
                {
                    ps.setInt(param, ((BigInteger)value).shortValue());
                }
                else if (value instanceof String)
                {
                    ps.setInt(param, Integer.parseInt((String)value));
                }
                else if (value instanceof Boolean)
                {
                    ps.setInt(param, ((Boolean)value) ? 1 : 0);
                }
                else
                {
                    throw new NucleusException("TinyIntColumnMapping.setObject called for " + StringUtils.toJVMIDString(value) + " but not supported");
                }
                //TODO bug with SQL SERVER DRIVER. It doesn't accept Byte -128
                //ps.setByte(param, ((Byte)value).byteValue());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","Byte", "" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;
        try
        {
            int d = rs.getInt(param);
            if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_MATH_BIGINTEGER))
            {
                value = rs.wasNull() ? null : BigInteger.valueOf(d);
            }
            else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_BOOLEAN))
            {
                value = rs.wasNull() ? null : (d == 1 ? Boolean.TRUE : Boolean.FALSE);
            }
            else
            {
                value = rs.wasNull() ? null : Byte.valueOf(rs.getByte(param));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Byte", "" + param, column, e.getMessage()), e);
        }

        return value;
    }
}