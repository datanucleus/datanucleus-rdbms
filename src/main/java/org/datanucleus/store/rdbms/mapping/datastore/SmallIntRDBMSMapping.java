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
2005 David Eaves - contributed support for byte/Byte
2006 Andy Jefferson - added support for boolean/Boolean
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

/**
 * Mapping of a SMALLINT RDBMS type.
 */
public class SmallIntRDBMSMapping extends AbstractDatastoreMapping
{
    public SmallIntRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(storeMgr, mapping);
        column = col;
        initialize();
    }

    /**
     * Initialise the mapping.
     */
    private void initialize()
    {
        if (column != null)
        {
            column.checkPrimitive();

            // Valid Values
            JavaTypeMapping m = getJavaTypeMapping();
            if (m instanceof SingleFieldMapping)
            {
                Object[] validValues = ((SingleFieldMapping)m).getValidValues(0);
                if (validValues != null)
                {
                    String constraints = storeMgr.getDatastoreAdapter().getCheckConstraintForValues(
                            column.getIdentifier(), validValues, column.isNullable());
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

    public int getJDBCType()
    {
        return Types.SMALLINT;
    }

    /**
     * Setter for when we are storing a boolean field as a SMALLINT.
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
     * Getter for when we are storing a boolean field as a SMALLINT.
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
                throw new NucleusDataStoreException(Localiser.msg("055006", "Types.SMALLINT", "" + intValue));
            }

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
            throw new NucleusDataStoreException(Localiser.msg("055002","Boolean", "" + param, column, e.getMessage()), e);
        }

        return value;
    }
    
    public void setShort(PreparedStatement ps, int param, short value)
    {
        try
        {
            ps.setShort(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "short", "" + value, column, e.getMessage()), e);
        }
    }

    public short getShort(ResultSet rs, int param)
    {
        short value;

        try
        {
            value = rs.getShort(param);
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
            throw new NucleusDataStoreException(Localiser.msg("055002", "short", "" + param, column, e.getMessage()), e);
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
            throw new NucleusDataStoreException(Localiser.msg("055001", "int", "" + value, column, e.getMessage()), e);
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
                    throw new NullValueException(Localiser.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "int", "" + param, column, e.getMessage()), e);
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
            throw new NucleusDataStoreException(Localiser.msg("055001", "short", "" + value, column, e.getMessage()), e);
        }
    }

    public long getLong(ResultSet rs, int param)
    {
        long value;

        try
        {
            value = rs.getShort(param);
            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull())
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "short", "" + param, column, e.getMessage()), e);
        }

        return value;
    }

    public void setByte(PreparedStatement ps, int param, byte value)
    {
        try
        {
            // TODO bug with MSSQL SERVER DRIVER. It doesn't accept Byte -128
            // ps.setByte(param, value);
            ps.setInt(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "byte", "" + value, column, e.getMessage()), e);
        }
    }

    public byte getByte(ResultSet rs, int param)
    {
        byte value;

        try
        {
            value = rs.getByte(param);
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
            throw new NucleusDataStoreException(Localiser.msg("055002", "byte", "" + param, column, e.getMessage()), e);
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
                    ps.setNull(param, getJDBCType());
                }
            }
            else
            {
                Class type = value.getClass();
                if (type == Integer.class)
                {
                    ps.setShort(param, ((Integer)value).shortValue());
                }
                else if (type == Short.class)
                {
                    ps.setShort(param, ((Short)value).shortValue());
                }
                else if (type == Byte.class)
                {
                    ps.setShort(param, ((Byte)value).shortValue());
                }
                else if (type == Character.class)
                {
                    ps.setShort(param, (short)((Character)value).charValue());
                }
                else if (type == Boolean.class)
                {
                    ps.setShort(param, (short)(((Boolean)value) ? 1 : 0));
                }
                else if (type == BigInteger.class)
                {
                    ps.setShort(param, ((BigInteger)value).shortValue());
                }
                else if (type == Long.class)
                {
                    ps.setShort(param, ((Long)value).shortValue());
                }
                else
                {
                    throw new NucleusException("SmallIntRDBMSMapping.setObject called for " + 
                        StringUtils.toJVMIDString(value) + " but not supported");
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            short d = rs.getShort(param);
            Class type = getJavaTypeMapping().getJavaType();
            if (type == Short.class)
            {
                value = rs.wasNull() ? null : Short.valueOf(d);
            }
            else if (type == Integer.class)
            {
                value = rs.wasNull() ? null : Integer.valueOf(d);
            }
            else if (type == Byte.class)
            {
                value = rs.wasNull() ? null : Byte.valueOf((byte)d);
            }
            else if (type == BigInteger.class)
            {
                value = rs.wasNull() ? null : BigInteger.valueOf(d);
            }
            else if (type == Boolean.class)
            {
                value = rs.wasNull() ? null : (d == 1 ? Boolean.TRUE : Boolean.FALSE);
            }
            else
            {
                value = rs.wasNull() ? null : Short.valueOf(d);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
        }

        return value;
    }
}