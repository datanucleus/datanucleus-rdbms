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
2004 Andy Jefferson - fixed getObject() to cater for decimal values
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
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a Big Integer RDBMS type.
 */
public class BigIntRDBMSMapping extends AbstractDatastoreMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public BigIntRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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
                Object[] validValues = ((SingleFieldMapping) getJavaTypeMapping()).getValidValues(0);
                if (validValues != null)
                {
                    String constraints = storeMgr.getDatastoreAdapter().getCheckConstraintForValues(column.getIdentifier(), validValues, column.isNullable());
                    column.setConstraints(constraints);
                }
            }
        }
        initTypeInfo();
    }

    public int getJDBCType()
    {
        return Types.BIGINT;
    }

    public void setInt(PreparedStatement ps, int param, int value)
    {
        try
        {
            ps.setLong(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "int", "" + value), e);
        }
    }

    public int getInt(ResultSet rs, int param)
    {
        int value;

        try
        {
            value = (int) rs.getLong(param);

            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull())
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003", column));
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
            throw new NucleusDataStoreException(Localiser.msg("055001", "long", "" + value, column, e.getMessage()), e);
        }
    }

    public long getLong(ResultSet rs, int param)
    {
        long value;

        try
        {
            value = rs.getLong(param);

            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull())
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003", column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "long", "" + param, column, e.getMessage()), e);
        }

        return value;
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.AbstractDatastoreMapping#setString(java.lang.Object, int,
     * java.lang.String)
     */
    @Override
    public void setString(PreparedStatement ps, int exprIndex, String value)
    {
        setLong(ps, exprIndex, Long.parseLong(value));
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.AbstractDatastoreMapping#getString(java.lang.Object, int)
     */
    @Override
    public String getString(ResultSet resultSet, int exprIndex)
    {
        return Long.toString(getLong(resultSet, exprIndex));
    }

    /**
     * Setter for a parameter in a PreparedStatement
     * @param ps The PreparedStatement
     * @param param The parameter number to set
     * @param value The value to set it to.
     */
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                if (useDefaultWhenNull())
                {
                    ps.setLong(param, Long.parseLong(column.getDefaultValue().toString().trim()));
                }
                else
                {
                    ps.setNull(param, getJDBCType());
                }
            }
            else
            {
                if (value instanceof Character)
                {
                    ps.setInt(param, (Character)value);
                }
                else if (value instanceof String)
                {
                    ps.setLong(param, Long.parseLong((String)value));
                }
                else if (value instanceof java.util.Date)
                {
                    ps.setLong(param, ((java.util.Date)value).getTime());
                }
                else
                {
                    ps.setLong(param, ((Number) value).longValue());
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Long", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Method to retrieve a Big int from a ResultSet.
     * @param rs ResultSet
     * @param param The Parameter number in the result set
     * @return The BIGINT object
     */
    public Object getObject(ResultSet rs, int param)
    {
        Object value;
        try
        {
            // Read the object as a String since that is the most DB independent type we can use and should always get us something.
            String str = rs.getString(param);
            if (rs.wasNull())
            {
                value = null;
            }
            else
            {
                // Some RDBMS (e.g PostgreSQL) can return a long as a double so cater for this, and generate a Long :-)
                try
                {
                    // Try it as a long
                    value = Long.valueOf(str);
                }
                catch (NumberFormatException nfe)
                {
                    // Must be a double precision, so cast it
                    value = Long.valueOf((new Double(str)).longValue());
                }

                if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_UTIL_DATE))
                {
                    value = new java.util.Date(((Long) value).longValue());
                }
            }
        }
        catch (SQLException e)
        {
            String msg = Localiser.msg("055002", "Long", "" + param, column, e.getMessage());
            throw new NucleusDataStoreException(msg, e);
        }

        return value;
    }
}