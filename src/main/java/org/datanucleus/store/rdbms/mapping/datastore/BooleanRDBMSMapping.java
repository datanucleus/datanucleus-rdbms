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
	Andy Jefferson - localised messages
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a BOOLEAN RDBMS type.
 */
public class BooleanRDBMSMapping extends AbstractDatastoreMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col column to be mapped
     */    
    public BooleanRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(storeMgr, mapping);
		column = col;
		initialize();
	}
    
    private void initialize()
    {
        initTypeInfo();
    }

    /**
     * Accessor for whether the mapping is boolean-based.
     * @return Whether the mapping is boolean based
     */
    public boolean isBooleanBased()
    {
        return true;
    }

    public int getJDBCType()
    {
        return Types.BOOLEAN;
    }

    public void setBoolean(PreparedStatement ps, int param, boolean value)
    {
        try
        {
            ps.setBoolean(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "boolean", "" + value, column, e.getMessage()), e);
        }
    }

    public boolean getBoolean(ResultSet rs, int param)
    {
        boolean value;

        try
        {
            value = rs.getBoolean(param);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Boolean", "" + param, column, e.getMessage()), e);
        }

        return value;
    }

    /**
     * Setter for booleans stored as String datastore types.
     * @param ps PreparedStatement
     * @param param Number of the field
     * @param value Value of the boolean
     */
    public void setString(PreparedStatement ps, int param, String value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getJDBCType());
            }
            else
            {
                ps.setBoolean(param, value.equals("Y") ? true : false);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "String", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Accessor for the value for a boolean field stored as a String datastore type.
     * @param rs ResultSet
     * @param param number of the parameter.
     * @return The value
     */
    public String getString(ResultSet rs, int param)
    {
        String value;

        try
        {
            value = (rs.getBoolean(param) ? "Y" : "N");
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","String", "" + param, column, e.getMessage()), e);
        }

        return value;
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
                if (value instanceof String)
                {
                    ps.setBoolean(param, value.equals("Y") ? true : false);
                }
                else if (value instanceof Boolean)
                {
                    ps.setBoolean(param, ((Boolean)value).booleanValue());
                }
                else
                {
                    throw new NucleusUserException(Localiser.msg("055004", value, column));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","Object", "" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            boolean b = rs.getBoolean(param);

            if (rs.wasNull())
            {
                value = null;
            }
            else
            {
                if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_STRING))
                {
                    value = (b ? "Y" : "N");
                }
                else //Boolean.class
                {
                    value = (b ? Boolean.TRUE : Boolean.FALSE);
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
        }

        return value;
    }
}