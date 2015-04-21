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

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a Long VARCHAR RDBMS type.
 */
public class LongVarcharRDBMSMapping extends AbstractDatastoreMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column to be mapped
     */
    public LongVarcharRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(storeMgr, mapping);
		column = col;
		initialize();
	}

    /**
     * Accessor for whether the mapping is string-based.
     * @return Whether the mapping is string based
     */
    public boolean isStringBased()
    {
        return true;
    }

    private void initialize()
    {
		initTypeInfo();
    }

    public int getJDBCType()
    {
        return Types.LONGVARCHAR;
    }

    public void setString(PreparedStatement ps, int param, String value)
    {
        try
        {
            if (value == null)
            {
                if (column != null && column.isDefaultable() && column.getDefaultValue() != null)
                {
                    ps.setString(param,column.getDefaultValue().toString().trim());
                }
                else
                {
                    ps.setNull(param, getJDBCType());
                }
            }
            else
            {
                ps.setString(param, value);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","String", "" + value, column, e.getMessage()), e);
        }
    }

    public String getString(ResultSet rs, int param)
    {
        String value;

        try
        {
            value = rs.getString(param);
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
                ps.setString(param, ((String)value));
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
            String s = rs.getString(param);
            value = rs.wasNull() ? null : s;
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Object", "" + param, column, e.getMessage()), e);
        }

        return value;
    }    
}