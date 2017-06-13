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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.Calendar;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a TIME RDBMS type.
 */
public class TimeRDBMSMapping extends AbstractDatastoreMapping
{
    public TimeRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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

    public int getJDBCType()
    {
        return Types.TIME;
    }

    /**
     * Mutator for the object.
     * @param ps The JDBC Statement
     * @param param The Parameter position
     * @param value The value to set
     **/ 
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getJDBCType());
            }
            else if (value instanceof Calendar)
            {
                ps.setTime(param, new Time(((Calendar)value).getTime().getTime()));
            }
            else if (value instanceof Time)
            {
                ps.setTime(param, (Time)value);
            }
            else if (value instanceof java.util.Date)
            {
                ps.setTime(param, new Time(((java.util.Date)value).getTime()));
            }
            else
            {
                throw new NucleusDataStoreException("Cannot set TIME RDBMS type with value of type " + value.getClass().getName());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","java.sql.Time","" + value, column, e.getMessage()), e);
        }
    }

    protected Time getTime(ResultSet rs, int param)
    {
        Time value;

        try
        {
            value = rs.getTime(param);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","java.sql.Time","" + param, column, e.getMessage()), e);
        }

        return value;
    }

    /**
     * Accessor for the object.
     * @param rs The ResultSet to extract the value from
     * @param param The parameter position
     * @return The object value
     **/
    public Object getObject(ResultSet rs, int param)
    {
        Time value = getTime(rs, param);
        if (value == null)
        {
            return null;
        }
        return value;
    }
}