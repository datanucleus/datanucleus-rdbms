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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a DATE column.
 */
public class DateColumnMapping extends AbstractColumnMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public DateColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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
        return Types.DATE;
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
            else if (value instanceof java.util.Calendar)
            {
                ps.setDate(param, new Date(((java.util.Calendar)value).getTime().getTime()));
            }
            else if (value instanceof java.sql.Date)
            {
                ps.setDate(param, (java.sql.Date)value);
            }
            else if (value instanceof java.util.Date)
            {
                ps.setDate(param, new java.sql.Date(((java.util.Date)value).getTime()));
            }
            else
            {
                throw new NucleusDataStoreException("Cannot set DATE RDBMS type with value of type " + value.getClass().getName());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","java.sql.Date","" + value), e);
        }
    }

    protected Date getDate(ResultSet rs, int param)
    {
        try
        {
            return rs.getDate(param);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","java.sql.Date","" + param), e);
        }
    }

    /**
     * Accessor for the object.
     * @param rs The ResultSet to extract the value from
     * @param param The parameter position
     * @return The object value
     **/
    public Object getObject(ResultSet rs, int param)
    {
        Date value = getDate(rs, param);
        if (value == null)
        {
            return null;
        }

        if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_UTIL_DATE))
        {
            return new java.util.Date(value.getTime());
        }
        return value;
    }
}