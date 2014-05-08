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
2005 Andy Jefferson - added control over timezone to use
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Mapping of a TIMESTAMP RDBMS type.
 */
public class TimestampRDBMSMapping extends AbstractDatastoreMapping
{
    public TimestampRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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
     * Accessor for the type info for this column.
     * @return Datastore type info
     */
    public SQLTypeInfo getTypeInfo()
    {
        if (column != null && column.getColumnMetaData().getSqlType() != null)
        {
            return storeMgr.getSQLTypeInfoForJDBCType(Types.TIMESTAMP, column.getColumnMetaData().getSqlType());
        }
        return storeMgr.getSQLTypeInfoForJDBCType(Types.TIMESTAMP);
    }

    /**
     * Method to set an object in a PreparedStatement for sending to the datastore.
     * @param ps The PreparedStatement
     * @param param The parameter position (in the statement)
     * @param value The value to set
     */
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            Calendar cal = storeMgr.getCalendarForDateTimezone();
            // Note that passing the calendar to oracle makes it loses milliseconds

            if (value == null)
            {
                ps.setNull(param, getTypeInfo().getDataType());
            }
            else if (value instanceof java.sql.Timestamp)
            {
                if (cal != null)
                {
                    ps.setTimestamp(param, (Timestamp) value, cal);
                }
                else
                {
                    ps.setTimestamp(param, (Timestamp) value);
                }
            }
            else if (value instanceof java.sql.Time)
            {
                if (cal != null)
                {
                    ps.setTimestamp(param, new Timestamp(((Time) value).getTime()), cal);
                }
                else
                {
                    ps.setTimestamp(param, new Timestamp(((Time) value).getTime()));
                }
            }
            else if (value instanceof java.sql.Date)
            {
                if (cal != null)
                {
                    ps.setTimestamp(param, new Timestamp(((java.sql.Date) value).getTime()), cal);
                }
                else
                {
                    ps.setTimestamp(param, new Timestamp(((java.sql.Date) value).getTime()));
                }
            }
            else if (value instanceof Calendar)
            {
                if (cal != null)
                {
                    ps.setTimestamp(param, new Timestamp(((Calendar)value).getTime().getTime()), cal);
                }
                else
                {
                    ps.setTimestamp(param, new Timestamp(((Calendar)value).getTime().getTime()));
                }
            }
            else if (value instanceof java.util.Date)
            {
                if (cal != null)
                {
                    ps.setTimestamp(param, new Timestamp(((java.util.Date) value).getTime()), cal);
                }
                else
                {
                    ps.setTimestamp(param, new Timestamp(((java.util.Date) value).getTime()));
                }
            }
            else
            {
                if (cal != null)
                {
                    ps.setTimestamp(param, (Timestamp) value, cal);
                }
                else
                {
                    ps.setTimestamp(param, (Timestamp) value);
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001", "Timestamp", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Method to access a Timestamp from the ResultSet.
     * @param rs The ResultSet
     * @param param The parameter position in the ResultSet row.
     * @return The Timestamp object
     */
    protected Timestamp getTimestamp(ResultSet rs, int param)
    {
        Timestamp value;

        Calendar cal = storeMgr.getCalendarForDateTimezone();

        try
        {
            // pass the calendar to oracle makes it loses milliseconds
            // value = rs.getTimestamp(param,cal);
            if (cal != null)
            {
                value = rs.getTimestamp(param, cal);
            }
            else
            {
                value = rs.getTimestamp(param);
            }
        }
        catch (SQLException e)
        {
            try
            {
                String s = rs.getString(param);
                if (rs.wasNull())
                {
                    value = null;
                }
                else
                {
                    // JDBC driver has returned something other than a java.sql.Timestamp
                    // so we convert it to a Timestamp using its string form.
                    value = s == null ? null : TypeConversionHelper.stringToTimestamp(s, cal);
                }
            }
            catch (SQLException nestedEx)
            {
                throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055002", "Timestamp", "" + param, column, e.getMessage()), nestedEx);
            }
        }

        return value;
    }

    /**
     * Method to access an Object from the ResultSet.
     * @param rs The ResultSet
     * @param param The parameter position in the ResultSet row.
     * @return The Object
     */
    public Object getObject(ResultSet rs, int param)
    {
        Timestamp value = getTimestamp(rs, param);

        if (value == null)
        {
            return null;
        }
        else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_UTIL_DATE))
        {
            return new Date(getDatastoreAdapter().getAdapterTime(value));
        }
        else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_SQL_DATE))
        {
            return new java.sql.Date(getDatastoreAdapter().getAdapterTime(value));
        }
        else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_SQL_TIME))
        {
            return new Time(getDatastoreAdapter().getAdapterTime(value));
        }
        else
        {
            return value;
        }
    }
}