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
package org.datanucleus.store.rdbms.mapping.column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a TIMESTAMP column.
 */
public class TimestampColumnMapping extends AbstractColumnMapping
{
    public TimestampColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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
        return Types.TIMESTAMP;
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
            // Note that passing the calendar to oracle/hsqldb makes it loses milliseconds

            if (value == null)
            {
                ps.setNull(param, getJDBCType());
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
                throw new NucleusDataStoreException("Cannot set TIMESTAMP RDBMS type with value of type " + value.getClass().getName());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Timestamp", "" + value, column, e.getMessage()), e);
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
            // TODO pass the calendar to Oracle or HSQLDB makes it loses milliseconds
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
                    // JDBC driver has returned something other than a java.sql.Timestamp so we convert it to a Timestamp using its string form.
                    value = s == null ? null : stringToTimestamp(s, cal);
                }
            }
            catch (SQLException nestedEx)
            {
                throw new NucleusDataStoreException(Localiser.msg("055002", "Timestamp", "" + param, column, e.getMessage()), nestedEx);
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

    /**
     * Converts a string in JDBC timestamp escape format to a Timestamp object.
     * To be precise, we prefer to find a JDBC escape type sequence in the format "yyyy-mm-dd hh:mm:ss.fffffffff", but this does not accept
     * other separators of fields, so as long as the numbers are in the order year, month, day, hour, minute, second then we accept it.
     * @param s Timestamp string
     * @param cal The Calendar to use for conversion
     * @return Corresponding <i>java.sql.Timestamp</i> value.
     * @exception java.lang.IllegalArgumentException Thrown if the format of the
     * String is invalid
     */
    private static Timestamp stringToTimestamp(String s, Calendar cal)
    {
        int[] numbers = convertStringToIntArray(s);
        if (numbers == null || numbers.length < 6)
        {
            throw new IllegalArgumentException(Localiser.msg("030003", s));
        }

        int year = numbers[0];
        int month = numbers[1];
        int day = numbers[2];
        int hour = numbers[3];
        int minute = numbers[4];
        int second = numbers[5];
        int nanos = 0;
        if (numbers.length > 6)
        {
            StringBuilder zeroedNanos = new StringBuilder("" + numbers[6]);
            if (zeroedNanos.length() < 9)
            {
                // Add trailing zeros
                int numZerosToAdd = 9-zeroedNanos.length();
                for (int i=0;i<numZerosToAdd;i++)
                {
                    zeroedNanos.append("0");
                }
                nanos = Integer.valueOf(zeroedNanos.toString());
            }
            else
            {
                nanos = numbers[6];
            }
        }

        Calendar thecal = cal;
        if (cal == null)
        {
            thecal = new GregorianCalendar();
        }
        thecal.set(Calendar.ERA, GregorianCalendar.AD);
        thecal.set(Calendar.YEAR, year);
        thecal.set(Calendar.MONTH, month - 1);
        thecal.set(Calendar.DATE, day);
        thecal.set(Calendar.HOUR_OF_DAY, hour);
        thecal.set(Calendar.MINUTE, minute);
        thecal.set(Calendar.SECOND, second);
        Timestamp ts = new Timestamp(thecal.getTime().getTime());
        ts.setNanos(nanos);

        return ts;
    }

    /**
     * Convenience method to convert a String containing numbers (separated by assorted
     * characters) into an int array. The separators can be ' '  '-'  ':'  '.'  ',' etc.
     * @param str The String
     * @return The int array
     */
    private static int[] convertStringToIntArray(String str)
    {
        if (str == null)
        {
            return null;
        }

        int[] values = null;
        ArrayList list = new ArrayList();

        int start = -1;
        for (int i=0;i<str.length();i++)
        {
            if (start == -1 && Character.isDigit(str.charAt(i)))
            {
                start = i;
            }
            if (start != i && start >= 0)
            {
                if (!Character.isDigit(str.charAt(i)))
                {
                    list.add(Integer.valueOf(str.substring(start, i)));
                    start = -1;
                }
            }
            if (i == str.length()-1 && start >= 0)
            {
                list.add(Integer.valueOf(str.substring(start)));
            }
        }

        if (!list.isEmpty())
        {
            values = new int[list.size()];
            Iterator iter = list.iterator();
            int n = 0;
            while (iter.hasNext())
            {
                values[n++] = ((Integer)iter.next()).intValue();
            }
        }
        return values;
    }
}