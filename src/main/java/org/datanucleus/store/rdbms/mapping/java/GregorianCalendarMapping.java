/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Table;

/**
 * Maps the class fields of a GregorianCalendar to column(s).
 * We default to a single column (timestamp), but allow the option of persisting to 2 columns (timestamp millisecs and timezone).
 * JPOX traditionally supported persistence to 2 columns.
 */
public class GregorianCalendarMapping extends SingleFieldMultiMapping
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.JavaTypeMapping#initialize(AbstractMemberMetaData, DatastoreContainerObject, ClassLoaderResolver)
     */
    public void initialize(AbstractMemberMetaData fmd, Table table, ClassLoaderResolver clr)
    {
        super.initialize(fmd, table, clr);
        addColumns();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.JavaTypeMapping#initialize(RDBMSStoreManager, java.lang.String)
     */
    public void initialize(RDBMSStoreManager storeMgr, String type)
    {
        super.initialize(storeMgr, type);
        addColumns();
    }

    protected void addColumns()
    {
        boolean singleColumn = true;
        if (mmd != null)
        {
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            if (colmds != null && colmds.length == 2)
            {
                // 2 columns specified so use that handling
                singleColumn = false;
            }
            else if (mmd.hasExtension(MetaData.EXTENSION_MEMBER_CALENDAR_ONE_COLUMN) && mmd.getValueForExtension(MetaData.EXTENSION_MEMBER_CALENDAR_ONE_COLUMN).equals("false"))
            {
                singleColumn = false;
            }
        }

        if (singleColumn)
        {
            // (Timestamp) implementation
            addColumns(ClassNameConstants.JAVA_SQL_TIMESTAMP);
        }
        else
        {
            // (Timestamp millisecs, Timezone) implementation
            addColumns(ClassNameConstants.LONG); // Timestamp millisecs
            addColumns(ClassNameConstants.JAVA_LANG_STRING); // Timezone
        }
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return GregorianCalendar.class;
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore field.
     * This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        if (getNumberOfDatastoreMappings() == 1)
        {
            // (Timestamp) implementation
            return ClassNameConstants.JAVA_SQL_TIMESTAMP;
        }

        // (Timestamp millisecs, Timezone) implementation
        if (index == 0)
        {
            return ClassNameConstants.LONG;
        }
        else if (index == 1)
        {
            return ClassNameConstants.JAVA_LANG_STRING;
        }
        return null;
    }

    /**
     * Method to return the value to be stored in the specified datastore index given the overall
     * value for this java type.
     * @param index The datastore index
     * @param value The overall value for this java type
     * @return The value for this datastore index
     */
    public Object getValueForDatastoreMapping(NucleusContext nucleusCtx, int index, Object value)
    {
        if (getNumberOfDatastoreMappings() == 1)
        {
            return value;
        }
        else if (index == 0)
        {
            return ((Calendar)value).getTime().getTime();
        }
        else if (index == 1)
        {
            return ((Calendar)value).getTimeZone().getID();
        }
        throw new IndexOutOfBoundsException();
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#setObject(org.datanucleus.ExecutionContext, java.lang.Object,
     *  int[], java.lang.Object)
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        GregorianCalendar cal = (GregorianCalendar) value;
        if (getNumberOfDatastoreMappings() == 1)
        {
            // (Timestamp) implementation
            Timestamp ts = null;
            if (cal != null)
            {
                ts = new Timestamp(cal.getTimeInMillis());
            }
            // Server timezone will be applied in the RDBMSMapping at persistence
            getDatastoreMapping(0).setObject(ps, exprIndex[0], ts);
        }
        else
        {
            // (Timestamp millisecs, Timezone) implementation
            if (cal == null)
            {
                getDatastoreMapping(0).setObject(ps, exprIndex[0], null);
                getDatastoreMapping(1).setObject(ps, exprIndex[1], null);
            }
            else
            {
                getDatastoreMapping(0).setLong(ps, exprIndex[0], cal.getTime().getTime());
                getDatastoreMapping(1).setString(ps, exprIndex[1], cal.getTimeZone().getID());
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getObject(org.datanucleus.ExecutionContext, java.lang.Object, int[])
     */
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        try
        {
            // Check for null entries
            if (getDatastoreMapping(0).getObject(resultSet, exprIndex[0]) == null)
            {
                return null;
            }
        }
        catch (Exception e)
        {
            // Do nothing
        }

        if (getNumberOfDatastoreMappings() == 1)
        {
            Date date = (Date)getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTimeInMillis(date.getTime());

            String timezoneID = ec.getNucleusContext().getConfiguration().getStringProperty(PropertyNames.PROPERTY_SERVER_TIMEZONE_ID);
            if (timezoneID != null)
            {
                // Apply server timezone ID since we dont know what it was upon persistence
                cal.setTimeZone(TimeZone.getTimeZone(timezoneID));
            }
            return cal;
        }

        // (Timestamp millisecs, Timezone) implementation
        long millisecs = getDatastoreMapping(0).getLong(resultSet, exprIndex[0]);

        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(new Date(millisecs));
        String timezoneId = getDatastoreMapping(1).getString(resultSet, exprIndex[1]);
        if (timezoneId != null)
        {
            cal.setTimeZone(TimeZone.getTimeZone(timezoneId));
        }
        return cal;
    }
}