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
2004 Andy Jefferson - added Currency
2005 Andy Jefferson - added control over TimeZone
2007 Andy Jefferson - add persist empty string as null, add support for char(XX) padding
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Mapping of a CHAR RDBMS type.
 */
public class CharRDBMSMapping extends AbstractDatastoreMapping
{
    private static final ThreadLocal<FormatterInfo> formatterThreadInfo = new ThreadLocal<CharRDBMSMapping.FormatterInfo>()
    {
        protected FormatterInfo initialValue()
        {
            return new FormatterInfo();
        }
    };

    static class FormatterInfo
    {
        SimpleDateFormat formatter;
    }

    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col column to be mapped
     */
    public CharRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(storeMgr, mapping);
        column = col;
        initialize();
    }

    /**
     * Method to initialise the column mapping. Provides default length specifications for the CHAR column to
     * fit the data being stored.
     */
    protected void initialize()
    {
        if (column != null)
        {
            // Default Length
            if (getJavaTypeMapping() instanceof SingleFieldMapping && column.getColumnMetaData().getLength() == null)
            {
                SingleFieldMapping m = (SingleFieldMapping) getJavaTypeMapping();
                if (m.getDefaultLength(0) > 0)
                {
                    // No column length provided by user and the type has a default length so use it
                    column.getColumnMetaData().setLength(m.getDefaultLength(0));
                }
            }

            column.getColumnMetaData().setJdbcType("CHAR");
            column.checkString();

            // Valid Values
            if (getJavaTypeMapping() instanceof SingleFieldMapping)
            {
                Object[] validValues = ((SingleFieldMapping) getJavaTypeMapping()).getValidValues(0);
                if (validValues != null)
                {
                    String constraints = getDatastoreAdapter().getCheckConstraintForValues(column.getIdentifier(), validValues, column.isNullable());
                    column.setConstraints(constraints);
                }
            }

            if (getJavaTypeMapping().getJavaType() == Boolean.class)
            {
                // With a Boolean we'll store it as "Y", "N" (see setBoolean/getBoolean methods)
                column.getColumnMetaData().setLength(1);
                StringBuilder constraints = new StringBuilder("CHECK (" + column.getIdentifier() + " IN ('Y','N')");
                if (column.isNullable())
                {
                    constraints.append(" OR " + column.getIdentifier() + " IS NULL");
                }
                constraints.append(')');
                column.setConstraints(constraints.toString());
            }

            // Check on max length of the type against the length we have set
            SQLTypeInfo typeInfo = getTypeInfo();
            int maxlength = typeInfo.getPrecision();
            if (column.getColumnMetaData().getLength().intValue() <= 0 || column.getColumnMetaData().getLength().intValue() > maxlength)
            {
                if (typeInfo.isAllowsPrecisionSpec())
                {
                    throw new NucleusUserException("String max length of " + column.getColumnMetaData().getLength() + " is outside the acceptable range [0, " + 
                        maxlength + "] for column \"" + column.getIdentifier() + "\"");
                }
            }
        }
        initTypeInfo();
    }

    /**
     * Accessor for whether the mapping is string-based.
     * @return Whether the mapping is string based
     */
    public boolean isStringBased()
    {
        return true;
    }

    public SQLTypeInfo getTypeInfo()
    {
        if (column != null && column.getColumnMetaData().getSqlType() != null)
        {
            return storeMgr.getSQLTypeInfoForJDBCType(Types.CHAR, column.getColumnMetaData().getSqlType());
        }
        return storeMgr.getSQLTypeInfoForJDBCType(Types.CHAR);
    }

    /**
     * Method to set a character at the specified position in the JDBC PreparedStatement.
     * @param ps The PreparedStatement
     * @param param Parameter position
     * @param value The value to set
     */
    public void setChar(PreparedStatement ps, int param, char value)
    {
        try
        {
            if (value == Character.UNASSIGNED && !getDatastoreAdapter().supportsOption(DatastoreAdapter.PERSIST_OF_UNASSIGNED_CHAR))
            {
                // Some datastores (e.g Postgresql) dont allow persistence of 0x0 ("\0") so use a space
                value = ' ';
                NucleusLogger.DATASTORE.warn(Localiser.msg("055008"));
            }
            ps.setString(param, Character.valueOf(value).toString());
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "char", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Method to extract a character from the ResultSet at the specified position
     * @param rs The Result Set
     * @param param The parameter position
     * @return the character
     */
    public char getChar(ResultSet rs, int param)
    {
        char value;

        try
        {
            String str = rs.getString(param);
            if (str == null)
            {
                return 0;
            }
            value = str.charAt(0);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "char", "" + param, column, e.getMessage()), e);
        }
        return value;
    }

    /**
     * Method to set a String at the specified position in the JDBC PreparedStatement.
     * @param ps The PreparedStatement
     * @param param Parameter position
     * @param value The value to set
     */
    public void setString(PreparedStatement ps, int param, String value)
    {
        try
        {
            if (value == null)
            {
                // Null string
                if (column != null && column.isDefaultable() && column.getDefaultValue() != null)
                {
                    ps.setString(param, column.getDefaultValue().toString().trim());
                }
                else
                {
                    ps.setNull(param, getTypeInfo().getDataType());
                }
            }
            else if (value.length() == 0)
            {
                // Empty string
                if (storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_PERSIST_EMPTY_STRING_AS_NULL))
                {
                    // Persist as null
                    ps.setString(param, null);
                }
                else
                {
                    if (getDatastoreAdapter().supportsOption(DatastoreAdapter.NULL_EQUALS_EMPTY_STRING))
                    {
                        // Datastore doesnt support empty string so use special character
                        value = getDatastoreAdapter().getSurrogateForEmptyStrings();
                    }
                    ps.setString(param, value);
                }
            }
            else
            {
                if (column != null) // Column could be null if we have a query of something like an "enumClass.value"
                {
                    Integer colLength = column.getColumnMetaData().getLength();
                    if (colLength != null && colLength.intValue() < value.length())
                    {
                        // Data in field exceeds datastore column, so take required action
                        String action = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STRING_LENGTH_EXCEEDED_ACTION);
                        if (action.equals("EXCEPTION"))
                        {
                            throw new NucleusUserException(Localiser.msg("055007", value, column.getIdentifier().toString(), "" + colLength.intValue())).setFatal();
                        }
                        else if (action.equals("TRUNCATE"))
                        {
                            value = value.substring(0, colLength.intValue());
                        }
                    }
                }
                ps.setString(param, value);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "String", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Method to extract a String from the ResultSet at the specified position
     * @param rs The Result Set
     * @param param The parameter position
     * @return the String
     */
    public String getString(ResultSet rs, int param)
    {
        try
        {
            String value = rs.getString(param);
            if (value == null)
            {
                return value;
            }
            else if (getDatastoreAdapter().supportsOption(DatastoreAdapter.NULL_EQUALS_EMPTY_STRING) && value.equals(getDatastoreAdapter().getSurrogateForEmptyStrings()))
            {
                // Special character simbolizing empty string
                return "";
            }
            else
            {
                if (column.getJdbcType() == JdbcType.CHAR && getDatastoreAdapter().supportsOption(DatastoreAdapter.CHAR_COLUMNS_PADDED_WITH_SPACES))
                {
                    // String has likely been padded with spaces at the end by the datastore so trim trailing whitespace
                    int numPaddingChars = 0;
                    for (int i = value.length() - 1; i >= 0; i--)
                    {
                        if (value.charAt(i) == ' ') // Only allow for space currently
                        {
                            numPaddingChars++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (numPaddingChars > 0)
                    {
                        value = value.substring(0, value.length() - numPaddingChars);
                    }
                }
                return value;
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "String", "" + param, column, e.getMessage()), e);
        }
    }

    /**
     * Method to set a boolean at the specified position in the JDBC PreparedStatement.
     * @param ps The PreparedStatement
     * @param param Parameter position
     * @param value The value to set
     */
    public void setBoolean(PreparedStatement ps, int param, boolean value)
    {
        try
        {
            ps.setString(param, value ? "Y" : "N");
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "boolean", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Method to extract a boolean from the ResultSet at the specified position
     * @param rs The Result Set
     * @param param The parameter position
     * @return the boolean
     */
    public boolean getBoolean(ResultSet rs, int param)
    {
        boolean value;

        try
        {
            String s = rs.getString(param);
            if (s == null)
            {
                if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull())
                {
                    if (rs.wasNull())
                    {
                        throw new NullValueException(Localiser.msg("055003", column));
                    }
                }
                return false;
            }

            if (s.equals("Y"))
            {
                value = true;
            }
            else if (s.equals("N"))
            {
                value = false;
            }
            else
            {
                throw new NucleusDataStoreException(Localiser.msg("055003", column));
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "boolean", "" + param, column, e.getMessage()), e);
        }

        return value;
    }

    /**
     * Method to set an object at the specified position in the JDBC PreparedStatement.
     * @param ps The PreparedStatement
     * @param param Parameter position
     * @param value The value to set
     */
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getTypeInfo().getDataType());
            }
            else
            {
                if (value instanceof Boolean)
                {
                    ps.setString(param, ((Boolean) value).booleanValue() ? "Y" : "N");
                }
                else if (value instanceof java.sql.Time)
                {
                    ps.setString(param, ((java.sql.Time) value).toString());
                }
                else if (value instanceof java.sql.Date)
                {
                    ps.setString(param, ((java.sql.Date) value).toString());
                }
                else if (value instanceof java.sql.Timestamp)
                {
                    Calendar cal = storeMgr.getCalendarForDateTimezone();
                    if (cal != null)
                    {
                        // Note that passing the calendar to oracle makes it loses milliseconds
                        ps.setTimestamp(param, (Timestamp) value, cal);
                    }
                    else
                    {
                        ps.setTimestamp(param, (Timestamp) value);
                    }
                }
                else if (value instanceof java.util.Date)
                {
                    ps.setString(param, getJavaUtilDateFormat().format((java.util.Date) value));
                }
                else if (value instanceof String)
                {
                    ps.setString(param, ((String) value));
                }
                else
                {
                    // This caters for all non-string types. If any more need specific treatment, split them out above.
                    ps.setString(param, value.toString());
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + value, column, e.getMessage()), e);
        }
    }

    /**
     * Method to extract an object from the ResultSet at the specified position
     * @param rs The Result Set
     * @param param The parameter position
     * @return the object
     */
    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            String s = rs.getString(param);

            if (s == null)
            {
                value = null;
            }
            else
            {
                if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_BOOLEAN))
                {
                    if (s.equals("Y"))
                    {
                        value = Boolean.TRUE;
                    }
                    else if (s.equals("N"))
                    {
                        value = Boolean.FALSE;
                    }
                    else
                    {
                        throw new NucleusDataStoreException(Localiser.msg("055003", column));
                    }
                }
                else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_LANG_CHARACTER))
                {
                    value = Character.valueOf(s.charAt(0));
                }
                else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_SQL_TIME))
                {
                    value = java.sql.Time.valueOf(s);
                }
                else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_SQL_TIMESTAMP))
                {
                    Calendar cal = storeMgr.getCalendarForDateTimezone();
                    value = TypeConversionHelper.stringToTimestamp(s, cal);
                }
                else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_SQL_DATE))
                {
                    value = java.sql.Date.valueOf(s);
                }
                else if (getJavaTypeMapping().getJavaType().getName().equals(ClassNameConstants.JAVA_UTIL_DATE))
                {
                    value = getJavaUtilDateFormat().parse(s);
                }
                else
                {
                    value = s;
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
        }
        catch (ParseException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
        }

        return value;
    }

    /**
     * Get a Format object to handle java.util.Date. If a TimeZone is present, it will be used to format dates
     * to that zone.
     * @return Date formatter to use
     */
    public SimpleDateFormat getJavaUtilDateFormat()
    {
        FormatterInfo formatInfo = formatterThreadInfo.get();
        if (formatInfo.formatter == null)
        {
            Calendar cal = storeMgr.getCalendarForDateTimezone();
            formatInfo.formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            if (cal != null)
            {
                formatInfo.formatter.setTimeZone(cal.getTimeZone());
            }
        }
        return formatInfo.formatter;
    }
}