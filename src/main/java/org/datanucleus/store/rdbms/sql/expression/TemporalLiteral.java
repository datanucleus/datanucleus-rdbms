/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.sql.expression;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.mapping.column.CharColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Representation of temporal literal in a Query.
 * Can be used for anything based on java.util.Date.
 * Also supports a temporal literal specified as a String in JDBC escape syntax i.e "{d 'yyyy-mm-dd'}", "{t 'hh:mm:ss'}", "{ts 'yyyy-mm-dd hh:mm:ss.f...'}"
 */
public class TemporalLiteral extends TemporalExpression implements SQLLiteral
{
    private final Date value;

    private String jdbcEscapeValue;

    /**
     * Constructor for a temporal literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public TemporalLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof Date)
        {
            this.value = (Date)value;
        }
        else if (value instanceof Calendar)
        {
            this.value = ((Calendar)value).getTime();
        }
        else if (value instanceof String)
        {
            // JDBC escape syntax
            this.value = null;
            this.jdbcEscapeValue = (String) value;
        }
        else
        {
            Class type = value.getClass();
            if (mapping != null)
            {
                type = mapping.getJavaType();
            }

            // Cater for any TypeConverter that converts to java.sql.Time/java.sql.Date/java.sql.Timestamp/java.util.Date
            TypeConverter converter = stmt.getRDBMSManager().getNucleusContext().getTypeManager().getTypeConverterForType(type, Time.class);
            if (converter != null)
            {
                // Use converter
                this.value = (Time)converter.toDatastoreType(value);
            }
            else
            {
                converter = stmt.getRDBMSManager().getNucleusContext().getTypeManager().getTypeConverterForType(type, java.sql.Date.class);
                if (converter != null)
                {
                    // Use converter
                    this.value = (java.sql.Date)converter.toDatastoreType(value);
                }
                else
                {
                    converter = stmt.getRDBMSManager().getNucleusContext().getTypeManager().getTypeConverterForType(type, Timestamp.class);
                    if (converter != null)
                    {
                        // Use converter
                        this.value = (Timestamp)converter.toDatastoreType(value);
                    }
                    else
                    {
                        converter = stmt.getRDBMSManager().getNucleusContext().getTypeManager().getTypeConverterForType(type, Date.class);
                        if (converter != null)
                        {
                            // Use converter
                            this.value = (Date)converter.toDatastoreType(value);
                        }
                        else
                        {
                            // Allow for using an input parameter literal
                            throw new NucleusException("Cannot create " + this.getClass().getName() + " for value of type " + value.getClass().getName());
                        }
                    }
                }
            }
        }

        if (parameterName != null)
        {
            st.appendParameter(parameterName, mapping, this.value);
        }
        else
        {
            setStatement();
        }
    }

    public String toString()
    {
        if (jdbcEscapeValue != null)
        {
            return jdbcEscapeValue;
        }
        else if (value == null)
        {
            return "null";
        }
        return value.toString();
    }

    public SQLExpression invoke(String methodName, List args)
    {
        if (jdbcEscapeValue != null)
        {
            throw new NucleusUserException("Cannot invoke methods on TemporalLiteral using JDBC escape syntax - not supported");
        }

        if (parameterName == null)
        {
            if (methodName.equals("getDay"))
            {
                // Date.getDay()
                Calendar cal = Calendar.getInstance();
                cal.setTime(value);
                JavaTypeMapping m = stmt.getRDBMSManager().getMappingManager().getMapping(Integer.class);
                return new IntegerLiteral(stmt, m, Integer.valueOf(cal.get(Calendar.DAY_OF_MONTH)), null);
            }
            else if (methodName.equals("getMonth"))
            {
                // Date.getMonth()
                Calendar cal = Calendar.getInstance();
                cal.setTime(value);
                JavaTypeMapping m = stmt.getRDBMSManager().getMappingManager().getMapping(Integer.class);
                return new IntegerLiteral(stmt, m, Integer.valueOf(cal.get(Calendar.MONTH)), null);
            }
            else if (methodName.equals("getYear"))
            {
                // Date.getMonth()
                Calendar cal = Calendar.getInstance();
                cal.setTime(value);
                JavaTypeMapping m = stmt.getRDBMSManager().getMappingManager().getMapping(Integer.class);
                return new IntegerLiteral(stmt, m, Integer.valueOf(cal.get(Calendar.YEAR)), null);
            }
            else if (methodName.equals("getHour"))
            {
                // Date.getHour()
                Calendar cal = Calendar.getInstance();
                cal.setTime(value);
                JavaTypeMapping m = stmt.getRDBMSManager().getMappingManager().getMapping(Integer.class);
                return new IntegerLiteral(stmt, m, Integer.valueOf(cal.get(Calendar.HOUR_OF_DAY)), null);
            }
            else if (methodName.equals("getMinutes"))
            {
                // Date.getMinutes()
                Calendar cal = Calendar.getInstance();
                cal.setTime(value);
                JavaTypeMapping m = stmt.getRDBMSManager().getMappingManager().getMapping(Integer.class);
                return new IntegerLiteral(stmt, m, Integer.valueOf(cal.get(Calendar.MINUTE)), null);
            }
            else if (methodName.equals("getSeconds"))
            {
                // Date.getMinutes()
                Calendar cal = Calendar.getInstance();
                cal.setTime(value);
                JavaTypeMapping m = stmt.getRDBMSManager().getMappingManager().getMapping(Integer.class);
                return new IntegerLiteral(stmt, m, Integer.valueOf(cal.get(Calendar.SECOND)), null);
            }
        }

        return super.invoke(methodName, args);
    }

    public Object getValue()
    {
        if (jdbcEscapeValue != null)
        {
            return jdbcEscapeValue;
        }
        return value;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#setNotParameter()
     */
    public void setNotParameter()
    {
        if (parameterName == null)
        {
            return;
        }
        parameterName = null;
        st.clearStatement();
        setStatement();
    }

    protected void setStatement()
    {
        String formatted;
        if (jdbcEscapeValue != null)
        {
            st.append(jdbcEscapeValue);
        }
        else
        {
            if (value instanceof java.sql.Time || value instanceof java.sql.Date || value instanceof java.sql.Timestamp)
            {
                // Use native format of the type
                formatted = value.toString();
            }
            else if (mapping.getColumnMapping(0) instanceof CharColumnMapping)
            {
                // Stored as String so use same formatting
                SimpleDateFormat fmt = ((CharColumnMapping)mapping.getColumnMapping(0)).getJavaUtilDateFormat();
                formatted = fmt.format(value);
            }
            else
            {
                // TODO Include more variations of inputting a Date into JDBC
                // TODO Cater for timezone storage options see TimestampColumnMapping
                formatted = new Timestamp(value.getTime()).toString();
            }
            st.append('\'').append(formatted).append('\'');
        }
    }
}