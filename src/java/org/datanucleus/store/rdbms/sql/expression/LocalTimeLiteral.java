/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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

import java.util.Calendar;
import java.util.GregorianCalendar;

import javax.time.calendar.LocalTime;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.types.converters.LocalTimeStringConverter;

/**
 * Representation of an javax.time.calendar "LocalTime" literal.
 */
public class LocalTimeLiteral extends JavaxTimeLiteral
{
    private final LocalTime value;

    /**
     * Constructor for an LocalTime literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public LocalTimeLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, mapping, parameterName);

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof LocalTime)
        {
            this.value = (LocalTime)value;
        }
        else
        {
            throw new NucleusException("Cannot create " + this.getClass().getName() +
                " for value of type " + value.getClass().getName());
        }

        if (mapping.getJavaTypeForDatastoreMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            String str = new LocalTimeStringConverter().toDatastoreType((LocalTime)value);
            delegate = new StringLiteral(stmt, mapping,
                (this.value != null ? str : null), parameterName);
        }
        else if (this.value == null)
        {
            delegate = new TemporalLiteral(stmt, mapping, null, parameterName);
        }
        else
        {
            LocalTime localTime = (LocalTime)value;
            Calendar cal = GregorianCalendar.getInstance();
            cal.set(Calendar.HOUR_OF_DAY, localTime.getHourOfDay());
            cal.set(Calendar.MINUTE, localTime.getMinuteOfHour());
            cal.set(Calendar.SECOND, localTime.getSecondOfMinute());
            // TODO Handle milli/nanoseconds
            delegate = new TemporalLiteral(stmt, mapping, cal.getTime(), parameterName);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#getValue()
     */
    public Object getValue()
    {
        return value;
    }
}