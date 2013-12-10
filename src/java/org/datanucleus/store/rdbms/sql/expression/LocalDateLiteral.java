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

import java.util.Date;

import javax.time.calendar.LocalDate;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.types.converters.LocalDateStringConverter;

/**
 * Representation of an javax.time.calendar "LocalDate" literal.
 */
public class LocalDateLiteral extends JavaxTimeLiteral
{
    private final LocalDate value;

    /**
     * Constructor for an LocalDate literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    @SuppressWarnings("deprecation")
    public LocalDateLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, mapping, parameterName);

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof LocalDate)
        {
            this.value = (LocalDate)value;
        }
        else
        {
            throw new NucleusException("Cannot create " + this.getClass().getName() +
                " for value of type " + value.getClass().getName());
        }

        if (mapping.getJavaTypeForDatastoreMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            String str = new LocalDateStringConverter().toDatastoreType((LocalDate)value);
            delegate = new StringLiteral(stmt, mapping,
                (this.value != null ? str : null), parameterName);
        }
        else if (this.value == null)
        {
            delegate = new TemporalLiteral(stmt, mapping, null, parameterName);
        }
        else
        {
            LocalDate localDate = (LocalDate)value;
            Date date = new Date();
            date.setYear(localDate.getYear());
            date.setMonth(localDate.getMonthOfYear().ordinal());
            date.setDate(localDate.getDayOfMonth());
            delegate = new TemporalLiteral(stmt, mapping, date, parameterName);
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