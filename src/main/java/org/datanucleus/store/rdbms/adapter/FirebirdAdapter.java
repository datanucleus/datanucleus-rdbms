/**********************************************************************
Copyright (c) 2002 David Jencks and others. All rights reserved.
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
2003 Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;

/**
 * Provides methods for adapting SQL language elements to the Firebird database.
 * @see BaseDatastoreAdapter
 */
public class FirebirdAdapter extends BaseDatastoreAdapter
{
    public FirebirdAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(BOOLEAN_COMPARISON);
        supportedOptions.remove(NULLS_IN_CANDIDATE_KEYS);
        supportedOptions.remove(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
        supportedOptions.remove(INCLUDE_ORDERBY_COLS_IN_SELECT);
        supportedOptions.add(ALTER_TABLE_DROP_FOREIGN_KEY_CONSTRAINT);
        supportedOptions.add(CREATE_INDEXES_BEFORE_FOREIGN_KEYS);
        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(GROUP_BY_REQUIRES_ALL_SELECT_PRIMARIES);

        // Firebird doesn't hold cursors over commit so have to load all results before committing connection
        supportedOptions.remove(HOLD_CURSORS_OVER_COMMIT);

        if (datastoreMajorVersion < 2)
        {
            // CROSS JOIN syntax is not supported before Firebird 2
            supportedOptions.remove(ANSI_CROSSJOIN_SYNTAX);
            supportedOptions.add(CROSSJOIN_ASINNER11_SYNTAX);
        }
    }

    public String getVendorID()
    {
        return "firebird";
    }

    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.adapter.FirebirdTypeInfo(rs);
    }

    /**
     * Firebird accepts the PK in the CREATE TABLE statement.
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        return null;
    }

    /**
     * Accessor for the sequence create statement for this datastore.
     * @param sequenceName Name of the sequence 
     * @param min Minimum value for the sequence
     * @param max Maximum value for the sequence
     * @param start Start value for the sequence
     * @param increment Increment value for the sequence
     * @param cacheSize Cache size for the sequence
     * @return The statement for getting the next id from the sequence
     */
    public String getSequenceCreateStmt(String sequenceName, Integer min, Integer max, Integer start, Integer increment, Integer cacheSize)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequenceName);
        // TODO Support the other parameters if Firebird ever supports specification of such with its sequences

        return stmt.toString();
    }

    /**
     * Accessor for the sequence statement to get the next id for this datastore.
     * @param sequenceName Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequenceName)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt=new StringBuilder("SELECT GEN_ID(");
        stmt.append(sequenceName);
        stmt.append(",1) FROM RDB$DATABASE");

        return stmt.toString();
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
     * restriction of ranges using the ROWS keyword.
     * @param offset The offset to return from
     * @param count The number of items to return
     * @param hasOrdering Whether ordering is present
     * @return The SQL to append to allow for ranges using LIMIT.
     */
    @Override
    public String getRangeByLimitEndOfStatementClause(long offset, long count, boolean hasOrdering)
    {
        if (offset >= 0 && count > 0)
        {
            return "ROWS " + (offset+1) + " TO " + (offset+count) + " ";
        }
        else if (offset <= 0 && count > 0)
        {
            return "ROWS 1 TO " + count + " ";
        }
        else if (offset >= 0 && count < 0)
        {
            return "ROWS " + offset + " ";
        }
        else
        {
            return "";
        }
    }

    public boolean supportsCharLengthFunction()
    {
        // CHAR_LENGTH supported in v2+
        return datastoreMajorVersion > 1;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLMethodClass(java.lang.String, java.lang.String)
     */
    @Override
    public Class getSQLMethodClass(String className, String methodName, ClassLoaderResolver clr)
    {
        if (className == null)
        {
            if ("YEAR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
            else if ("MONTH".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
            else if ("MONTH_JAVA".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthJavaMethod5.class;
            else if ("DAY".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
            else if ("DAY_OF_WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod6.class;
            else if ("HOUR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
            else if ("MINUTE".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
            else if ("SECOND".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod7.class;
        }
        else
        {
            Class cls = null;
            try
            {
                cls = clr.classForName(className);
            }
            catch (ClassNotResolvedException cnre) {}

            if ("java.lang.String".equals(className) && "length".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringLength2Method.class;
            else if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
            {
                if ("getDay".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
                else if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod6.class;
                else if ("getDate".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
                else if ("getMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthJavaMethod5.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod7.class;
            }
            else if ("java.time.LocalTime".equals(className))
            {
                if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod6.class;
            }
            else if ("java.time.LocalDate".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
                else if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod6.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
            }
            else if ("java.time.LocalDateTime".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
                else if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod6.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod6.class;
            }
            else if ("java.time.MonthDay".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
                else if ("java.time.MonthDay".equals(className) && "getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
            }
            else if ("java.time.Period".equals(className))
            {
                if ("getMonths".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
                else if ("getDays".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
                else if ("getYears".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
            }
            else if ("java.time.YearMonth".equals(className))
            {
                if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
            }
        }

        return super.getSQLMethodClass(className, methodName, clr);
    }
}