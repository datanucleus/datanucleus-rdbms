/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.adapter;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;

/**
 * Provides methods for adapting SQL language elements to the SQLite database.
 */
public class SQLiteAdapter extends BaseDatastoreAdapter
{
    protected static final int MAX_IDENTIFIER_LENGTH = 128;

    /**
     * Constructor.
     * @param metadata MetaData for the Database
     */
    public SQLiteAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(CHECK_IN_END_CREATE_STATEMENTS);
        supportedOptions.add(UNIQUE_IN_END_CREATE_STATEMENTS);
        supportedOptions.add(FK_IN_END_CREATE_STATEMENTS);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(AUTO_INCREMENT_PK_IN_CREATE_TABLE_COLUMN_DEF);
        supportedOptions.add(ORDERBY_NULLS_USING_COLUMN_IS_NULL);

        supportedOptions.remove(TX_ISOLATION_READ_COMMITTED);
        supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
        supportedOptions.remove(AUTO_INCREMENT_KEYS_NULL_SPECIFICATION);

        supportedOptions.remove(RIGHT_OUTER_JOIN);
        supportedOptions.remove(SOME_ANY_ALL_SUBQUERY_EXPRESSIONS);
        supportedOptions.remove(UPDATE_STATEMENT_ALLOW_TABLE_ALIAS_IN_SET_CLAUSE);
        supportedOptions.remove(UPDATE_DELETE_STATEMENT_ALLOW_TABLE_ALIAS_IN_WHERE_CLAUSE);

        // SQLite (xerial driver) returns 0 for these! The 18 chosen here is totally arbitrary
        maxTableNameLength = MAX_IDENTIFIER_LENGTH;
        maxColumnNameLength = MAX_IDENTIFIER_LENGTH;
        maxConstraintNameLength = MAX_IDENTIFIER_LENGTH;
        maxIndexNameLength = MAX_IDENTIFIER_LENGTH;
    }

    @Override
    public String getVendorID()
    {
        return "sqlite";
    }

    @Override
    public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn)
    {
        super.initialiseTypes(handler, mconn);

        // Add on any missing JDBC types not provided by JDBC driver (SQLite only provides NULL, REAL, BLOB, INTEGER, TEXT)

        // DOUBLE -> "double"
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "double", (short)Types.DOUBLE, 0, null, null, null, 1, true, (short)3, false, false, false, "double", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DOUBLE, sqlType, true);

        // FLOAT -> "float"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "float", (short)Types.FLOAT, 0, null, null, null, 1, true, (short)3, false, false, false, "float", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.FLOAT, sqlType, true);

        // DECIMAL -> "float"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "decimal", (short)Types.DECIMAL, 0, null, null, null, 1, true, (short)3, false, false, false, "decimal", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DECIMAL, sqlType, true);

        // NUMERIC -> "numeric"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "numeric", (short)Types.NUMERIC, 0, null, null, null, 1, true, (short)3, false, false, false, "numeric", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.NUMERIC, sqlType, true);

        // BOOLEAN -> "integer"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "integer", (short)Types.BOOLEAN, 0, null, null, null, 1, true, (short)3, false, false, false, "integer", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BOOLEAN, sqlType, true);

        // BIT -> "integer"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "integer", (short)Types.BIT, 0, null, null, null, 1, true, (short)3, false, false, false, "integer", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BIT, sqlType, true);

        // TINYINT -> "tinyint"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "tinyint", (short)Types.TINYINT, 0, null, null, null, 1, true, (short)3, false, false, false, "tinyint", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.TINYINT, sqlType, true);

        // SMALLINT -> "smallint"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "smallint", (short)Types.SMALLINT, 0, null, null, null, 1, true, (short)3, false, false, false, "smallint", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.SMALLINT, sqlType, true);

        // BIGINT -> "bigint"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "bigint", (short)Types.BIGINT, 0, null, null, null, 1, true, (short)3, false, false, false, "bigint", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BIGINT, sqlType, true);

        // CHAR -> "char"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "char", (short)Types.CHAR, 255, null, null, null, 1, true, (short)3, false, false, false, "char", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CHAR, sqlType, true);

        // VARCHAR -> "varchar"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "varchar", (short)Types.VARCHAR, 255, null, null, null, 1, true, (short)3, false, false, false, "varchar", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.VARCHAR, sqlType, true);

        // LONGVARCHAR -> "longvarchar"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "longvarchar", (short)Types.LONGVARCHAR, 16777215, null, null, null, 1, true, (short)3, false, false, false, "longvarchar", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.LONGVARCHAR, sqlType, true);

        // CLOB -> "clob"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "clob", (short)Types.CLOB, 2147483647, null, null, null, 1, true, (short)3, false, false, false, "clob", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);

        // DATE -> "date"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "date", (short)Types.DATE, 0, null, null, null, 1, true, (short)3, false, false, false, "date", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DATE, sqlType, true);

        // TIME -> "time"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "time", (short)Types.TIME, 0, null, null, null, 1, true, (short)3, false, false, false, "time", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.TIME, sqlType, true);

        // TIMESTAMP -> "timestamp"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "timestamp", (short)Types.TIMESTAMP, 0, null, null, null, 1, true, (short)3, false, false, false, "timestamp", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.TIMESTAMP, sqlType, true);

        // BINARY -> "blob"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "blob", (short)Types.BINARY, 255, null, null, null, 1, true, (short)3, false, false, false, "blob", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BINARY, sqlType, true);

        // VARBINARY -> "blob"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "blob", (short)Types.VARBINARY, 255, null, null, null, 1, true, (short)3, false, false, false, "blob", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.VARBINARY, sqlType, true);

        // LONGVARBINARY -> "blob"
        sqlType = new org.datanucleus.store.rdbms.adapter.SQLiteTypeInfo(
            "blob", (short)Types.LONGVARBINARY, 16777215, null, null, null, 1, true, (short)3, false, false, false, "blob", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.LONGVARBINARY, sqlType, true);
    }

    public String getCreateDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("SQLite does not support CREATE SCHEMA; everything is in a single schema");
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("SQLite does not support DROP SCHEMA; everything is in a single schema");
    }

    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    @Override
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        // ALTER TABLE ADD CONSTRAINT Syntax not supported
        return null;
    }

    @Override
    public String getAddCandidateKeyStatement(CandidateKey ck, IdentifierFactory factory)
    {
        // ALTER TABLE ADD CONSTRAINT Syntax not supported
        return null;
    }

    @Override
    public String getAddForeignKeyStatement(ForeignKey fk, IdentifierFactory factory)
    {
        // ALTER TABLE ADD CONSTRAINT Syntax not supported
        return null;
    }

    /**
     * Method to return the SQLText for an UPDATE TABLE statement.
     * Returns the SQLText for <code>UPDATE SCH1.TBL1 SET x1 = val1, x2 = val2</code> since SQLite doesn't allow any aliases in UPDATEs.
     * @param tbl The primary table
     * @param setSQL The SQLText for the SET component
     * @return SQLText for the update statement
     */
    public SQLText getUpdateTableStatement(SQLTable tbl, SQLText setSQL)
    {
        SQLText sql = new SQLText("UPDATE ").append(tbl.getTable().toString()); // "UPDATE SCH1.TBL1"
        sql.append(" ").append(setSQL); // " SET x1 = val1, x2 = val2"
        return sql;
    }

    /**
     * Method to return the basic SQL for a DELETE TABLE statement.
     * Returns the String as <code>DELETE FROM SCH1.TBL1</code> since SQLite doesn't allow any aliases in DELETEs.
     * @param tbl The SQLTable to delete
     * @return The delete table string
     */
    public String getDeleteTableStatement(SQLTable tbl)
    {
        return "DELETE FROM " + tbl.getTable().toString(); // "DELETE FROM SCH1.TBL1"
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
     * restriction of ranges using the LIMIT keyword.
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
            return "LIMIT " + count + " OFFSET " + offset + " ";
        }
        else if (offset <= 0 && count > 0)
        {
            return "LIMIT " + count + " ";
        }
        else if (offset >= 0 && count < 0)
        {
            // SQLite doesn't allow just offset so use Long.MAX_VALUE as count
            return "LIMIT " + Long.MAX_VALUE + " OFFSET " + offset + " ";
        }
        else
        {
            return "";
        }
    }

    @Override
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        return "SELECT last_insert_rowid()";
    }

    @Override
    public String getAutoIncrementKeyword()
    {
        return "autoincrement";
    }

    @Override
    public Class getAutoIncrementJavaTypeForType(Class type)
    {
        // SQLite only seems to allow INTEGER columns for AUTOINCREMENT
        if (type.isPrimitive())
        {
            return int.class;
        }
        return Integer.class;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLMethodClass(java.lang.String, java.lang.String)
     */
    @Override
    public Class getSQLMethodClass(String className, String methodName, ClassLoaderResolver clr)
    {
        if (className == null)
        {
            if ("YEAR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod4.class;
            else if ("MONTH".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod4.class;
            else if ("MONTH_JAVA".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthJavaMethod4.class;
            else if ("DAY".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod4.class;
            else if ("DAY_OF_WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod5.class;
            else if ("HOUR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod5.class;
            else if ("MINUTE".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod5.class;
            else if ("SECOND".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod6.class;
        }
        else
        {
            Class cls = null;
            try
            {
                cls = clr.classForName(className);
            }
            catch (ClassNotResolvedException cnre) {}

            if ("java.lang.String".equals(className))
            {
                if ("indexOf".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringIndexOf2Method.class;
                else if ("length".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringLength3Method.class;
                else if ("substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring3Method.class;
            }
            else if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
            {
                if ("getDay".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod4.class;
                else if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod5.class;
                else if ("getDate".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod4.class;
                else if ("getMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthJavaMethod4.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod4.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod5.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod5.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod6.class;
            }
            else if ("java.time.LocalTime".equals(className))
            {
                if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod5.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod5.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod6.class;
            }
            else if ("java.time.LocalDate".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod4.class;
                else if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod5.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod4.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod4.class;
            }
            else if ("java.time.LocalDateTime".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod4.class;
                else if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod5.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod4.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod4.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod5.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod5.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod6.class;
            }
            else if ("java.time.MonthDay".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod4.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod4.class;
            }
            else if ("java.time.Period".equals(className))
            {
                if ("getMonths".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod4.class;
                else if ("getDays".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod4.class;
                else if ("getYears".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod4.class;
            }
            else if ("java.time.YearMonth".equals(className))
            {
                if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod4.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod4.class;
            }
        }

        return super.getSQLMethodClass(className, methodName, clr);
    }
}