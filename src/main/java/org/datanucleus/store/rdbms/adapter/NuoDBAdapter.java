/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

/**
 * Adapter for NuoDB (http://www.nuodb.com).
 * This adapter was written based on v2.0.2 of the NuoDB JDBC driver.
 */
public class NuoDBAdapter extends BaseDatastoreAdapter
{
    public static final String NONSQL92_RESERVED_WORDS =
            "BIGINT,BINARY,BLOB,BOOLEAN,CLOB,LIMIT,NCLOB,OFFSET,ROLE,TRIGGER";

    public static final String NUODB_EXTRA_RESERVED_WORDS =
            "BITS,BREAK,CATCH,CONTAINING,END_FOR,END_IF,END_PROCEDURE,END_TRIGGER,END_TRY,END_WHILE,ENUM,FOR_UPDATE,IF," +
            "LOGICAL_AND,LOGICAL_NOT,LOGICAL_OR,NEXT_VALUE,NOT_BETWEEN,NOT_CONTAINING,NOT_IN,NOT_LIKE,NOT_STARTING,NVARCHAR,OFF," +
            "RECORD_BATCHING,REGEXP,SHOW,SMALLDATETIME,STARTING,STRING_TYPE,THROW,TINYBLOB,TINYINT,TRY,VAR,VER";

    public NuoDBAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(NONSQL92_RESERVED_WORDS));
        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(NUODB_EXTRA_RESERVED_WORDS));

        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(LOCK_ROW_USING_SELECT_FOR_UPDATE);
        supportedOptions.add(STORED_PROCEDURES);

        // NuoDB JDBC driver doesn't specify lengths in 2.0.2
        if (maxTableNameLength <= 0)
        {
            maxTableNameLength = 128;
        }
        if (maxColumnNameLength <= 0)
        {
            maxColumnNameLength = 128;
        }
        if (maxConstraintNameLength <= 0)
        {
            maxConstraintNameLength = 128;
        }
        if (maxIndexNameLength <= 0)
        {
            maxIndexNameLength = 128;
        }

        // CROSS JOIN syntax is not supported
        supportedOptions.remove(ANSI_CROSSJOIN_SYNTAX);
        supportedOptions.add(CROSSJOIN_ASINNER11_SYNTAX);

        // Doesn't seem to support FK constraints
        supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
        supportedOptions.remove(FK_DELETE_ACTION_NULL);
        supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
        supportedOptions.remove(FK_UPDATE_ACTION_NULL);
        supportedOptions.remove(FK_DELETE_ACTION_CASCADE);
        supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
        supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);

        supportedOptions.remove(RESULTSET_TYPE_SCROLL_SENSITIVE);
        supportedOptions.remove(RESULTSET_TYPE_SCROLL_INSENSITIVE);

        supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
        supportedOptions.remove(TX_ISOLATION_READ_UNCOMMITTED);
        supportedOptions.remove(TX_ISOLATION_NONE);

        supportedOptions.remove(ACCESS_PARENTQUERY_IN_SUBQUERY_JOINED);

        supportedOptions.add(OPERATOR_BITWISE_AND);
        supportedOptions.add(OPERATOR_BITWISE_OR);
        supportedOptions.add(OPERATOR_BITWISE_XOR);
    }

    public String getVendorID()
    {
        return "nuodb";
    }

    public String getCatalogName(Connection conn) throws SQLException
    {
        return null;
    }

    public String getSchemaName(Connection conn) throws SQLException 
    {
        Statement stmt = conn.createStatement();
        try 
        {
            String stmtText = "SELECT CURRENT_SCHEMA FROM DUAL";
            ResultSet rs = stmt.executeQuery(stmtText);
            try 
            {
                if (!rs.next()) 
                {
                    throw new NucleusDataStoreException("No result returned from " + stmtText).setFatal();
                }
                return rs.getString(1);
            }
            finally 
            {
                rs.close();
            }
        }
        finally
        {
            stmt.close();
        }
    }

    public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn) 
    {
        super.initialiseTypes(handler, mconn);

        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.adapter.NuoDBTypeInfo(
                "FLOAT", (short) Types.DOUBLE, 53, null, null, null, 1, false, (short) 2,
                false, false, false, null, (short) 0, (short) 0, 2);
        addSQLTypeForJDBCType(handler, mconn, (short) Types.DOUBLE, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.NuoDBTypeInfo(
                "TEXT", (short) Types.CLOB, 2147483647, null, null, null, 1, true, (short) 1,
                false, false, false, "TEXT", (short) 0, (short) 0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short) Types.CLOB, sqlType, true);
    }

    /**
     * Returns the appropriate DDL to create an index. It should return something like:
     * <pre>
     * CREATE [UNIQUE ]INDEX FOO_N1 ON FOO (BAR,BAZ) [Extended Settings]
     * </pre>
     * @param idx An object describing the index.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getCreateIndexStatement(Index idx, IdentifierFactory factory)
    {
        // ? Doesn't use fully qualified form of the index. Is this correct?
        StringBuilder str = new StringBuilder("CREATE ").append((idx.getUnique() ? "UNIQUE " : "")).append("INDEX ");
        str.append(factory.getIdentifierInAdapterCase(idx.getName()));
        str.append(" ON ").append(idx.getTable().toString()).append(" ").append(idx.getColumnList(supportsOption(CREATE_INDEX_COLUMN_ORDERING)));

        String extendedSetting = idx.getValueForExtension(Index.EXTENSION_INDEX_EXTENDED_SETTING);
        if (extendedSetting != null)
        {
            str.append(" ").append(extendedSetting);
        }

        return str.toString();
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        return "DROP SCHEMA " + schemaName + " CASCADE";
    }

    /**
     * Override the default implementation since we accept the PK in the CREATE TABLE statement.
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        // We generate the PK in the CREATE TABLE statement
        return null;
    }

    public boolean sequenceExists(Connection conn, String catalogName, String schemaName, String seqName)
    {
        /*PreparedStatement ps = null;
        ResultSet rs = null;
        try
        {
            String GET_SEQUENCE_STMT = "SELECT * FROM SYSTEM.SEQUENCES";

            NucleusLogger.DATASTORE_SCHEMA.debug("Retrieving sequence info using the following SQL : " + GET_SEQUENCE_STMT);
            ps = conn.prepareStatement(GET_SEQUENCE_STMT);
            rs = ps.executeQuery();
            while (rs.next())
            {
                
            }
        }
        catch (SQLException sqle)
        {
            NucleusLogger.DATASTORE_SCHEMA.warn(">> Exception caught", sqle);
        }
        finally
        {
            try
            {
                if (rs != null && !rs.isClosed())
                {
                    rs.close();
                }
                ps.close();
            }
            catch (SQLException sqle)
            {

            }
        }*/

        // TODO Make use of the following
        // SELECT * FROM SYSTEM.SEQUENCES;
        //SCHEMA SEQUENCENAME
        //====== ============
        //HOCKEY HOCKEY$IDENTITY_SEQUENCE
        return true;
    }

    /**
     * Accessor for the sequence statement to create the sequence.
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
        if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        // TODO Support other parameters if NuoDB ever supports them

        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence for this datastore.
     * @param sequenceName Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequenceName)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }
        StringBuilder stmt=new StringBuilder("SELECT NEXT VALUE FOR ");
        stmt.append(sequenceName);
        stmt.append(" FROM DUAL");

        return stmt.toString();
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     */
    public String getIdentityKeyword()
    {
        return "GENERATED BY DEFAULT AS IDENTITY";
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
     * restriction of ranges using the OFFSET/FETCH keywords.
     * @param offset The offset to return from
     * @param count The number of items to return
     * @param hasOrdering Whether there is ordering present
     * @return The SQL to append to allow for ranges using OFFSET/FETCH.
     */
    @Override
    public String getRangeByLimitEndOfStatementClause(long offset, long count, boolean hasOrdering)
    {
        if (datastoreMajorVersion < 10 || (datastoreMajorVersion == 10 && datastoreMinorVersion < 5))
        {
            return "";
        }
        else if (offset <= 0 && count <= 0)
        {
            return "";
        }

        StringBuilder str = new StringBuilder();
        if (offset > 0)
        {
            str.append("OFFSET " + offset + (offset > 1 ? " ROWS " : " ROW "));
        }
        if (count > 0)
        {
            str.append("FETCH NEXT " + (count > 1 ? (count + " ROWS ONLY ") : "ROW ONLY "));
        }
        return str.toString();
    }

    public String getDatastoreDateStatement()
    {
        return "SELECT CURRENT_DATE FROM DUAL";
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
            else if ("WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalWeekMethod5.class;
            else if ("QUARTER".equals(methodName)) return null; // TODO Support this
        }
        else
        {
            Class cls = null;
            try
            {
                cls = clr.classForName(className);
            }
            catch (ClassNotResolvedException cnre) {}

            if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
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

    /**
     * Load all datastore mappings for this RDBMS database.
     * @param mgr the PluginManager
     * @param clr the ClassLoaderResolver
     */
    protected void loadColumnMappings(PluginManager mgr, ClassLoaderResolver clr)
    {
        // Load up built-in types for this datastore
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BitColumnMapping.class, JDBCType.BIT, "BIT", true);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.BOOLEAN, "BOOLEAN", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);

        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);

        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", true);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);

        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", true);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.ClobColumnMapping.class, JDBCType.CLOB, "CLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NVarcharColumnMapping.class, JDBCType.NVARCHAR, "NVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NCharColumnMapping.class, JDBCType.NCHAR, "NCHAR", false);

        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", true);
        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);

        registerColumnMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", true);

        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", true);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", true);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);

        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerColumnMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryStreamColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

        super.loadColumnMappings(mgr, clr);
    }
}