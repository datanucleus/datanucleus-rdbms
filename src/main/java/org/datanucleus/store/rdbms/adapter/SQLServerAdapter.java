/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved.
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
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the Microsoft SQL Server database.
 * Note that majorVersion from JDBC doesn't align to version number of SQLServer.
 * 1995 = "6", 1998 = "7", 2000/2003 = "8", 2005 = "9", 2008 = "10", 2012 = "11", 2014 = "12", ...
 *
 * @see BaseDatastoreAdapter
 */
public class SQLServerAdapter extends BaseDatastoreAdapter
{
    /**
     * Microsoft SQL Server 2000 uses reserved keywords for defining,
     * manipulating, and accessing databases. Reserved keywords are part of the
     * grammar of the Transact-SQL language used by SQL Server to parse and
     * understand Transact-SQL statements and batches. Although it is
     * syntactically possible to use SQL Server reserved keywords as identifiers
     * and object names in Transact-SQL scripts, this can be done only using
     * delimited identifiers.
     */
    private static final String MSSQL_RESERVED_WORDS =
        "ADD,ALL,ALTER,AND,ANY,AS," +
        "ASC,AUTHORIZATION,BACKUP,BEGIN,BETWEEN,BREAK," +
        "BROWSE,BULK,BY,CASCADE,CASE,CHECK," +
        "CHECKPOINT,CLOSE,CLUSTERED,COALESCE,COLLATE,COLUMN," +
        "COMMIT,COMPUTE,CONSTRAINT,CONTAINS,CONTAINSTABLE,CONTINUE," +
        "CONVERT,CREATE,CROSS,CURRENT,CURRENT_DATE,CURRENT_TIME," +
        "CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DBCC,DEALLOCATE,DECLARE," +
        "DEFAULT,DELETE,DENY,DESC,DISK,DISTINCT," +
        "DISTRIBUTED,DOUBLE,DROP,DUMMY,DUMP,ELSE," +
        "END,ERRLVL,ESCAPE,EXCEPT,EXEC,EXECUTE," +
        "EXISTS,EXIT,FETCH,FILE,FILLFACTOR,FOR," +
        "FOREIGN,FREETEXT,FREETEXTTABLE,FROM,FULL,FUNCTION," +
        "GOTO,GRANT,GROUP,HAVING,HOLDLOCK,IDENTITY," +
        "IDENTITY_INSERT,IDENTITYCOL,IF,IN,INDEX,INNER," +
        "INSERT,INTERSECT,INTO,IS,JOIN,KEY," +
        "KILL,LEFT,LIKE,LINENO,LOAD,NATIONAL," +
        "NOCHECK,NONCLUSTERED,NOT,NULL,NULLIF,OF," +
        "OFF,OFFSETS,ON,OPEN,OPENDATASOURCE,OPENQUERY," +
        "OPENROWSET,OPENXML,OPTION,OR,ORDER,OUTER," +
        "OVER,PERCENT,PLAN,PRECISION,PRIMARY,PRINT," +
        "PROC,PROCEDURE,PUBLIC,RAISERROR,READ,READTEXT," +
        "RECONFIGURE,REFERENCES,REPLICATION,RESTORE,RESTRICT,RETURN," +
        "REVOKE,RIGHT,ROLLBACK,ROWCOUNT,ROWGUIDCOL,RULE," +
        "SAVE,SCHEMA,SELECT,SESSION_USER,SET,SETUSER," +
        "SHUTDOWN,SOME,STATISTICS,SYSTEM_USER,TABLE,TEXTSIZE," +
        "THEN,TO,TOP,TRAN,DATABASE,TRANSACTION,TRIGGER," +
        "TRUNCATE,TSEQUAL,UNION,UNIQUE,UPDATE,UPDATETEXT," +
        "USE,USER,VALUES,VARYING,VIEW,WAITFOR," +
        "WHEN,WHERE,WHILE,WITH,WRITETEXT";
    
    /**
     * Constructs a SQL Server adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public SQLServerAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(MSSQL_RESERVED_WORDS));

        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(LOCK_ROW_USING_OPTION_AFTER_FROM);
        supportedOptions.add(LOCK_ROW_USING_OPTION_WITHIN_JOIN);
        supportedOptions.add(ANALYSIS_METHODS);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.add(ORDERBY_NULLS_USING_CASE_NULL);

        supportedOptions.remove(BOOLEAN_COMPARISON);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
        supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
        supportedOptions.remove(FK_DELETE_ACTION_NULL);
        supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
        supportedOptions.remove(FK_UPDATE_ACTION_NULL);

        if (datastoreMajorVersion >= 11)
        {
            // SQLServer 2012+ support these features
            supportedOptions.add(SEQUENCES);
        }

        if (datastoreMajorVersion >= 12)
        {
            // SQLServer 2014+ support these features (what about earlier?)
            supportedOptions.add(OPERATOR_BITWISE_AND);
            supportedOptions.add(OPERATOR_BITWISE_OR);
            supportedOptions.add(OPERATOR_BITWISE_XOR);
        }
    }

    /**
     * Initialise the types for this datastore.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn)
    {
        super.initialiseTypes(handler, mconn);

        // Add on any missing JDBC types
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
            "UNIQUEIDENTIFIER", (short)Types.CHAR, 36, "'", "'", "", 1, false, (short)2, false, false, false, "UNIQUEIDENTIFIER", (short)0, (short)0, 10);
        sqlType.setAllowsPrecisionSpec(false);
        addSQLTypeForJDBCType(handler, mconn, (short)SQLServerTypeInfo.UNIQUEIDENTIFIER, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
            "IMAGE", (short)Types.BLOB, 2147483647, null, null, null, 1, false, (short)1, false, false, false, "BLOB", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BLOB, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
            "varbinary", (short)Types.VARBINARY, 8000, "0x", null, "(max)", 1, false, (short)2, false, false, false, "varbinary", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.VARBINARY, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
            "TEXT", (short)Types.CLOB, 2147483647, null, null, null, 1, true, (short)1, false, false, false, "TEXT", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
            "float", (short)Types.DOUBLE, 53, null, null, null, 1, false, (short)2, false, false, false, null, (short)0, (short)0, 2);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DOUBLE, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
            "IMAGE", (short)Types.LONGVARBINARY, 2147483647, null, null, null, 1, false, (short)1, false, false, false, "LONGVARBINARY", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.LONGVARBINARY, sqlType, true);

        if (datastoreMajorVersion > 9)
        {
            // Support for build-in TIME and DATE data type for MS SQL Server version >= 2008
            sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
                "TIME", (short)Types.TIME, 0, null, null, null, 1, false, (short)1, true, true, false, "TIME", (short)0, (short)0, 0);
            addSQLTypeForJDBCType(handler, mconn, (short)Types.TIME, sqlType, true);
            sqlType = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(
                "DATE", (short)Types.DATE, 0, null, null, null, 1, false, (short)1, true, true, false, "DATE", (short)0, (short)0, 0);
            addSQLTypeForJDBCType(handler, mconn, (short)Types.DATE, sqlType, true);
        }
    }

    public String getVendorID()
    {
        return "sqlserver";
    }

    /**
     * Accessor for the catalog name.
     * @param conn The Connection to use
     * @return The catalog name used by this connection
     * @throws SQLException if an error occurs
     */
    public String getCatalogName(Connection conn)
    throws SQLException
    {
        String catalog = conn.getCatalog();
        // the ProbeTable approach returns empty string instead of null here,
        // so do the same
        return catalog != null ? catalog : "";
    }

    public String getSchemaName(Connection conn) throws SQLException
    {
        if (datastoreMajorVersion >= 9) // SQLServer 2005 onwards
        {
            // Since SQL Server 2005, SCHEMA_NAME() will return the current schema name of the caller
            Statement stmt = conn.createStatement();
            try
            {
                String stmtText = "SELECT SCHEMA_NAME();";
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

        /*
         * As of version 7 there was no equivalent to the concept of "schema"
         * in SQL Server.  For DatabaseMetaData functions that include
         * SCHEMA_NAME drivers usually return the user name that owns the table.
         *
         * So the default ProbeTable method for determining the current schema
         * just ends up returning the current user.  If we then use that name in
         * performing metadata queries our results may get filtered down to just
         * objects owned by that user.  So instead we report the schema name as
         * null which should cause those queries to return everything.
         * 
         * DO not use the user name here, as in MSSQL, you are able to use
         * an user name and access any schema.
         * 
         * Use an empty string, otherwise fullyqualified object name is invalid
         * In MSSQL, fully qualified must include the object owner, or an empty owner
         * <catalog>.<schema>.<object> e.g. mycatalog..mytable 
         */
        return "";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#isReservedKeyword(java.lang.String)
     */
    @Override
    public boolean isReservedKeyword(String word)
    {
        if (super.isReservedKeyword(word))
        {
            return true;
        }
        // MSSQL also allows tables with spaces, and these need escaping.
        if (word.indexOf(' ') >= 0)
        {
            return true;
        }
        return false;
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("SQLServer does not support dropping schema with cascade. You need to drop all tables first");
    }

    /**
     * Returns the appropriate DDL to create an index.
     * Overrides the superclass variant since Postgresql doesn't support having index names specified in a particular schema (i.e "{schema}.{indexName}").
     * @param idx An object describing the index.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getCreateIndexStatement(Index idx, IdentifierFactory factory)
    {
        /**
        CREATE [UNIQUE] [CLUSTERED | NONCLUSTERED] INDEX index_name
            ON tableName (column [ASC|DESC] [ ,...n ])
            [ INCLUDE (column [ ,...n ] )]
            [ WHERE <filter_predicate> ]
            [ WITH ( <relational_index_option> [ ,...n ] ) ]
            [ ON { partition_scheme_name (column_name) | filegroup_name | default }]  
        */

        // Add support for column ordering, and different index name
        String extendedSetting = idx.getValueForExtension(Index.EXTENSION_INDEX_EXTENDED_SETTING);
        String indexType = idx.getValueForExtension(Index.EXTENSION_INDEX_TYPE);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE").append((idx.getUnique() ? " UNIQUE" : ""));
        if (indexType != null)
        {
            stringBuilder.append(indexType.equalsIgnoreCase("CLUSTERED") ? " CLUSTERED" : indexType.equalsIgnoreCase("NONCLUSTERED") ? " NONCLUSTERED" : "");
        }
        stringBuilder.append(" INDEX ");
        stringBuilder.append(factory.getIdentifierInAdapterCase(idx.getName()));
        stringBuilder.append(" ON ").append(idx.getTable().toString());
        stringBuilder.append(" ").append(idx.getColumnList(true));
        if (extendedSetting != null)
        {
            stringBuilder.append(" ").append(extendedSetting);
        }
        return stringBuilder.toString();
    }

    /**
     * The function to creates a unique value of type uniqueidentifier.
     * @return The function. e.g. "SELECT NEWID()"
     **/
    public String getSelectNewUUIDStmt()
    {
        return "SELECT NEWID()";
    }

    /**
     * The function to creates a unique value of type uniqueidentifier.
     * @return The function. e.g. "NEWID()"
     **/
    public String getNewUUIDFunction()
    {
        return "NEWID()";
    }

    /**
     * Whether the datastore will support setting the query fetch size to the supplied value.
     * @param size The value to set to
     * @return Whether it is supported.
     */
    public boolean supportsQueryFetchSize(int size)
    {
        if (size < 1)
        {
            // MSSQL doesnt support setting to 0
            return false;
        }
        return true;
    }

    /**
     * Method to create a column info for the current row.
     * Overrides the dataType/columnSize/decimalDigits to cater for SQLServer particularities.
     * @param rs ResultSet from DatabaseMetaData.getColumns()
     * @return column info
     */
    public RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs)
    {
        RDBMSColumnInfo info = new RDBMSColumnInfo(rs);

        short dataType = info.getDataType();
        switch (dataType)
        {
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                // Values > 0 inexplicably get returned here.
                info.setDecimalDigits(0);
                break;
            default:
                break;
        }

        return info;
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        SQLTypeInfo ti = new org.datanucleus.store.rdbms.adapter.SQLServerTypeInfo(rs);

        // Discard TINYINT type because it doesn't support negative values.
        String typeName = ti.getTypeName();
        if (typeName.toLowerCase().startsWith("tinyint"))
        {
            return null;
        }

        // Discard VARBINARY type generated from driver info,
        // because it uses a bad String in "createParams" ('max length' instead of '(max)')
        // in combination with "datanucleus.rdbms.useDefaultSqlType" false, this leads to
        // wrong data length in the DB (varbinary(1) for java.io.Serializable - e.g. ByteArrays)
        if (typeName.toLowerCase().startsWith("varbinary"))
        {
            return null;
        }

        return ti;
    }

    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    /**
     * Method to return the basic SQL for a DELETE TABLE statement.
     * Returns the String as <code>DELETE MYTABLE FROM MYTABLE t1</code>.
     * @param tbl The SQLTable to delete
     * @return The delete table string
     */
    public String getDeleteTableStatement(SQLTable tbl)
    {
        return "DELETE " + tbl.getAlias() + " FROM " + tbl.toString();
    }

    /**
     * Method to return the SQLText for an UPDATE TABLE statement.
     * Returns the SQLText for <code>UPDATE T1 SET x1 = val1, x2 = val2 FROM MYTBL T1</code>.
     * Override if the datastore doesn't support that standard syntax.
     * @param tbl The primary table
     * @param setSQL The SQLText for the SET component
     * @return SQLText for the update statement
     */
    public SQLText getUpdateTableStatement(SQLTable tbl, SQLText setSQL)
    {
        SQLText sql = new SQLText("UPDATE ").append(tbl.getAlias().toString()); // "UPDATE T1"
        sql.append(" ").append(setSQL); // " SET x1 = val1, x2 = val2"
        sql.append(" FROM ").append(tbl.toString()); // " FROM MYTBL T1"
        return sql;
    }

	/**
	 * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
	 * @return The statement for getting the latest auto-increment key
	 **/
	public String getIdentityLastValueStmt(Table table, String columnName)
	{
		return "SELECT @@IDENTITY";
	}

	/**
	 * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @param storeMgr The Store Manager
	 * @return The keyword for a column using auto-increment
	 **/
	public String getIdentityKeyword(StoreManager storeMgr)
	{
		return "IDENTITY";
	}

    /**
     * Verifies if the given <code>columnDef</code> is auto incremented by the datastore.
     * @param columnDef the datastore type name
     * @return true when the <code>columnDef</code> has values auto incremented by the datastore
     **/
    public boolean isIdentityFieldDataType(String columnDef)
    {
        if (columnDef == null)
        {
            return false;
        }
        else if (columnDef.equalsIgnoreCase("uniqueidentifier"))
        {
            return true;
        }
        return false;
    }

    /**
     * Method to return the INSERT statement to use when inserting into a table that has no
     * columns specified. This is the case when we have a single column in the table and that column
     * is autoincrement/identity (and so is assigned automatically in the datastore).
     * @param table The table
     * @return The statement for the INSERT
     */
    public String getInsertStatementForNoColumns(Table table)
    {
        return "INSERT INTO " + table.toString() + " DEFAULT VALUES";
    }

    public String getSelectWithLockOption()
    {
        return "(UPDLOCK, ROWLOCK)";
    }

    /**
     * Method to return if it is valid to select the specified mapping for the specified statement
     * for this datastore adapter. Sometimes, dependent on the type of the column(s), and what other
     * components are present in the statement, it may be invalid to select the mapping.
     * This implementation returns true, so override in database-specific subclass as required.
     * @param stmt The statement
     * @param m The mapping that we want to select
     * @return Whether it is valid
     */
    public boolean validToSelectMappingInStatement(SelectStatement stmt, JavaTypeMapping m)
    {
        if (m.getNumberOfColumnMappings() <= 0)
        {
            return true;
        }

        for (int i=0;i<m.getNumberOfColumnMappings();i++)
        {
            Column col = m.getColumnMapping(i).getColumn();
            if (col.getJdbcType() == JdbcType.CLOB || col.getJdbcType() == JdbcType.BLOB)
            {
                // "The ... data type cannot be selected as DISTINCT because it is not comparable."
                if (stmt.isDistinct())
                {
                    NucleusLogger.QUERY.debug("Not selecting " + m + " since is for BLOB/CLOB and using DISTINCT");
                    return false;
                }
                else if (stmt.getNumberOfUnions() > 0)
                {
                    NucleusLogger.QUERY.debug("Not selecting " + m + " since is for BLOB/CLOB and using UNION");
                    return false;
                }
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#isStatementTimeout(java.sql.SQLException)
     */
    @Override
    public boolean isStatementTimeout(SQLException sqle)
    {
        if (sqle.getSQLState() != null && sqle.getSQLState().equalsIgnoreCase("HY008"))
        {
            return true;
        }

        return super.isStatementTimeout(sqle);
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
        if (datastoreMajorVersion < 11) // Prior to SQLServer 2012
        {
            // Not supported
            return "";
        }
        else if (offset <= 0 && count <= 0)
        {
            return "";
        }
        else if (!hasOrdering)
        {
            // SQLServer requires ORDER BY to be able to use OFFSET https://technet.microsoft.com/en-us/library/gg699618%28v=sql.110%29.aspx
            return "";
        }

        StringBuilder str = new StringBuilder();
        str.append("OFFSET " + offset + (offset == 1 ? " ROW " : " ROWS "));
        if (count > 0)
        {
            str.append("FETCH NEXT " + count + (count == 1 ? " ROW " : " ROWS ") + "ONLY ");
        }
        return str.toString();
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
        if (datastoreMajorVersion < 11)
        {
            // Not supported prior to SQLServer 2012
            return super.getSequenceCreateStmt(sequenceName, min, max, start, increment, cacheSize);
        }

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
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (min != null)
        {
            stmt.append(" MINVALUE " + min);
        }
        if (max != null)
        {
            stmt.append(" MAXVALUE " + max);
        }
        if (cacheSize != null)
        {
            stmt.append(" CACHE " + cacheSize);
        }
        else
        {
            stmt.append(" CACHE 1");
        }

        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence for this datastore.
     * @param sequenceName Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequenceName)
    {
        if (datastoreMajorVersion < 11)
        {
            // Not supported prior to SQLServer 2012
            return super.getSequenceNextStmt(sequenceName);
        }

        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("SELECT NEXT VALUE FOR ");
        stmt.append(sequenceName);

        return stmt.toString();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLOperationClass(java.lang.String)
     */
    @Override
    public Class getSQLOperationClass(String operationName)
    {
        if ("concat".equals(operationName)) return org.datanucleus.store.rdbms.sql.operation.Concat2Operation.class;

        return super.getSQLOperationClass(operationName);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLMethodClass(java.lang.String, java.lang.String)
     */
    @Override
    public Class getSQLMethodClass(String className, String methodName, ClassLoaderResolver clr)
    {
        if (className == null)
        {
            if ("SQL_cube".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.SQLCubeFunction.class;
            else if ("SQL_rollup".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.SQLRollupFunction.class;
            else if ("DAY_OF_WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod4.class;
            else if ("HOUR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod4.class;
            else if ("MINUTE".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod4.class;
            else if ("SECOND".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod4.class;
            else if ("WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalWeekMethod4.class;
            else if ("QUARTER".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalQuarterMethod4.class;
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
                if ("concat".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringConcat2Method.class;
                else if ("indexOf".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringIndexOf4Method.class;
                else if ("length".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringLength4Method.class;
                else if ("startsWith".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringStartsWith2Method.class;
                else if ("substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring4Method.class;
                else if ("trim".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrim2Method.class;
            }
            else if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
            {
                if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod4.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod4.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod4.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod4.class;
            }
            else if ("java.time.LocalTime".equals(className))
            {
                if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod4.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod4.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod4.class;
            }
            else if ("java.time.LocalDate".equals(className) && "getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod4.class;
            else if ("java.time.LocalDateTime".equals(className))
            {
                if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod4.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod4.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod4.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod4.class;
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

        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);

        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "FLOAT", false);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", true);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);

        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", true);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.ClobColumnMapping.class, JDBCType.CLOB, "CLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "UNIQUEIDENTIFIER", false);
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

        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", true);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", false);

        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", true);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", false);

        registerColumnMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryStreamColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", true);

        super.loadColumnMappings(mgr, clr);
    }
}
