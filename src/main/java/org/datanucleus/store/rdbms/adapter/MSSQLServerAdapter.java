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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.MSSQLTypeInfo;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the Microsoft SQL Server database.
 * Note that majorVersion from JDBC doesn't align to version number of SQLServer.
 * 1995 = "6", 1998 = "7", 2000/2003 = "8", 2005 = "9", 2008 = "10", 2012 = "11"
 *
 * @see BaseDatastoreAdapter
 */
public class MSSQLServerAdapter extends BaseDatastoreAdapter
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
     * Constructs a Microsoft SQL Server adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public MSSQLServerAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(MSSQL_RESERVED_WORDS));

        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(LOCK_OPTION_PLACED_AFTER_FROM);
        supportedOptions.add(LOCK_OPTION_PLACED_WITHIN_JOIN);
        supportedOptions.add(ANALYSIS_METHODS);
        supportedOptions.add(STORED_PROCEDURES);

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
            supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
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
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.schema.MSSQLTypeInfo(
            "UNIQUEIDENTIFIER", (short)Types.CHAR, 36, "'", "'", "", 1, false, (short)2,
            false, false, false, "UNIQUEIDENTIFIER", (short)0, (short)0, 10);
        sqlType.setAllowsPrecisionSpec(false);
        addSQLTypeForJDBCType(handler, mconn, (short)MSSQLTypeInfo.UNIQUEIDENTIFIER, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.schema.MSSQLTypeInfo(
            "IMAGE", (short)Types.BLOB, 2147483647, null, null, null, 1, false, (short)1,
            false, false, false, "BLOB", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BLOB, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.schema.MSSQLTypeInfo(
            "TEXT", (short)Types.CLOB, 2147483647, null, null, null, 1, true, (short)1,
            false, false, false, "TEXT", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.schema.MSSQLTypeInfo(
            "float", (short)Types.DOUBLE, 53, null, null, null, 1, false, (short)2,
            false, false, false, null, (short)0, (short)0, 2);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DOUBLE, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.schema.MSSQLTypeInfo(
            "IMAGE", (short)Types.LONGVARBINARY, 2147483647, null, null, null, 1, false, (short)1,
            false, false, false, "LONGVARBINARY", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.LONGVARBINARY, sqlType, true);
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
        if (word != null && word.indexOf(' ') >= 0)
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
     * Overrides the superclass variant since Postgresql doesn't support having index names specified in
     * a particular schema (i.e "{schema}.{indexName}").
     * @param idx An object describing the index.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getCreateIndexStatement(Index idx, IdentifierFactory factory)
    {
        String idxIdentifier = factory.getIdentifierInAdapterCase(idx.getName());
        return "CREATE " + (idx.getUnique() ? "UNIQUE " : "") + "INDEX " + idxIdentifier + " ON " + idx.getTable().toString() + ' ' +
           idx + (idx.getExtendedIndexSettings() == null ? "" : " " + idx.getExtendedIndexSettings());
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
     * Overrides the dataType/columnSize/decimalDigits to cater for MSSQL particularities.
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
        SQLTypeInfo ti = new org.datanucleus.store.rdbms.schema.MSSQLTypeInfo(rs);

        // Discard TINYINT type because it doesn't support negative values.
        String typeName = ti.getTypeName();
        if (typeName.toLowerCase().startsWith("tinyint"))
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
	public String getAutoIncrementStmt(Table table, String columnName)
	{
		return "SELECT @@IDENTITY";
	}

	/**
	 * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
	 * @return The keyword for a column using auto-increment
	 **/
	public String getAutoIncrementKeyword()
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

    /**
     * An operator in a string expression that concatenates two or more
     * character or binary strings, columns, or a combination of strings and
     * column names into one expression (a string operator).
     * 
     * @return the operator SQL String
     */
    public String getOperatorConcat()
    {
        return "+";
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
    public boolean validToSelectMappingInStatement(SQLStatement stmt, JavaTypeMapping m)
    {
        if (m.getNumberOfDatastoreMappings() <= 0)
        {
            return true;
        }

        for (int i=0;i<m.getNumberOfDatastoreMappings();i++)
        {
            Column col = m.getDatastoreMapping(i).getColumn();
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
     * @return The SQL to append to allow for ranges using OFFSET/FETCH.
     */
    public String getRangeByLimitEndOfStatementClause(long offset, long count)
    {
        if (datastoreMajorVersion < 11) // Prior to SQLServer 2012
        {
            return super.getRangeByLimitEndOfStatementClause(offset, count);
        }
        else if (offset <= 0 && count <= 0)
        {
            return "";
        }

        StringBuilder str = new StringBuilder();
        str.append("OFFSET " + offset + (offset == 1 ? " ROW " : " ROWS "));
        if (count > 0)
        {
            str.append("FETCH NEXT " + (count > 1 ? (count + " ROWS ONLY ") : "ROW ONLY "));
        }
        return str.toString();
    }

    /**
     * Accessor for the sequence statement to create the sequence.
     * @param sequence_name Name of the sequence 
     * @param min Minimum value for the sequence
     * @param max Maximum value for the sequence
     * @param start Start value for the sequence
     * @param increment Increment value for the sequence
     * @param cache_size Cache size for the sequence
     * @return The statement for getting the next id from the sequence
     */
    public String getSequenceCreateStmt(String sequence_name,
            Integer min, Integer max, Integer start, Integer increment, Integer cache_size)
    {
        if (datastoreMajorVersion < 11)
        {
            // Not supported prior to SQLServer 2012
            return super.getSequenceCreateStmt(sequence_name, min, max, start, increment, cache_size);
        }

        if (sequence_name == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequence_name);
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
        if (cache_size != null)
        {
            stmt.append(" CACHE " + cache_size);
        }
        else
        {
            stmt.append(" CACHE 1");
        }

        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence for this datastore.
     * @param sequence_name Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequence_name)
    {
        if (datastoreMajorVersion < 11)
        {
            // Not supported prior to SQLServer 2012
            return super.getSequenceNextStmt(sequence_name);
        }

        if (sequence_name == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        // Do we need to quote the sequence name here?
        StringBuilder stmt = new StringBuilder("SELECT NEXT VALUE FOR '");
        stmt.append(sequence_name);
        stmt.append("'");

        return stmt.toString();
    }
}