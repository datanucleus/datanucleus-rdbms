/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved.
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
2002 Mike Martin (TJDO)
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - added sequence methods
2004 Andy Jefferson - updated Oracle version checks to use majorVersion
2005 Andrew Hoffman - getColumnInfo method to cut down on Oracle startup
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.OracleMappingManagerImpl;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the Oracle database.
 * 
 * @see BaseDatastoreAdapter
 */
public class OracleAdapter extends BaseDatastoreAdapter
{
    /** What the official Oracle JDBC driver uses to identify itself. */
    public static final String OJDBC_DRIVER_NAME = "Oracle JDBC driver";

    /**
     * A string containing the list of Oracle keywords 
     * This list is normally obtained dynamically from the driver using
     * DatabaseMetaData.getSQLKeywords()
     * 
     * Based on database Oracle8
     */
    public static final String ORACLE_8_RESERVED_WORDS =
        "ACCESS,AUDIT,CLUSTER,COMMENT,COMPRESS,EXCLUSIVE,FILE,IDENTIFIED," +
        "INCREMENT,INDEX,INITIAL,LOCK,LONG,MAXEXTENTS,MINUS,MLSLABEL,MODE," +
        "MODIFY,NOAUDIT,NOCOMPRESS,NOWAIT,NUMBER,OFFLINE,ONLINE,PCTFREE,RAW," +
        "RENAME,RESOURCE,ROWID,ROWNUM,SHARE,SUCCESSFUL,SYNONYM,SYSDATE,UID," +
        "VALIDATE,VARCHAR2,VALIDATE,VARCHAR2";

    /**
     * A string containing the list of Oracle keywords 
     * This list is normally obtained dynamically from the driver using
     * DatabaseMetaData.getSQLKeywords()
     * 
     * Based on database Oracle9i
     */
    public static final String ORACLE_9_RESERVED_WORDS =
        "ACCESS,CHAR,DEFAULT,ADD,CHECK,DELETE,ALL,CLUSTER,DESC,ALTER,COLUMN," +
        "DISTINCT,AND,COMMENT,DROP,ANY,COMPRESS,ELSE,AS,CONNECT,EXCLUSIVE,ASC," +
        "CREATE,EXISTS,AUDIT,CURRENT,FILE,BETWEEN,DATE,FLOAT,BY,DECIMAL,FOR,FROM," +
        "NOT,SHARE,GRANT,NOWAIT,SIZE,GROUP,NULL,SMALLINT,HAVING,NUMBER,START," +
        "IDENTIFIED,OF,SUCCESSFUL,IMMEDIATE,OFFLINE,SYNONYM,IN,ON,SYSDATE," +
        "INCREMENT,ONLINE,TABLE,INDEX,OPTION,THEN,INITIAL,OR,TO,INSERT,ORDER," +
        "TRIGGER,INTEGER,PCTFREE,UID,INTERSECT,PRIOR,UNION,INTO,PRIVILEGES,UNIQUE," +
        "IS,PUBLIC,UPDATE,LEVEL,RAW,USER,LIKE,RENAME,VALIDATE,LOCK,RESOURCE,VALUES," +
        "LONG,REVOKE,VARCHAR,MAXEXTENTS,ROW,VARCHAR2,MINUS,ROWID,VIEW,MLSLABEL," +
        "ROWNUM,WHENEVER,MODE,ROWS,WHERE,MODIFY,SELECT,WITH,NOAUDIT,SESSION," +
        "NOCOMPRESS,SET";
    
    /**
     * A string containing the list of Oracle keywords 
     * This list is normally obtained dynamically from the driver using
     * DatabaseMetaData.getSQLKeywords()
     * 
     * Based on database Oracle10g
     */
    public static final String ORACLE_10_RESERVED_WORDS =
        "ACCESS,ADD,ALL,ALTER,AND,ANY,AS,ASC,AUDIT,BETWEEN,BY,CHAR,CHECK,CLUSTER," +
        "COLUMN,COMMENT,COMPRESS,CONNECT,CREATE,CURRENT,DATE,DECIMAL,DEFAULT,DELETE," +
        "DESC,DISTINCT,DROP,ELSE,EXCLUSIVE,EXISTS,FILE,FLOAT,FOR,FROM,GRANT,GROUP," +
        "HAVING,IDENTIFIED,IMMEDIATE,IN,INCREMENT,INDEX,INITIAL,INSERT,INTEGER," +
        "INTERSECT,INTO,IS,LEVEL,LIKE,LOCK,LONG,MAXEXTENTS,MINUS,MLSLABEL,MODE," +
        "MODIFY,NOAUDIT,NOCOMPRESS,NOT,NOWAIT,NULL,NUMBER,OF,OFFLINE,ON,ONLINE," +
        "OPTION,OR,ORDER,PCTFREE,PRIOR,PRIVILEGES,PUBLIC,RAW,RENAME,RESOURCE," +
        "REVOKE,ROW,ROWID,ROWNUM,ROWS,SELECT,SESSION,SET,SHARE,SIZE,SMALLINT," +
        "START,SUCCESSFUL,SYNONYM,SYSDATE,TABLE,THEN,TO,TRIGGER,UID,UNION," +
        "UNIQUE,UPDATE,USER,VALIDATE,VALUES,VARCHAR,VARCHAR2,VIEW,WHENEVER," +
        "WHERE,WITH";

    
    /**
     * Constructs an Oracle adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public OracleAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        if (datastoreMajorVersion <= 8)
        {
            reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(ORACLE_8_RESERVED_WORDS));
        }
        else if (datastoreMajorVersion == 9)
        {
            reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(ORACLE_9_RESERVED_WORDS));
        }
        else
        {
            reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(ORACLE_10_RESERVED_WORDS));
        }

        supportedOptions.add(LOCK_ROW_USING_SELECT_FOR_UPDATE);
        supportedOptions.add(LOCK_ROW_USING_SELECT_FOR_UPDATE_NOWAIT);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(NULL_EQUALS_EMPTY_STRING);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
        supportedOptions.add(GROUP_BY_REQUIRES_ALL_SELECT_PRIMARIES);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);

        supportedOptions.remove(BOOLEAN_COMPARISON);
        if (datastoreMajorVersion < 9)
        {
            // Oracle 8 doesnt allow "INNER JOIN"
            supportedOptions.remove(ANSI_JOIN_SYNTAX);
        }
        else
        {
            supportedOptions.add(ANSI_JOIN_SYNTAX);
        }

        supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
        supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
        supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
        supportedOptions.remove(FK_UPDATE_ACTION_NULL);
        supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);

        if (datastoreMajorVersion >= 12) 
        {
            supportedOptions.add(IDENTITY_COLUMNS);
        }
        if ((datastoreMajorVersion == 11 && datastoreMinorVersion >= 2) || datastoreMajorVersion >= 12)
        {
            // In 11.2+ Oracle supports things like "VARCHAR(50 BYTE)"
            supportedOptions.add(COLUMN_LENGTH_SEMANTICS);
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
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.adapter.OracleTypeInfo(
            "CLOB", (short)Types.CLOB, 1073741823, "'", "'", null, 1, true, (short)0, false, false, false, "CLOB", (short)0, (short)0, 10);
        sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a CLOB
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.OracleTypeInfo(
            "DATE", (short)Types.DATE, 7, null, null, null, 1, false, (short)3, false, false, false, "DATE", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DATE, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.OracleTypeInfo(
            "DECIMAL", (short)Types.DECIMAL, 38, null, null, null, 1, false, (short)3, false, true, false, "NUMBER", (short)-84, (short)127, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DECIMAL, sqlType, true);

        // Oracle has a synonym "DOUBLE PRECISION" (can't specify precision/scale) mapping to sql type of "FLOAT"
        sqlType = new org.datanucleus.store.rdbms.adapter.OracleTypeInfo(
            "DOUBLE PRECISION", (short)Types.DOUBLE, 38, null, null, null, 1, false, (short)3, false, true, false, "NUMBER", (short)-84, (short)127, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.DOUBLE, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.OracleTypeInfo(
            OracleTypeInfo.TYPES_NAME_SYS_XMLTYPE, (short)OracleTypeInfo.TYPES_SYS_XMLTYPE, 1073741823, "'", "'", null, 1, true, (short)0, 
            false, false, false, OracleTypeInfo.TYPES_NAME_SYS_XMLTYPE, (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)OracleTypeInfo.TYPES_SYS_XMLTYPE, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.OracleTypeInfo(
            "NVARCHAR2", (short)Types.NVARCHAR, 4000, "'", "'", null, 1, true, (short)3, false, false, false, "NVARCHAR2", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.NVARCHAR, sqlType, true);

        // Update any types that need extra info relative to the JDBC info
        Collection<SQLTypeInfo> sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.BLOB);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a BLOB
            }
        }

        sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.CLOB);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a CLOB
            }
        }

        supportedJdbcTypesById.put(OracleTypeInfo.TYPES_SYS_XMLTYPE, "XMLTYPE");
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.adapter.OracleTypeInfo(rs);
    }

    /**
     * Accessor for a MappingManager suitable for use with this datastore adapter.
     * @param storeMgr The StoreManager
     * @return the MappingManager
     */
    public MappingManager getMappingManager(RDBMSStoreManager storeMgr)
    {
        return new OracleMappingManagerImpl(storeMgr);
    }

    /**
     * Accessor for the vendor id
     * @return The Oracle vendor id
     */
    public String getVendorID()
    {
        return "oracle";
    }

    /**
     * Some databases, Oracle, treats an empty string (0 length) equals null
     * @return returns a surrogate to replace the empty string in the database
     * otherwise it would be treated as null
     */
    public String getSurrogateForEmptyStrings()
    {
        return "\u0001";
    }
    
    /**
     * @return null, because oracle does not have catalogs
     */
    public String getCatalogName(Connection conn) throws SQLException
    {
        return null;
    }
    
    public String getSchemaName(Connection conn) throws SQLException
    {
        Statement stmt = conn.createStatement();
        
        try
        {
            String stmtText = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL";
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

    public String getCreateDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("Oracle does not support CREATE SCHEMA; you need to create a USER (manually)");
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("Oracle does not support DROP SCHEMA; you need to delete a USER (manually)");
    }

    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        // We are using PRIMARYKEY_IN_CREATE_STATEMENTS so return null
        return null;
    }

    /**
     * Provide the existing indexes in the database for the table.
     * This is implemented if and only if the datastore has its own way of getting indexes.
     * In this implementation we provide an alternate method for Oracle JDBC driver &lt; 10.2.0.1.0 only.
     * All other versions of Oracle will use the default. The schemaName MUST BE PROVIDED.
     * @param conn the JDBC connection
     * @param catalog the catalog name
     * @param schema the schema name.
     * @param table the table name
     * @return a ResultSet with the format @see DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     * @throws SQLException if an error occurs
     */
    public ResultSet getExistingIndexes(Connection conn, String catalog, String schema, String table) 
    throws SQLException
    {
        if (isReservedKeyword(table) || !table.matches("[a-zA-Z]{1}\\w*(\\$|\\#)*\\w*"))
        {
            // THIS IS A FIX for the JDBC driver. Already noticed in previous versions, but this fix was 
            // only tested with Oracle 10g, and all drivers versions. Occurs when the table identifier is
            // a reserved word or doesn't match the regexp defined above
            String GET_INDEXES_STMT =
                "SELECT null as table_cat, "+
                "owner as table_schem, "+
                "table_name, "+
                "0 as NON_UNIQUE, "+
                "null as index_qualifier, "+
                "null as index_name, 0 as type, "+
                "0 as ordinal_position, null as column_name, "+
                "null as asc_or_desc, "+
                "num_rows as cardinality, "+
                "blocks as pages, "+
                "null as filter_condition "+
                "FROM all_tables "+
                "WHERE table_name = ? "+
                "AND owner = ? "+

                "UNION "+

                "SELECT null as table_cat, "+
                "i.owner as table_schem, "+
                "i.table_name, "+
                "decode (i.uniqueness, 'UNIQUE', 0, 1), "+
                "null as index_qualifier, "+
                "i.index_name, "+
                "1 as type, "+
                "c.column_position as ordinal_position, "+
                "c.column_name, "+
                "null as asc_or_desc, "+
                "i.distinct_keys as cardinality, "+
                "i.leaf_blocks as pages, "+
                "null as filter_condition "+
                "FROM all_indexes i, all_ind_columns c "+
                "WHERE i.table_name = ? "+
                "AND i.owner = ? "+
                "AND i.index_name = c.index_name "+
                "AND i.table_owner = c.table_owner "+
                "AND i.table_name = c.table_name "+
                "AND i.owner = c.index_owner "+
                "ORDER BY non_unique, type, index_name, ordinal_position";

            NucleusLogger.DATASTORE_SCHEMA.debug("Retrieving Oracle index info using the following SQL : " + GET_INDEXES_STMT);
            PreparedStatement stmt = conn.prepareStatement(GET_INDEXES_STMT);
	        stmt.setString(1,table);
	        stmt.setString(2,schema);
	        stmt.setString(3,table);
	        stmt.setString(4,schema);
	        return stmt.executeQuery();
        }

        return super.getExistingIndexes(conn,catalog,schema,table);
    }

    /**
     * Method to return the drop table statement for Oracle.
     * @param table The table
     * @return The statement text
     */
    public String getDropTableStatement(Table table)
    {
        if (datastoreMajorVersion >= 10)
        {
            // Add "PURGE" to avoid putting the table into the Oracle "recycle bin"
            return "DROP TABLE " + table.toString() + " CASCADE CONSTRAINTS PURGE";
        }
        return "DROP TABLE " + table.toString() + " CASCADE CONSTRAINTS";
    }

    /**
     * Accessor for the auto-increment sql statement for getting the last value for this table.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     */
    /*public String getIdentityLastValueStmt(Table table, String columnName)
    {
    // Oracle 12 doesnt seem to provide a mechanism for getting the identity value, so assume JDBC getGeneratedKeys works
        return "VALUES IDENTITY_VAL_LOCAL()";
    }*/

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @param storeMgr The Store Manager
     * @return The keyword for a column using auto-increment
     */
    public String getIdentityKeyword(StoreManager storeMgr)
    {
        // Oracle 12+
        return "generated always as identity";
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
        if (min != null)
        {
            stmt.append(" MINVALUE " + min);
        }
        if (max != null)
        {
            stmt.append(" MAXVALUE " + max);
        }
        if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (cacheSize != null)
        {
            stmt.append(" CACHE " + cacheSize);
        }
        else
        {
            stmt.append(" NOCACHE");
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
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }
        StringBuilder stmt=new StringBuilder("SELECT ");
        stmt.append(sequenceName);
        stmt.append(".NEXTVAL from dual ");

        return stmt.toString();
    }

    /**
     * Method to create a column info for the current row.
     * Overrides the dataType to cater for Oracle particularities.
     * @param rs ResultSet from DatabaseMetaData.getColumns()
     * @return column info
     */
    public RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs)
    {
        RDBMSColumnInfo info = new RDBMSColumnInfo(rs);
        String typeName = info.getTypeName();
        int dataType = -1;
        if (typeName == null)
        {
            dataType = Types.NULL;
        }
        else if (typeName.equals("ROWID"))
        {
            dataType = Types.INTEGER;
        }
        else if (typeName.equals("NUMBER") || typeName.equals("VARNUM"))
        {
            dataType = Types.NUMERIC;
        }
        else if (typeName.equals("VARCHAR2"))
        {
            dataType = Types.VARCHAR;
        }
        else if (typeName.equals("CHAR"))
        {
            dataType = Types.CHAR;
        }
        else if (typeName.equals("DATE"))
        {
            // should this be TIMESTAMP instead of DATE ??
            dataType = Types.DATE;
        }
        else if (typeName.equals("CLOB") || typeName.equals("NCLOB"))
        {
            dataType = Types.CLOB;
        }
        else if (typeName.equals("BLOB"))
        {
            dataType = Types.BLOB; 
        }
        else if (typeName.equals("LONG"))
        {
            dataType = Types.LONGVARCHAR;
        }
        else if (typeName.equals("LONG RAW"))
        {
            dataType = Types.LONGVARBINARY;
        }
        else if (typeName.equals("RAW"))
        {
            dataType = Types.VARBINARY;
        }
        else if (typeName.startsWith("TIMESTAMP"))
        {
            dataType = Types.TIMESTAMP;
        }
        else if (typeName.equals("FLOAT"))
        {
            dataType = Types.FLOAT;
        }
        else
        {
            NucleusLogger.DATASTORE.warn(Localiser.msg("020191", typeName));
            dataType = Types.OTHER;
        }

        info.setDataType((short)dataType);

        return info;
    }

    /**
     * Accessor for the transaction isolation level to use during schema creation.
     * @return The transaction isolation level for schema generation process
     */
    public int getTransactionIsolationForSchemaCreation()
    {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Accessor for table and column information for a catalog/schema in this datastore.
     * An override for the DatabaseMetaData.getColumns() method call as referenced in superclass. 
     * The default Oracle-provided getColumns() method is VERY slow for large schemas, 
     * particularly due to REMARKS and COLUMN_DEF (column defaults) columns 
     * (outer-joins in data dictionary views)
     * <b>Note:</b> This method DOES NOT return default column value (meta-data ResultSet column COLUMN_DEF)
     * as this column causes MAJOR slowdown in meta-data retrieval performance. 
     * @param conn Connection to use
     * @param catalog The catalog (null if none)
     * @param schema The schema (null if none)
     * @param table The table (null if all)
     * @param columnNamePattern Col name(s) (null if all) NOT USED
     * @return ResultSet containing the table/column information
     * @throws SQLException Thrown if an error occurs
     */
    public ResultSet getColumns(Connection conn, String catalog, String schema, String table, String columnNamePattern)
    throws SQLException
    {
        // setup SQL for query from Oracle data dictionary view ALL_TAB_COLUMNS
        StringBuilder columnsQuery = new StringBuilder();
        columnsQuery.append("SELECT NULL TABLE_CAT, OWNER TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, NULL DATA_TYPE, ");
        columnsQuery.append("DATA_TYPE TYPE_NAME, DECODE(DATA_TYPE,'NUMBER',DATA_PRECISION,DATA_LENGTH) COLUMN_SIZE, ");
        columnsQuery.append("0 BUFFER_LENGTH, DATA_SCALE DECIMAL_DIGITS, 10 NUM_PREC_RADIX, ");
        columnsQuery.append("DECODE(NULLABLE,'Y',1,0) NULLABLE, NULL REMARKS, NULL COLUMN_DEF, 0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB, ");
        columnsQuery.append("DATA_LENGTH CHAR_OCTET_LENGTH, COLUMN_ID ORDINAL_POSITION, DECODE(NULLABLE,'Y','YES','NO') IS_NULLABLE ");
        columnsQuery.append("FROM ALL_TAB_COLUMNS ");

        boolean outputWhere = false;
        if (schema != null && schema.length() > 0)
        {
            // where clause - schemaString
            columnsQuery.append("WHERE OWNER LIKE '").append(schema).append("' ");
            outputWhere = true;
        }
        if (table != null)
        {
            // where clause - tableString
            if (!outputWhere)
            {
                columnsQuery.append("WHERE ");
                outputWhere = true;
            }
            else
            {
                columnsQuery.append("AND ");
            }

            if (table.length() > 0)
            {
                columnsQuery.append("TABLE_NAME LIKE '").append(table).append("' ");
            }
            else
            {
                columnsQuery.append("TABLE_NAME IS NULL ");
            }
        }

        // TODO Support columnNamePattern

        columnsQuery.append("ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION ");
        NucleusLogger.DATASTORE_SCHEMA.debug("Retrieving Oracle column info using the following SQL : " + columnsQuery);

        PreparedStatement columnsStmt = conn.prepareStatement(columnsQuery.toString());
        ResultSet columnsResult = columnsStmt.executeQuery();

        columnsQuery = null;
        return columnsResult;
    }

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "SELECT CURRENT_TIMESTAMP FROM DUAL";
    }

    /**
     * Convenience method to allow adaption of an ordering string before applying it.
     * This is useful where the datastore accepts some conversion adapter around the ordering column
     * for example.
     * @param storeMgr StoreManager
     * @param orderString The basic ordering string
     * @param sqlExpr The sql expression being represented here
     * @return The adapted ordering string
     */
    public String getOrderString(StoreManager storeMgr, String orderString, SQLExpression sqlExpr)
    {
        String nlsSortOrder = "LATIN";
        String sortOrder = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_ORACLE_NLS_SORT_ORDER);
        if (sortOrder != null)
        {
            nlsSortOrder = sortOrder.toUpperCase();
        }

        if (sqlExpr instanceof CharacterExpression && !nlsSortOrder.equals("BINARY"))
        {
            // Wrap with NLSSORT function
            return "NLSSORT(" + orderString + ", 'NLS_SORT = " + nlsSortOrder + "')";
        }
        else if (datastoreMajorVersion < 9 && sqlExpr instanceof BooleanExpression &&
            !sqlExpr.getJavaTypeMapping().getColumnMapping(0).isStringBased())
        {
            // Oracle 8 or earlier don't support ORDER BY on non-String based booleans ? (came from TJDO)
            throw new NucleusException(Localiser.msg("052505")).setFatal();
        }
        else
        {
            return orderString;
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#isStatementTimeout(java.sql.SQLException)
     */
    @Override
    public boolean isStatementTimeout(SQLException sqle)
    {
        if (sqle.getSQLState() != null && sqle.getSQLState().equalsIgnoreCase("72000") && sqle.getErrorCode() == 1013)
        {
            return true;
        }

        return super.isStatementTimeout(sqle);
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
                // "java.sql.SQLException: ORA-00932: inconsistent datatypes: expected - got CLOB"
                if (stmt.isDistinct())
                {
                    NucleusLogger.QUERY.debug("Not selecting " + m + " since is for BLOB/CLOB and using DISTINCT");
                    return false;
                }
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#getRangeByRowNumberColumn2()
     */
    public String getRangeByRowNumberColumn2()
    {
        return "ROWNUM";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLOperationClass(java.lang.String)
     */
    @Override
    public Class getSQLOperationClass(String operationName)
    {
        if ("mod".equals(operationName)) return org.datanucleus.store.rdbms.sql.operation.Mod2Operation.class;

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
            if ("YEAR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod2.class;
            else if ("MONTH".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod2.class;
            else if ("MONTH_JAVA".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthJavaMethod2.class;
            else if ("DAY".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod2.class;
            else if ("HOUR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod2.class;
            else if ("MINUTE".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod2.class;
            else if ("SECOND".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod2.class;
            else if ("WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalWeekMethod6.class;
            else if ("QUARTER".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalQuarterMethod2.class;

            else if ("SQL_cube".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.SQLCubeFunction.class;
            else if ("SQL_rollup".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.SQLRollupFunction.class;
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
                if ("concat".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringConcat1Method.class;
                else if ("indexOf".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringIndexOf2Method.class;
                else if ("length".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringLength3Method.class;
                else if ("substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring3Method.class;
                else if ("translate".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTranslateMethod.class;
            }
            else if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
            {
                if ("getDay".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod2.class;
                else if ("getDate".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod2.class;
                else if ("getMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthJavaMethod2.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod2.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod2.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod2.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod2.class;
            }
            else if ("java.time.LocalTime".equals(className))
            {
                if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod2.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod2.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod2.class;
            }
            else if ("java.time.LocalDate".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod2.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod2.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod2.class;
            }
            else if ("java.time.LocalDateTime".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod2.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod2.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod2.class;
                else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod2.class;
                else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod2.class;
                else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod2.class;
            }
            else if ("java.time.MonthDay".equals(className))
            {
                if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod2.class;
                else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod2.class;
            }
            else if ("java.time.Period".equals(className))
            {
                if ("getMonths".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod2.class;
                else if ("getDays".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod2.class;
                else if ("getYears".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod2.class;
            }
            else if ("java.time.YearMonth".equals(className))
            {
                if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod2.class;
                else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod2.class;
            }
        }

        return super.getSQLMethodClass(className, methodName, clr);
    }

    @Override
    public String getInsertStatementForNoColumns(Table table)
    {
        return "INSERT INTO " + table.toString() + " VALUES (DEFAULT)";
    }

    /**
     * Load all datastore mappings for this RDBMS database.
     * @param mgr the PluginManager
     * @param clr the ClassLoaderResolver
     */
    protected void loadColumnMappings(PluginManager mgr, ClassLoaderResolver clr)
    {
        // Load up built-in types for this datastore
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", true);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BitColumnMapping.class, JDBCType.BIT, "BIT", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.BOOLEAN, "BOOLEAN", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", true);
        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", false);

        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", false);

        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", false);

        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", true);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", true);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", false);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);

        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", true);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.OracleClobColumnMapping.class, JDBCType.CLOB, "CLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.OracleBlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.OracleXMLTypeColumnMapping.class, "XMLTYPE", "XMLTYPE", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NVarcharColumnMapping.class, JDBCType.NVARCHAR, "NVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NCharColumnMapping.class, JDBCType.NCHAR, "NCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NVarcharColumnMapping.class, JDBCType.NVARCHAR, "NVARCHAR2", false);

        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", true);
        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", true);
        registerColumnMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

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

        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.OracleBlobColumnMapping.class, JDBCType.BLOB, "BLOB", true);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONG RAW", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "RAW", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "BINARY", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "RAW", false);

        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.OracleBlobColumnMapping.class, JDBCType.BLOB, "BLOB", true);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONG RAW", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "RAW", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "BINARY", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "RAW", false);

        registerColumnMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryStreamColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMBER", true);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

        super.loadColumnMappings(mgr, clr);
    }
}