/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.ForeignKeyInfo;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.method.SQLMethod;
import org.datanucleus.store.rdbms.sql.operation.SQLOperation;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.store.rdbms.table.ViewImpl;
import org.datanucleus.store.schema.StoreSchemaHandler;

/**
 * Definition of a datastore adapter, providing all characteristics of the underlying datastore
 * such as whether it supports FKs, indexes, and what statement syntax to use.
 */
public interface DatastoreAdapter
{
    /**
     * Whether this datastore adapter supports identity (autoincrement) fields. In SQL this would be things like "AUTOINCREMENT", "IDENTITY", "SERIAL".
     */
    public static final String IDENTITY_COLUMNS = "IdentityColumns";

    /**
     * Whether we support identity (auto-increment) keys with nullability specification.
     */
    public static final String IDENTITY_KEYS_NULL_SPECIFICATION = "IdentityNullSpecification";

    /**
     * Whether we support identity (auto-increment) keys with column type specification.
     */
    public static final String IDENTITY_COLUMN_TYPE_SPECIFICATION = "IdentityColumnTypeSpecification";

    /**
     * Whether this adapter requires any specification of primary key in the column definition of CREATE TABLE.
     */
    public static final String IDENTITY_PK_IN_CREATE_TABLE_COLUMN_DEF = "IdentityPkInCreateTableColumnDef";

    /** Whether we support sequences. */
    public static final String SEQUENCES = "Sequences";

    /** Support for JDO compatible UUID-STRING value generation. */
    public static final String VALUE_GENERATION_UUID_STRING = "ValueGeneratorUUIDString";

    /** Whether "Types.BIT" is really mapped as BOOLEAN. */
    public static final String BIT_IS_REALLY_BOOLEAN = "BitIsReallyBoolean";

    /** Do we support right outer join? */
    public static final String RIGHT_OUTER_JOIN = "RightOuterJoin";

    /** Do we support SOME|ALL|ANY {subquery}. */
    public static final String SOME_ANY_ALL_SUBQUERY_EXPRESSIONS = "SomeAllAnySubqueries";

    /** Whether we support Boolean comparisons. */
    public static final String BOOLEAN_COMPARISON = "BooleanExpression";

    public static final String ESCAPE_EXPRESSION_IN_LIKE_PREDICATE = "EscapeExpressionInLikePredicate";

    /**
     * Whether this datastore supports "SELECT a.* FROM (SELECT * FROM TBL1 INNER JOIN TBL2 ON tbl1.x = tbl2.y ) a"
     * If the database does not support the SQL statement generated is like 
     * "SELECT a.* FROM (TBL1 INNER JOIN TBL2 ON tbl1.x = tbl2.y ) a"
     */
    public static final String PROJECTION_IN_TABLE_REFERENCE_JOINS = "ProjectionInTableReferenceJoins";

    /**
     * Whether this datastore supports the use of the catalog name in ORM table definitions (DDL).
     */
    public static final String CATALOGS_IN_TABLE_DEFINITIONS = "CatalogInTableDefinition";

    /**
     * Whether this datastore supports the use of the schema name in ORM table definitions (DDL).
     */
    public static final String SCHEMAS_IN_TABLE_DEFINITIONS = "SchemaInTableDefinition";

    public static final String IDENTIFIERS_LOWERCASE = "LowerCaseIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE = "MixedCaseIdentifiers";
    public static final String IDENTIFIERS_UPPERCASE = "UpperCaseIdentifiers";
    public static final String IDENTIFIERS_LOWERCASE_QUOTED = "LowerCaseQuotedIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE_QUOTED = "MixedCaseQuotedIdentifiers";
    public static final String IDENTIFIERS_UPPERCASE_QUOTED = "UpperCaseQuotedIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE_SENSITIVE = "MixedCaseSensitiveIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE_QUOTED_SENSITIVE = "MixedCaseQuotedSensitiveIdentifiers";

    /** Whether the RDBMS supports SQL VIEWs. */
    public static final String VIEWS = "Views";

    /** Whether the RDBMS supports UNION syntax. */
    public static final String UNION_SYNTAX = "Union_Syntax";

    /**
     * Union combines the results of two or more queries into a single result set.
     * Union include only distinct rows and Union all may include duplicates.
     * When using the UNION statement, keep in mind that, by default, it performs the equivalent of 
     * a SELECT DISTINCT on the final result set. In other words, UNION takes the results of two like
     * recordsets, combines them, and then performs a SELECT DISTINCT in order to eliminate any 
     * duplicate rows. This process occurs even if there are no duplicate records in the final recordset. 
     * If you know that there are duplicate records, and this presents a problem for your application, 
     * then by all means use the UNION statement to eliminate the duplicate rows. On the other hand, 
     * if you know that there will never be any duplicate rows, or if there are, and this presents no 
     * problem to your application, then you should use the UNION ALL statement instead of the UNION 
     * statement. The advantage of the UNION ALL is that is does not perform the SELECT DISTINCT 
     * function, which saves a lot of unnecessary SQL Server resources from being using.
     */
    public static final String USE_UNION_ALL = "UseUnionAll";

    /**
     * Whether the RDBMS supports use of EXISTS syntax.
     */
    public static final String EXISTS_SYNTAX = "Exists_Syntax";

    /**
     * Whether this datastore supports ALTER TABLE DROP constraints
     */
    public static final String ALTER_TABLE_DROP_CONSTRAINT_SYNTAX = "AlterTableDropConstraint_Syntax";

    /**
     * Whether this datastore supports ALTER TABLE DROP FOREIGN KEY constraints
     */
    public static final String ALTER_TABLE_DROP_FOREIGN_KEY_CONSTRAINT = "AlterTableDropForeignKey_Syntax";

    /** Whether this datastore supports deferred constraints. */
    public static final String DEFERRED_CONSTRAINTS = "DeferredConstraints";

    public static final String DISTINCT_WITH_SELECT_FOR_UPDATE = "DistinctWithSelectForUpdate";
    public static final String GROUPING_WITH_SELECT_FOR_UPDATE = "GroupingWithSelectForUpdate";
    public static final String HAVING_WITH_SELECT_FOR_UPDATE = "HavingWithSelectForUpdate";
    public static final String ORDERING_WITH_SELECT_FOR_UPDATE = "OrderingWithSelectForUpdate";
    public static final String MULTITABLES_WITH_SELECT_FOR_UPDATE = "MultipleTablesWithSelectForUpdate";

    public static final String UPDATE_STATEMENT_ALLOW_TABLE_ALIAS_IN_SET_CLAUSE = "UpdateStmtAllowTableAliasInSet";
    public static final String UPDATE_DELETE_STATEMENT_ALLOW_TABLE_ALIAS_IN_WHERE_CLAUSE = "UpdateDeleteStmtAllowTableAliasInWhere";

    /** Whether the GROUP BY has to include all primary expressions selected. */
    public static final String GROUP_BY_REQUIRES_ALL_SELECT_PRIMARIES = "GroupByIncludesAllSelectPrimaries";

    /**
     * Whether the database server supports persist of an unassigned character ("0x0").
     * If not, any unassigned character will be replaced by " " (space) on persist.
     */
    public static final String PERSIST_OF_UNASSIGNED_CHAR = "PersistOfUnassignedChar";

    /**
     * Some databases store character strings in CHAR(XX) columns and when read back in have been padded
     * with spaces.
     */
    public static final String CHAR_COLUMNS_PADDED_WITH_SPACES = "CharColumnsPaddedWithSpaces";

    /**
     * Some databases, Oracle, treats an empty string (0 length) equals null.
     */
    public static final String NULL_EQUALS_EMPTY_STRING = "NullEqualsEmptyString";

    /**
     * Whether this datastore supports batching of statements.
     */
    public static final String STATEMENT_BATCHING = "StatementBatching";

    /**
     * Whether this datastore supports the use of "CHECK" in CREATE TABLE statements (DDL).
     */
    public static final String CHECK_IN_CREATE_STATEMENTS = "CheckInCreateStatements";

    /**
     * Whether this datastore supports the use of CHECK after the column definitions in the
     * CREATE TABLE statements (DDL). for example
     * <PRE>
     * CREATE TABLE MYTABLE
     * (
     *     COL_A int,
     *     COL_B char(1),
     *     PRIMARY KEY (COL_A),
     *     CHECK (COL_B IN ('Y','N'))
     * )
     * </PRE>
     */
    public static final String CHECK_IN_END_CREATE_STATEMENTS = "CheckInEndCreateStatements";

    /**
     * Whether this datastore supports the use of UNIQUE after the column
     * definitions in CREATE TABLE statements (DDL). For example
     * <PRE>
     * CREATE TABLE MYTABLE
     * (
     *     COL_A int,
     *     COL_B char(1),
     *     PRIMARY KEY (COL_A),
     *     UNIQUE (COL_B ...)
     * )
     * </PRE> 
     */
    public static final String UNIQUE_IN_END_CREATE_STATEMENTS = "UniqueInEndCreateStatements";

    /**
     * Whether this datastore supports the use of FOREIGN KEY after the column
     * definitions in CREATE TABLE statements (DDL). For example
     * <PRE>
     * CREATE TABLE MYTABLE
     * (
     *     COL_A int,
     *     COL_B char(1),
     *     FOREIGN KEY (COL_A) REFERENCES TBL2(COL_X)
     * )
     * </PRE> 
     */
    public static final String FK_IN_END_CREATE_STATEMENTS = "FKInEndCreateStatements";

    /**
     * Whether the datastore supports specification of the primary key in CREATE TABLE statements.
     */
    public static final String PRIMARYKEY_IN_CREATE_STATEMENTS = "PrimaryKeyInCreateStatements";

    /**
     * Whether the datastore supports "Statement.getGeneratedKeys".
     */
    public static final String GET_GENERATED_KEYS_STATEMENT = "GetGeneratedKeysStatement";

    /**
     * Whether we support NULLs in candidate keys.
     */
    public static final String NULLS_IN_CANDIDATE_KEYS = "NullsInCandidateKeys";

    /**
     * Whether the database support NULLs in the column options for table creation.
     */
    public static final String NULLS_KEYWORD_IN_COLUMN_OPTIONS = "ColumnOptions_NullsKeyword";

    /**
     * Whether we support DEFAULT tag in CREATE TABLE statements
     */
    public static final String DEFAULT_KEYWORD_IN_COLUMN_OPTIONS = "ColumnOptions_DefaultKeyword";

    /**
     * Whether we support DEFAULT tag together with NOT NULL in CREATE TABLE statements.
     * <pre>CREATE TABLE X ( MEMORY_SIZE BIGINT DEFAULT 0 NOT NULL )</pre>
     * Some databases only support <i>DEFAULT {ConstantExpression | NULL}</i>
     */
    public static final String DEFAULT_KEYWORD_WITH_NOT_NULL_IN_COLUMN_OPTIONS = "ColumnOptions_DefaultWithNotNull";

    /**
     * Whether any DEFAULT tag will be before any NULL/NOT NULL in the column options.
     */
    public static final String DEFAULT_BEFORE_NULL_IN_COLUMN_OPTIONS = "ColumnOptions_DefaultBeforeNull";

    /**
     * Accessor for whether the RDBMS supports ANSI join syntax.
     */
    public static final String ANSI_JOIN_SYNTAX = "ANSI_Join_Syntax";

    /**
     * Accessor for whether the RDBMS supports ANSI cross-join syntax.
     */
    public static final String ANSI_CROSSJOIN_SYNTAX = "ANSI_CrossJoin_Syntax";

    /**
     * Accessor for whether the RDBMS supports cross-join as "INNER 1=1" syntax.
     */
    public static final String CROSSJOIN_ASINNER11_SYNTAX = "ANSI_CrossJoinAsInner11_Syntax";

    /** Whether the row lock should use SELECT ... FOR UPDATE. */
    public static final String LOCK_ROW_USING_SELECT_FOR_UPDATE = "LockRowUsingSelectForUpdate";

    /** Whether this datastore supports SELECT ... WITH UPDLOCK. */
    public static final String LOCK_WITH_SELECT_WITH_UPDLOCK = "LockWithSelectWithUpdlock";

    /** Whether the row lock, when using SELECT ... FOR UPDATE, should also append NOWAIT. */
    public static final String LOCK_ROW_USING_SELECT_FOR_UPDATE_NOWAIT = "LockRowSelectForUpdateNowait";

    /** Whether the row lock is to be placed after the FROM. */
    public static final String LOCK_ROW_USING_OPTION_AFTER_FROM = "LockRowUsingOptionAfterFrom";

    /** Whether the row lock is to be placed within the JOIN clause. */
    public static final String LOCK_ROW_USING_OPTION_WITHIN_JOIN = "LockRowUsingOptionWithinJoin";

    /**
     * Accessor for whether setting a BLOB value allows use of PreparedStatement.setString()
     */
    public static final String BLOB_SET_USING_SETSTRING = "BlobSetUsingSetString";

    /**
     * Accessor for whether setting a CLOB value allows use of PreparedStatement.setString()
     */
    public static final String CLOB_SET_USING_SETSTRING = "ClobSetUsingSetString";

    /**
     * Whether to create indexes before foreign keys.
     */
    public static final String CREATE_INDEXES_BEFORE_FOREIGN_KEYS = "CreateIndexesBeforeForeignKeys";

    /** Whether to support ASC|DESC on columns in an INDEX. */
    public static final String CREATE_INDEX_COLUMN_ORDERING = "CreateIndexColumnOrdering";

    /**
     * Whether to include any ORDER BY columns in a SELECT.
     */
    public static final String INCLUDE_ORDERBY_COLS_IN_SELECT = "IncludeOrderByColumnsInSelect";

    /**
     * Whether DATETIME stores milliseconds.
     */
    public static final String DATETIME_STORES_MILLISECS = "DateTimeStoresMillisecs";

    /**
     * Whether this database supports joining outer and inner queries using columns.
     * i.e can you refer to a column of the outer query in a subquery when the outer query table
     * is not the primary table of the outer query (i.e joined)
     */
    public static final String ACCESS_PARENTQUERY_IN_SUBQUERY_JOINED = "AccessParentQueryInSubquery";

    /** Whether the adapter supports subqueries in the HAVING clause. */
    public static final String SUBQUERY_IN_HAVING = "SubqueryInHaving";

    /** In SAPDB any orderby has to be using the index(es) of any SELECT column(s) rather than their name(s). */
    public static final String ORDERBY_USING_SELECT_COLUMN_INDEX = "OrderByUsingSelectColumnIndex";

    /** Whether we support ANSI "NULLS [FIRST|LAST]" directives in ORDER expressions. */
    public static final String ORDERBY_NULLS_DIRECTIVES = "OrderByWithNullsDirectives";
    /** Whether we support ordering of NULLs using ISNULL. */
    public static final String ORDERBY_NULLS_USING_ISNULL = "OrderByNullsUsingIsNull";
    /** Whether we support ordering of NULLs using {col} IS NULL. */
    public static final String ORDERBY_NULLS_USING_COLUMN_IS_NULL = "OrderByNullsUsingColumnIsNull";
    /** Whether we support ordering of NULLs using "(CASE WHEN [Order] IS NULL THEN 0 ELSE 1 END), [Order]" */
    public static final String ORDERBY_NULLS_USING_CASE_NULL = "OrderByNullsUsingCaseNull";

    /** Whether this datastore supports stored procedures. */
    public static final String STORED_PROCEDURES = "StoredProcs";

    public static final String FK_UPDATE_ACTION_CASCADE = "FkUpdateActionCascade";
    public static final String FK_UPDATE_ACTION_DEFAULT = "FkUpdateActionDefault";
    public static final String FK_UPDATE_ACTION_NULL = "FkUpdateActionNull";
    public static final String FK_UPDATE_ACTION_RESTRICT = "FkUpdateActionRestrict";

    public static final String FK_DELETE_ACTION_CASCADE = "FkDeleteActionCascade";
    public static final String FK_DELETE_ACTION_DEFAULT = "FkDeleteActionDefault";
    public static final String FK_DELETE_ACTION_NULL = "FkDeleteActionNull";
    public static final String FK_DELETE_ACTION_RESTRICT = "FkDeleteActionRestrict";

    public static final String TX_ISOLATION_NONE = "TxIsolationNone";
    public static final String TX_ISOLATION_READ_COMMITTED = "TxIsolationReadCommitted";
    public static final String TX_ISOLATION_READ_UNCOMMITTED = "TxIsolationReadUncommitted";
    public static final String TX_ISOLATION_REPEATABLE_READ = "TxIsolationReadRepeatableRead";
    public static final String TX_ISOLATION_SERIALIZABLE = "TxIsolationSerializable";

    public static final String RESULTSET_TYPE_FORWARD_ONLY = "ResultSetTypeForwardOnly";
    public static final String RESULTSET_TYPE_SCROLL_SENSITIVE = "ResultSetTypeScrollSens";
    public static final String RESULTSET_TYPE_SCROLL_INSENSITIVE = "ResultSetTypeScrollInsens";

    public static final String HOLD_CURSORS_OVER_COMMIT = "HoldCursorsOverCommit";

    public static final String OPERATOR_BITWISE_AND = "BitwiseAndOperator";
    public static final String OPERATOR_BITWISE_OR = "BitwiseOrOperator";
    public static final String OPERATOR_BITWISE_XOR = "BitwiseXOrOperator";

    public static final String NATIVE_ENUM_TYPE = "NativeEnumType";

    public static final String PARAMETER_IN_CASE_IN_UPDATE_CLAUSE = "ParameterInCaseInUpdateClause";

    /** Whether it supports specifying length "type" for a column, e.g VARCHAR(50 BYTE). */
    public static final String COLUMN_LENGTH_SEMANTICS = "ColumnLengthSemantics";

    /**
     * Cloud Spanner needs to use raw strings (r'') so that it can escape /_% characters
     * by only one / character. Otherwise, double // is required. StringMatches method uses this
     * functionality.
     */
    public static final String RAW_PREFIX_LIKE_STATEMENTS = "RawPrefixLikeStatements";

    public static final String INCLUDE_TABLE_INDEX_STATISTICS = "IncludeTableIndexStatistics";

    /**
     * Initialise the datastore adapter.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    void initialise(StoreSchemaHandler handler, ManagedConnection mconn);

    /**
     * Initialise the types for this datastore.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn);

    /**
     * Accessor for the options that are supported by this datastore adapter and the underlying datastore.
     * @return The options (Collection&lt;String&gt;)
     */
    Collection<String> getSupportedOptions();

    /**
     * Accessor for whether the supplied option is supported.
     * @param option The option
     * @return Whether supported.
     */
    boolean supportsOption(String option);

    /**
     * Way for a DatastoreAdapter to specify a preferred default SQL type for a JDBC type (when there are multiple).
     * @param jdbcType The JDBC type
     * @return The SQL type preferred
     */
    String getPreferredDefaultSQLTypeForJDBCType(JdbcType jdbcType);

    /**
     * Return a name for a JDBC Types value.
     * @param jdbcType The jdbc type
     * @return The name
     */
    String getNameForJDBCType(int jdbcType);

    /**
     * Method to return the type given the "jdbc-type" name.
     * @param typeName "jdbc-type" name
     * @return Whether it is valid
     */
    int getJDBCTypeForName(String typeName);

    /**
     * Accessor for a Mapping Manager suitable for use with this datastore adapter.
     * @param storeMgr The StoreManager
     * @return The Mapping Manager.
     */
    MappingManager getMappingManager(RDBMSStoreManager storeMgr);

    /**
     * Accessor for the Vendor ID for this datastore.
     * @return Vendor id for this datastore
     */
    String getVendorID();

    /**
     * Method to check if a word is reserved for this datastore.
     * @param word The word
     * @return Whether it is reserved
     */
    boolean isReservedKeyword(String word);

    /**
     * Creates the auxiliary functions/procedures in the datastore 
     * @param conn the connection to the datastore
     */
    void initialiseDatastore(Connection conn);

    /**
     * Accessor for the quote string to use when quoting identifiers.
     * @return The quote string for the identifier
     */
    String getIdentifierQuoteString();

    /**
     * Accessor for the catalog separator (string to separate the catalog/schema and the identifier).
     * @return Catalog separator string.
     */
    String getCatalogSeparator();

    /**
     * Utility to return the adapter time in case there are rounding issues with millisecs etc.
     * @param time The timestamp
     * @return The time in millisecs
     */
    long getAdapterTime(Timestamp time);

    /**
     * Accessor for the datastore product name.
     * @return product name
     */
    String getDatastoreProductName();

    /**
     * Accessor for the datastore product version.
     * @return product version
     */
    String getDatastoreProductVersion();

    /**
     * Accessor for the datastore driver name.
     * @return product name
     */
    String getDatastoreDriverName();

    /**
     * Accessor for the datastore driver version.
     * @return driver version
     */
    String getDatastoreDriverVersion();

    /**
     * Accessor for the driver major version
     * @return The driver major version
     */
    int getDriverMajorVersion();

    /**
     * Accessor for the driver minor version
     * @return The driver minor version
     */
    int getDriverMinorVersion();

    /**
     * Verifies if the given <code>columnDef</code> is an identity (autoincrement) field type for the datastore.
     * @param columnDef the datastore type name
     * @return true when the <code>columnDef</code> has values for identity generation in the datastore
     **/
    boolean isIdentityFieldDataType(String columnDef);

    /**
     * Return the java type that represents any identity (autoincrement) column value.
     * @param type The type of the member mapping to an IDENTITY column
     * @return The type that should be used in generating the column
     */
    Class getIdentityJavaTypeForType(Class type);

    /**
     * Accessor for the identity (autoincrement) sql statement to get the latest key value for this table.
     * @param table Table (that the autoincrement is for)
     * @param columnName (that the autoincrement is for)
     * @return The statement for getting the latest auto-increment/identity key
     */
    String getIdentityLastValueStmt(Table table, String columnName);

    /**
     * Accessor for the identity (auto-increment) keyword for generating DDLs (CREATE TABLEs...).
     * @param storeMgr The Store manager
     * @return The keyword for a column using auto-increment/identity
     */
    String getIdentityKeyword(StoreManager storeMgr);

    /**
     * Accessor for the identity (auto-increment) keyword for generating DDLs (CREATE TABLEs...).
     * Provides the {@link ColumnMapping} as context for data stores that have different identity
     * keywords based on the mapped Java / JDBC type.
     * Defaults to {@link #getIdentityKeyword(StoreManager)} for backward-compatibility.
     * @param storeMgr The Store manager
     * @param columnMapping The column mapping
     * @return The keyword for a column using auto-increment/identity
     */
    default String getIdentityKeyword(StoreManager storeMgr, ColumnMapping columnMapping)
    {
        return getIdentityKeyword(storeMgr);
    }

    /**
     * Method to return the maximum length of a datastore identifier of the specified type.
     * If no limit exists then returns -1
     * @param identifierType Type of identifier
     * @return The max permitted length of this type of identifier
     */
    int getDatastoreIdentifierMaxLength(IdentifierType identifierType);

    /**
     * Accessor for the maximum foreign keys by table permitted in this datastore.
     * @return Max number of foreign keys
     */
    int getMaxForeignKeys();
    
    /**
     * Accessor for the maximum indexes by table permitted in this datastore.
     * @return Max number of indices
     */
    int getMaxIndexes();

    /**
     * Whether the datastore will support setting the query fetch size to the supplied value.
     * @param size The value to set to
     * @return Whether it is supported.
     */
    boolean supportsQueryFetchSize(int size);

    /**
     * Method to return this object as a string.
     * @return String version of this object.
     */
    String toString();

    /**
     * Accessor for whether this database adapter supports the specified transaction isolation.
     * @param level The isolation level (as defined by Connection enums).
     * @return Whether it is supported.
     */
    boolean supportsTransactionIsolation(int level);

    /**
     * Method to return the SQL to append to the end of the SELECT statement to handle
     * restriction of ranges using the LIMIT keyword. Defaults to an empty string (not supported).
     * SELECT param ... WHERE {LIMIT}
     * @param offset The offset to return from
     * @param count The number of items to return
     * @param hasOrdering Whether there is ordering present
     * @return The SQL to append to allow for ranges using LIMIT.
     */
    String getRangeByLimitEndOfStatementClause(long offset, long count, boolean hasOrdering);

    /**
     * Method to return the column name to use when handling ranges via
     * a rownumber on the select using the original method (DB2). Defaults to an empty string (not supported).
     * @return The row number column.
     */
    String getRangeByRowNumberColumn();

    /**
     * Method to return the column name to use when handling ranges via a rownumber on the select using the second method (Oracle). 
     * Defaults to an empty string (not supported).
     * @return The row number column.
     */
    String getRangeByRowNumberColumn2();

    /**
     * Accessor for table and column information for a catalog/schema in this datastore.
     * @param conn Connection to use
     * @param catalog The catalog (null if none)
     * @param schema The schema (null if none)
     * @param table The table (null if all)
     * @param columnNamePattern The column name (null if all)
     * @return ResultSet containing the table/column information
     * @throws SQLException Thrown if an error occurs
     */
    ResultSet getColumns(Connection conn, String catalog, String schema, String table, String columnNamePattern)
    throws SQLException;

    /**
     * Method to retutn the INSERT statement to use when inserting into a table that has no columns specified. 
     * This is the case when we have a single column in the table and that column is autoincrement/identity (and so is assigned automatically in the datastore).
     * @param table The table
     * @return The statement for the INSERT
     */
    String getInsertStatementForNoColumns(Table table);
    
    /**
     * Returns the precision value to be used when creating string columns of "unlimited" length.
     * Usually, if this value is needed it is provided in the database metadata.
     * However, for some types in some databases the value must be computed.
     * @param typeInfo the typeInfo object for which the precision value is needed.
     * @return the precision value to be used when creating the column, or -1 if no value should be used.
     */
    int getUnlimitedLengthPrecisionValue(SQLTypeInfo typeInfo);

    /**
     * Method to return the statement necessary to create a database with this RDBMS.
     * Note that some RDBMS don't support this.
     * @param catalogName name of the catalog
     * @param schemaName Name of the schema
     * @return The DDL statement
     */
    String getCreateDatabaseStatement(String catalogName, String schemaName);

    /**
     * Method to return the statement necessary to drop a database with this RDBMS.
     * Note that some RDBMS don't support this.
     * @param catalogName Name of the catalog
     * @param schemaName Name of the schema
     * @return The DDL statement
     */
    String getDropDatabaseStatement(String catalogName, String schemaName);

    /**
     * Returns the appropriate SQL to drop the given table.
     * It should return something like:
     * <pre>
     * DROP TABLE FOO CASCADE
     * </pre>
     *
     * @param table The table to drop.
     * @return The text of the SQL statement.
     */
    String getDropTableStatement(Table table);

    /**
     * Method to return the basic SQL for a DELETE TABLE statement.
     * Returns a String like <pre>DELETE FROM tbl t1</pre>. Doesn't include any where clause.
     * @param tbl The SQLTable to delete
     * @return The delete table string
     */
    String getDeleteTableStatement(SQLTable tbl);

    /**
     * Method to return the basic SQL for an UPDATE TABLE statement.
     * Returns a String like <pre>UPDATE tbl t1 SET x1 = val1</pre>. Doesn't include any WHERE clause.
     * @param tbl The SQLTable to update
     * @param setSQL The SQLText for the SET component
     * @return The update table string
     */
    SQLText getUpdateTableStatement(SQLTable tbl, SQLText setSQL);

    /**
     * Returns the appropriate SQL to add a candidate key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_CK UNIQUE (BAZ)
     * </pre>
     *
     * @param ck An object describing the candidate key.
     * @param factory Identifier factory
     * @return  The text of the SQL statement.
     */
    String getAddCandidateKeyStatement(CandidateKey ck, IdentifierFactory factory);

    /**
     * Method to return whether the specified JDBC type is valid for use in a PrimaryKey.
     * @param datatype The JDBC type.
     * @return Whether it is valid for use in the PK
     */
    boolean isValidPrimaryKeyType(JdbcType datatype);
    
    /**
     * Accessor for the SQL statement to add a column to a table.
     * @param table The table
     * @param col The column
     * @return The SQL necessary to add the column
     */
    String getAddColumnStatement(Table table, Column col);

    /**
     * Returns the appropriate SQL to add an index to its table.
     * It should return something like:
     * <pre>
     * CREATE INDEX FOO_N1 ON FOO (BAR,BAZ)
     * CREATE UNIQUE INDEX FOO_U1 ON FOO (BAR,BAZ)
     * </pre>
     *
     * @param idx An object describing the index.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    String getCreateIndexStatement(Index idx, IdentifierFactory factory);

    /**
     * Provide the existing indexes in the database for the table
     * @param conn the JDBC connection
     * @param catalog the catalog name
     * @param schema the schema name
     * @param table the table name
     * @return a ResultSet with the format @see DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     * @throws SQLException if an error occurs
     */
    ResultSet getExistingIndexes(Connection conn, String catalog, String schema, String table)
    throws SQLException;
    
    /**
     * Returns the appropriate SQL to create the given table having the given
     * columns. No column constraints or key definitions should be included.
     * It should return something like:
     * <pre>
     * CREATE TABLE FOO (BAR VARCHAR(30), BAZ INTEGER)
     * </pre>
     *
     * @param table The table to create.
     * @param columns The columns of the table.
     * @param props Properties for controlling the table creation
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    String getCreateTableStatement(TableImpl table, Column[] columns, Properties props, IdentifierFactory factory);

    /**
     * Returns the appropriate SQL to add a primary key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_PK PRIMARY KEY (BAR)
     * </pre>
     *
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory);

    /**
     * Returns the appropriate SQL to add a foreign key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_FK1 FOREIGN KEY (BAR, BAZ) REFERENCES ABC (COL1, COL2)
     * </pre>
     * @param fk An object describing the foreign key.
     * @param factory Identifier factory
     * @return  The text of the SQL statement.
     */
    String getAddForeignKeyStatement(ForeignKey fk, IdentifierFactory factory);

    /**
     * Returns the appropriate SQL to drop the given view.
     * It should return something like:
     * <pre>
     * DROP VIEW FOO
     * </pre>
     *
     * @param view The view to drop.
     * @return The text of the SQL statement.
     */
    String getDropViewStatement(ViewImpl view);

    /**
     * Method returning the text to append to the end of the SELECT to perform the equivalent
     * of "SELECT ... FOR UPDATE" (on some RDBMS). This method means that we can have different
     * text with some datastores (e.g Derby).
     * @return The "FOR UPDATE" text
     */
    String getSelectForUpdateText();

    /**
     * Some databases, Oracle, treats an empty string (0 length) equals null
     * @return returns a surrogate to replace the empty string in the database
     * otherwise it would be treated as null
     */
    String getSurrogateForEmptyStrings();

    /**
     * Accessor for the transaction isolation level to use during schema creation.
     * @return The transaction isolation level for schema generation process
     */
    int getTransactionIsolationForSchemaCreation();

    /**
     * Accessor for the "required" transaction isolation level if it has to be a certain value
     * for this adapter.
     * @return Transaction isolation level (-1 implies no restriction)
     */
    int getRequiredTransactionIsolationLevel();

    /**
     * Accessor for the Catalog Name for this datastore.
     * @param conn Connection to the datastore
     * @return The catalog name
     * @throws SQLException Thrown if error occurs in determining the
     * catalog name.
     **/
    String getCatalogName(Connection conn)
    throws SQLException;

    /**
     * Accessor for the Schema Name for this datastore.
     * @param conn Connection to the datastore
     * @return The schema name
     * @throws SQLException Thrown if error occurs in determining the
     * schema name.
     **/
    String getSchemaName(Connection conn)
    throws SQLException;

    /**
     * The option to specify in "SELECT ... FROM TABLE ... WITH (option)" to lock instances
     * Null if not supported.
     * @return The option to specify with "SELECT ... FROM TABLE ... WITH (option)"
     **/
    String getSelectWithLockOption();

    /**
     * The function to creates a unique value of type uniqueidentifier.
     * @return The function. e.g. "SELECT NEWID()"
     **/
    String getSelectNewUUIDStmt();

    /**
     * Accessor for the sequence statement to get the next id for this 
     * datastore.
     * @param sequenceName Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    String getSequenceNextStmt(String sequenceName);

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
    String getSequenceCreateStmt(String sequenceName, Integer min, Integer max, Integer start, Integer increment, Integer cacheSize);

    /**
     * Convenience method to return whether the specified sequence already exists.
     * @param conn Connection to use for checking
     * @param catalogName Catalog name
     * @param schemaName Schema name
     * @param seqName Name of the sequence
     * @return Whether it exists
     */
    boolean sequenceExists(Connection conn, String catalogName, String schemaName, String seqName);

    /**
     * Accessor for the reserved words constructed from the method DataBaseMetaData.getSQLKeywords + standard SQL reserved words
     * @return Set of reserved words
     */
    Set<String> getReservedWords();

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    String getDatastoreDateStatement();
    
    /**
     * Creates a CHECK constraint definition based on the given values
     * e.g. <pre>CHECK ("COLUMN" IN ('VAL1','VAL2') OR "COLUMN" IS NULL)</pre>
     * @param identifier Column identifier
     * @param values valid values
     * @param nullable whether the datastore identifier is null
     * @return The check constraint
     */
    String getCheckConstraintForValues(DatastoreIdentifier identifier, Object[] values, boolean nullable);

    /**
     * Create a new SQL type info from the current row of the passed ResultSet.
     * Allows an adapter to override particular types where the JDBC driver is known to be buggy.
     * @param rs ResultSet
     * @return The SQL type info
     */
    SQLTypeInfo newSQLTypeInfo(ResultSet rs);

    /**
     * Create a new column info from the current row of the passed ResultSet.
     * Allows an adapter to override particular column information where the JDBC driver is known
     * to be buggy.
     * @param rs Result Set
     * @return The column info
     */
    RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs);

    /**
     * Method to return ForeignKeyInfo for the current row of the ResultSet which will have been
     * obtained from a call to DatabaseMetaData.getImportedKeys() or DatabaseMetaData.getExportedKeys().
     * @param rs The result set returned from DatabaseMetaData.get??portedKeys()
     * @return The foreign key info 
     */
    ForeignKeyInfo newFKInfo(ResultSet rs);

    /**
     * Convenience method to allow adaption of an ordering string before applying it.
     * This is useful where the datastore accepts some conversion adapter around the ordering column
     * for example.
     * @param storeMgr StoreManager
     * @param orderString The basic ordering string
     * @param sqlExpr The sql expression being represented here
     * @return The adapted ordering string
     */
    String getOrderString(StoreManager storeMgr, String orderString, SQLExpression sqlExpr);

    /**
     * Method to return if it is valid to select the specified mapping for the specified statement
     * for this datastore adapter. Sometimes, dependent on the type of the column(s), and what other
     * components are present in the statement, it may be invalid to select the mapping.
     * @param stmt The statement
     * @param m The mapping that we want to select
     * @return Whether it is valid
     */
    boolean validToSelectMappingInStatement(SelectStatement stmt, JavaTypeMapping m);

    /**
     * return whether this exception represents a cancelled statement.
     * @param sqle the exception
     * @return whether it is a cancel
     */
    boolean isStatementCancel(SQLException sqle);

    /**
     * return whether this exception represents a timed out statement.
     * @param sqle the exception
     * @return whether it is a timeout
     */
    boolean isStatementTimeout(SQLException sqle);

    /**
     * Accessor for the function to use for converting to numeric.
     * @return The numeric conversion function for this datastore.
     */
    String getNumericConversionFunction();

    /**
     * The character for escaping characters in pattern expressions.
     * @return the character.
     **/
    String getEscapePatternExpression();

    /**
     * The character for escaping characters in pattern expressions.
     * @return the character.
     **/
    String getEscapeCharacter();

    /**
     * The pattern string for representing one character that is expanded in word searches.
     * Most of databases will use the underscore character.
     * @return the pattern string.
     **/
    String getPatternExpressionAnyCharacter();

    /**
     * The pattern string for representing zero or more characters that is expanded in word searches.
     * Most of databases will use the percent sign character.
     * @return the pattern string.
     **/
    String getPatternExpressionZeroMoreCharacters();

    /**
     * Method to return whether the specified mapping is indexable. Allows a datastore to not index particular column types.
     * @param mapping The mapping
     * @return Whether it is indexable
     */
    boolean validToIndexMapping(JavaTypeMapping mapping);

    /**
     * Accessor for the SQLOperation class for the specified operation (if available for this datastore).
     * @param operationName operation name
     * @return SQLOperation class (or null if none available)
     */
    Class<? extends SQLOperation> getSQLOperationClass(String operationName);

    /**
     * Accessor for the SQLMethod class for the query invocation of specified class + method name (if available for this datastore).
     * @param className Name of the class (or null if this is a STATIC method)
     * @param methodName Method name
     * @param clr ClassLoader resolver, in case <cite>className</cite> is a subclass of a supported type
     * @return The SQLMethod class (or null if not defined for this datastore).
     */
    Class<? extends SQLMethod> getSQLMethodClass(String className, String methodName, ClassLoaderResolver clr);

    /**
     * Method to register a column mapping for a specified java type, and against particular JDBC/SQL type.
     * @param javaTypeName Java type that this is used for
     * @param columnMappingType The column mapping class to use
     * @param jdbcType The JDBC type
     * @param sqlType The SQL type (optional)
     * @param dflt Whether this is the default mapping for this java type
     */
    void registerColumnMapping(String javaTypeName, Class<? extends ColumnMapping> columnMappingType, String jdbcType, String sqlType, boolean dflt);

    /**
     * Method to remove all support for the specified JDBC type (since the JDBC driver doesn't know about it)
     * @param jdbcTypeName The JDBC type
     */
    void deregisterColumnMappingsForJDBCType(String jdbcTypeName);

    /**
     * Method to return the default sql-type for the specified java type (and JDBC type)
     * @param javaType The java type
     * @param jdbcType The JDBC type (optional)
     * @return The SQL type
     */
    String getDefaultSqlTypeForJavaType(String javaType, String jdbcType);

    /**
     * generate select for update lock
     * @param statement
     * @return updated statement
     */
    String generateLockWithSelectForUpdate(String statement);

    /**
     * Method to return the column mapping class to use for the specified java type (and optional JDBC / SQL types).
     * @param javaType The java type of the member
     * @param jdbcType The JDBC type (optional). If provided is used in preference to the java type
     * @param sqlType The SQL type (optional). If provided is used in preference to JDBC type
     * @param clr ClassLoader resolver
     * @param fieldName Name of the field/property (for logging only, can be null).
     * @return The column mapping type to use
     */
    Class<? extends ColumnMapping> getColumnMappingClass(String javaType, String jdbcType, String sqlType, ClassLoaderResolver clr, String fieldName);
}