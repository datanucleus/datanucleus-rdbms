/**********************************************************************
 Copyright 2021 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 except in compliance with the License. You may obtain a copy of the License at

 https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under the
 License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied. See the License for the specific language governing permissions and
 limitations under the License.

 Contributors:
 2021 Yunus Durmus - Spanner support
 **********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import static org.datanucleus.metadata.JdbcType.DECIMAL;
import static org.datanucleus.metadata.JdbcType.NUMERIC;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * CloudSpannerAdapter defines the types, features that are supported and also
 * deviate from the {@link BaseDatastoreAdapter}.
 */
public class CloudSpannerAdapter extends BaseDatastoreAdapter {

  public static final String CLOUD_SPANNER_RESERVED_KEYWORDS =
      "ALL,AND,ANY,ARRAY,AS,ASC,ASSERT_ROWS_MODIFIED,AT,BETWEEN,"
          + "BY,CASE,CAST,COLLATE,CONTAINS,CREATE,CROSS,CUBE,CURRENT,"
          + "DEFAULT,DEFINE,DESC,DISTINCT,ELSE,END,ENUM,ESCAPE,EXCEPT,"
          + "EXCLUDE,EXISTS,EXTRACT,FALSE,FETCH,FOLLOWING,FOR,FROM,FULL,"
          + "GROUP,GROUPING,GROUPS,HASH,HAVING,IF,IGNORE,IN,INNER,INTERSECT,"
          + "INTERVAL,INTO,IS,JOIN,LATERAL,LEFT,LIKE,LIMIT,LOOKUP,MERGE,"
          + "NATURAL,NEW,NO,NOT,NULL,NULLS,OF,ON,OR,ORDER,OUTER,OVER,"
          + "PARTITION,PRECEDING,PROTO,RANGE,RECURSIVE,RESPECT,RIGHT,"
          + "ROLLUP,ROWS,SELECT,SET,SOME,STRUCT,TABLESAMPLE,THEN,TO,TREAT,"
          + "TRUE,UNBOUNDED,UNION,UNNEST,USING,WHEN,WHERE,WINDOW,WITH,WITHIN";

  public CloudSpannerAdapter(DatabaseMetaData metadata) {
    super(metadata);
    NucleusLogger.DATASTORE.debug("Initializing Cloud Spanner Adapter");

    reservedKeywords.addAll(
        StringUtils.convertCommaSeparatedStringToSet(CLOUD_SPANNER_RESERVED_KEYWORDS));

    //    SUPPORTED OPTIONS. Below we list all the supported options.
    //    Some of them are not mentioned as they are available in BaseAdapter class

    supportedOptions.remove(IDENTITY_COLUMNS);
    supportedOptions.remove(SEQUENCES);
    supportedOptions.remove(VALUE_GENERATION_UUID_STRING);
    supportedOptions.remove(SOME_ANY_ALL_SUBQUERY_EXPRESSIONS);
    supportedOptions.remove(ANALYSIS_METHODS);

    supportedOptions.remove(ALTER_TABLE_DROP_FOREIGN_KEY_CONSTRAINT);
    supportedOptions.remove(CHAR_COLUMNS_PADDED_WITH_SPACES);
    supportedOptions.remove(NULL_EQUALS_EMPTY_STRING);
    supportedOptions.remove(UNIQUE_IN_END_CREATE_STATEMENTS);
    supportedOptions.remove(NULLS_IN_CANDIDATE_KEYS);
    supportedOptions.remove(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
    supportedOptions.remove(DEFAULT_KEYWORD_IN_COLUMN_OPTIONS);
    supportedOptions.remove(DEFAULT_KEYWORD_WITH_NOT_NULL_IN_COLUMN_OPTIONS);
    supportedOptions.remove(DEFAULT_BEFORE_NULL_IN_COLUMN_OPTIONS);
    supportedOptions.remove(IDENTITY_KEYS_NULL_SPECIFICATION);
    supportedOptions.remove(IDENTITY_COLUMN_TYPE_SPECIFICATION);
    // Spanner provides LOCK_SCANNED_RANGES hint for exclusive locking.
    // Since it is a hint, we pretend that Database does not support it.
    supportedOptions.remove(LOCK_ROW_USING_SELECT_FOR_UPDATE);
    supportedOptions.remove(LOCK_ROW_USING_SELECT_FOR_UPDATE_NOWAIT);
    supportedOptions.remove(LOCK_ROW_USING_OPTION_AFTER_FROM);
    supportedOptions.remove(LOCK_ROW_USING_OPTION_WITHIN_JOIN);
    supportedOptions.remove(DISTINCT_WITH_SELECT_FOR_UPDATE);
    supportedOptions.remove(GROUPING_WITH_SELECT_FOR_UPDATE);
    supportedOptions.remove(HAVING_WITH_SELECT_FOR_UPDATE);
    supportedOptions.remove(ORDERING_WITH_SELECT_FOR_UPDATE);
    supportedOptions.remove(MULTITABLES_WITH_SELECT_FOR_UPDATE);
    supportedOptions.remove(CREATE_INDEXES_BEFORE_FOREIGN_KEYS);
    supportedOptions.remove(ACCESS_PARENTQUERY_IN_SUBQUERY_JOINED);
    supportedOptions.remove(ORDERBY_NULLS_DIRECTIVES);
    supportedOptions.remove(ORDERBY_NULLS_USING_ISNULL);
    supportedOptions.remove(ORDERBY_NULLS_USING_COLUMN_IS_NULL);
    supportedOptions.remove(ORDERBY_NULLS_USING_CASE_NULL);
    supportedOptions.remove(STORED_PROCEDURES);
    supportedOptions.remove(FK_DELETE_ACTION_CASCADE);
    supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
    supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
    supportedOptions.remove(FK_DELETE_ACTION_NULL);
    supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
    supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
    supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
    supportedOptions.remove(FK_UPDATE_ACTION_NULL);

    // Primary key is at the end not inside the statement
    supportedOptions.remove(PRIMARYKEY_IN_CREATE_STATEMENTS);
    //  FKs in create statements are allowed in Spanner
    //  but its logic in Datanucleus has not been implemented
    //  hence we make unsupport this feature to add FK
    //  constraints via Alter Table statements.
    supportedOptions.remove(CHECK_IN_CREATE_STATEMENTS);
    supportedOptions.remove(FK_IN_END_CREATE_STATEMENTS);
    supportedOptions.remove(DEFERRED_CONSTRAINTS);
    supportedOptions.remove(TX_ISOLATION_READ_COMMITTED);
    supportedOptions.remove(TX_ISOLATION_READ_UNCOMMITTED);
    supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
    supportedOptions.remove(ESCAPE_EXPRESSION_IN_LIKE_PREDICATE);


    supportedOptions.add(BIT_IS_REALLY_BOOLEAN); // Bit is stored as Bool
    supportedOptions.add(PROJECTION_IN_TABLE_REFERENCE_JOINS);
    supportedOptions.add(GROUP_BY_REQUIRES_ALL_SELECT_PRIMARIES);
    supportedOptions.add(CROSSJOIN_ASINNER11_SYNTAX);
    supportedOptions.add(BLOB_SET_USING_SETSTRING);
    supportedOptions.add(CLOB_SET_USING_SETSTRING);
    supportedOptions.add(ORDERBY_USING_SELECT_COLUMN_INDEX);
    supportedOptions.add(USE_UNION_ALL);
    supportedOptions.add(RAW_PREFIX_LIKE_STATEMENTS);
  }

  /**
   * How vendor calls this driver
   *
   * @return the vendor naming
   */
  @Override
  public String getVendorID() {
    return "cloudspanner";
  }

  /**
   * Spanner INFORMATION_SCHEMA works only in read-only transactions
   * https://cloud.google.com/spanner/docs/information-schema
   *
   * @return transaction isolation level for schema creation
   */
  @Override
  public int getTransactionIsolationForSchemaCreation() {
    return Connection.TRANSACTION_SERIALIZABLE;
  }

  /**
   * This function adds on any missing JDBC types when not available from driver metadata Spanner
   * driver only provides the common types. We should map the missing ones.
   *
   * <p>JDBC type -> Spanner type
   * ----------------------------
   * nvarchar -> string
   * bigint -> int64
   * binary -> byte
   * double -> float64
   * boolean -> bool
   * date -> date
   * timestamp -> timestamp
   * numeric -> numeric
   *
   * <p>Copied the mappings from Hibernate,
   * https://github.com/GoogleCloudPlatform/google-cloud-spanner-hibernate/blob/master/google-cloud-spanner-hibernate-dialect/src/main/java/com/google/cloud/spanner/hibernate/SpannerDialect.java
   *
   * <p>The precision values are obtained from Spanner JDBC driver metadata
   * https://github.com/googleapis/java-spanner-jdbc/blob/master/src/main/java/com/google/cloud/spanner/jdbc/JdbcDatabaseMetaData.java
   *
   * @param handler SchemaHandler that we initialise the types for
   * @param mconn managed connection to use
   */
  @Override
  public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn) {
    super.initialiseTypes(handler, mconn);

    SQLTypeInfo sqlType =
        new CloudSpannerTypeInfo(
            "BOOL",
            (short) Types.BIT,
            0,
            null,
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            true,
            false,
            false,
            "BOOL",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.BIT, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "BYTES",
            (short) Types.BLOB,
            10485760,
            null,
            null,
            "(length)",
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            true,
            false,
            false,
            "BYTES",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.BLOB, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "STRING",
            (short) Types.CHAR,
            2621440,
            null,
            null,
            "(length)",
            DatabaseMetaData.typeNullable,
            true,
            (short) DatabaseMetaData.typePredChar,
            true,
            false,
            false,
            "STRING",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.CHAR, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "STRING",
            (short) Types.CLOB,
            2621440,
            null,
            null,
            "(length)",
            DatabaseMetaData.typeNullable,
            true,
            (short) DatabaseMetaData.typeSearchable,
            true,
            false,
            false,
            "STRING",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.CLOB, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "NUMERIC",
            (short) Types.DECIMAL,
            2621440,
            null,
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            false,
            false,
            false,
            "NUMERIC",
            (short) 0,
            (short) 0,
            10);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.DECIMAL, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "FLOAT64",
            (short) Types.FLOAT,
            15,
            null,
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            false,
            false,
            false,
            "FLOAT64",
            (short) 0,
            (short) 0,
            2);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.FLOAT, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "INT64",
            (short) Types.INTEGER,
            19,
            null,
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            false,
            false,
            false,
            "INT64",
            (short) 0,
            (short) 0,
            10);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.INTEGER, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "BYTES",
            (short) Types.LONGVARBINARY,
            10485760,
            null,
            null,
            "(MAX)",
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            true,
            false,
            false,
            "BYTES",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.LONGVARBINARY, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "STRING",
            (short) Types.LONGVARCHAR,
            2621440,
            null,
            null,
            "(MAX)",
            DatabaseMetaData.typeNullable,
            true,
            (short) DatabaseMetaData.typeSearchable,
            true,
            false,
            false,
            "STRING",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.LONGVARCHAR, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "FLOAT64",
            (short) Types.REAL,
            15,
            null,
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            false,
            false,
            false,
            "FLOAT64",
            (short) 0,
            (short) 0,
            2);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.REAL, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "INT64",
            (short) Types.SMALLINT,
            19,
            null,
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            false,
            false,
            false,
            "INT64",
            (short) 0,
            (short) 0,
            10);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.SMALLINT, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "TIMESTAMP",
            (short) Types.TIME,
            35,
            "TIMESTAMP ",
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            true,
            false,
            false,
            "TIMESTAMP",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.TIME, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "INT64",
            (short) Types.TINYINT,
            19,
            null,
            null,
            null,
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            false,
            false,
            false,
            "INT64",
            (short) 0,
            (short) 0,
            10);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.TINYINT, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "BYTES",
            (short) Types.VARBINARY,
            10485760,
            null,
            null,
            "(length)",
            DatabaseMetaData.typeNullable,
            false,
            (short) DatabaseMetaData.typePredBasic,
            true,
            false,
            false,
            "BYTES",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.VARBINARY, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "STRING",
            (short) Types.VARCHAR,
            2621440,
            null,
            null,
            "(length)",
            DatabaseMetaData.typeNullable,
            true,
            (short) DatabaseMetaData.typeSearchable,
            true,
            false,
            false,
            "STRING",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.VARCHAR, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "STRING",
            (short) Types.NCHAR,
            2621440,
            null,
            null,
            "(length)",
            DatabaseMetaData.typeNullable,
            true,
            (short) DatabaseMetaData.typeSearchable,
            true,
            false,
            false,
            "STRING",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.NCHAR, sqlType, true);

    sqlType =
        new CloudSpannerTypeInfo(
            "STRING",
            (short) Types.NCLOB,
            2621440,
            null,
            null,
            "(length)",
            DatabaseMetaData.typeNullable,
            true,
            (short) DatabaseMetaData.typeSearchable,
            true,
            false,
            false,
            "STRING",
            (short) 0,
            (short) 0,
            0);
    addSQLTypeForJDBCType(handler, mconn, (short) Types.NCLOB, sqlType, true);
  }

  /**
   * Spanner does not support NUMERIC (and hence DECIMAL) as a valid
   * primary key column type.
   *
   * @param jdbcType the jdbc type to check
   * @return
   */
  @Override
  public boolean isValidPrimaryKeyType(JdbcType jdbcType) {
    return !(jdbcType == NUMERIC || jdbcType == DECIMAL);
  }

  /**
   * Does Spanner support sequence statements? -> no.
   *
   * @return false since Spanner does not support sequence statements
   */
  public boolean sequenceExists(Connection conn, String catalogName, String schemaName, String seqName)
  {
    return false;
  }

  /**
   * Create database statement for Spanner JDBC.
   *
   * @param catalogName catalog name (does not exist in Spanner)
   * @param schemaName schema name
   */
  @Override
  public String getCreateDatabaseStatement(String catalogName, String schemaName) {
    return "CREATE DATABASE " + schemaName;
  }

  /**
   * Drop database statement for Spanner JDBC
   *
   * @param catalogName catalog name (does not exist in Spanner)
   * @param schemaName schema name
   */
  @Override
  public String getDropDatabaseStatement(String catalogName, String schemaName) {
    return "DROP DATABASE " + schemaName;
  }

  /**
   * Drop table statement
   *
   * @param table to drop.
   */
  @Override
  public String getDropTableStatement(Table table) {
    return "DROP TABLE " + table.toString();
  }

  /**
   * Creates a spanner table with primary key.
   * Many other features like check, constraint, interleave, cascade are not supported yet.
   *
   * It is better to create table without using Datanucleus. Instead, use plain SQL statements.
   *
   * @param table the table to create.
   * @param columns the columns of the table.
   * @param props properties for controlling the table creation
   * @param factory factory for identifiers
   */
  @Override
  public String getCreateTableStatement(
      TableImpl table, Column[] columns, Properties props, IdentifierFactory factory) {
    StringBuilder createStmt = new StringBuilder();
    String indent = getContinuationString().length() == 0 ? "" : "    ";

    // CREATE TABLE with column specifiers
    createStmt.append("CREATE TABLE ").append(table.toString())
        .append(getContinuationString())
        .append("(")
        .append(getContinuationString());
    for (int i = 0; i < columns.length; ++i)
    {
      if (i > 0)
      {
        createStmt.append(",").append(getContinuationString());
      }

      createStmt.append(indent).append(columns[i].getSQLDefinition());
    }

    // We don't set check and foreign key statements since they need a constraint name which
    // we don't know how to generate.

    // finish the column definitions block
    createStmt.append(getContinuationString()).append(")");

    // we add primary key at the end
    PrimaryKey pk = table.getPrimaryKey();
    if (pk != null && pk.getNumberOfColumns() > 0) {
      createStmt.append(indent).append(pk.toString());
    }

    return createStmt.toString();
  }

  /**
   * Cannot add or change primary key after creation
   *
   * @param pk an object describing the primary key.
   * @param factory identifier factory
   */
  @Override
  public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory) {
    return null;
  }

  /**
   * Escape pattern is not supported in Spanner
   *
   * @return the character.
   */
  @Override
  public String getEscapePatternExpression() {
    return "";
  }

  /**
   * The character for escaping characters in pattern expressions.
   *
   * @return the character.
   */
  @Override
  public String getEscapeCharacter() {
    return "\\\\";
  }

  /**
   * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
   * restriction of ranges using the LIMIT keyword.
   *
   * @param offset the offset to return from
   * @param count the number of items to return
   * @param hasOrdering whether ordering is present
   * @return the SQL to append to allow for ranges using OFFSET/FETCH.
   */
  @Override
  public String getRangeByLimitEndOfStatementClause(long offset, long count, boolean hasOrdering) {
    String limitClause = String.format("LIMIT %d ", count);
    return offset > 0 ? limitClause + String.format("OFFSET %d ", offset) : limitClause;
  }

  public SQLTypeInfo newSQLTypeInfo(ResultSet rs){
    return new CloudSpannerTypeInfo(rs);
  }

  public boolean isStatementCancel(SQLException sqle)
  {
    // https://github.com/googleapis/common-protos-java/blob/master/proto-google-common-protos/src/main/java/com/google/rpc/Code.java
    return sqle.getErrorCode() == 1;
  }

  public boolean isStatementTimeout(SQLException sqle)
  {
    // https://github.com/googleapis/common-protos-java/blob/master/proto-google-common-protos/src/main/java/com/google/rpc/Code.java
    return sqle.getErrorCode() == 4;
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
    else if ("HOUR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
    else if ("MINUTE".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
    else if ("SECOND".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod7.class;
    else if ("WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalWeekMethod5.class;
    else if ("QUARTER".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalQuarterMethod5.class;
    else if ("DAY_OF_WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod7.class;
    else if ("DAY_OF_YEAR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfYearMethod.class;
    else if ("ISOYEAR".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalIsoYearMethod.class;
    else if ("ISOWEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalIsoWeekMethod.class;
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
        if ("charAt".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringCharAt2Method.class;
        else if ("endsWith".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringEndsWith2Method.class;
        else if ("equals".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringEqualsMethod.class;
        else if ("equalsIgnoreCase".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringEqualsIgnoreCaseMethod.class;
        else if ("indexOf".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringIndexOf5Method.class;
        else if ("length".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringLength3Method.class;
        else if ("matches".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringMatchesMethod.class;
        else if ("replaceAll".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringReplaceAllMethod.class;
        else if ("startsWith".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringStartsWith4Method.class;
        else if ("substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring3Method.class;
        else if ("toUpperCase".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringToUpperMethod.class;
        else if ("toLowerCase".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringToLowerMethod.class;
        else if ("trim".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrimMethod.class;
        else if ("trimLeft".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrimLeftMethod.class;
        else if ("trimRight".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrimRightMethod.class;
      }
      if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
      {
        if ("getDay".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
        else if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod7.class;
        else if ("getDate".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
        else if ("getMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthJavaMethod5.class;
        else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
        else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
        else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
        else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod7.class;
      }

      if ("java.time.LocalTime".equals(className))
      {
        if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
        else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
        else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod7.class;
      }
      if ("java.time.LocalDate".equals(className))
      {
        if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
        else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
        else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
      }
      if ("java.time.LocalDateTime".equals(className))
      {
        if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
        else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
        else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
        else if ("getHour".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalHourMethod6.class;
        else if ("getMinute".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMinuteMethod6.class;
        else if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod7.class;
      }
      if ("java.time.MonthDay".equals(className))
      {
        if ("getDayOfMonth".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
        else if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
      }
      if ("java.time.Period".equals(className))
      {
        if ("getMonths".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
        else if ("getDays".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayMethod5.class;
        else if ("getYears".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
      }
      if ("java.time.YearMonth".equals(className))
      {
        if ("getMonthValue".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalMonthMethod5.class;
        else if ("getYear".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalYearMethod5.class;
      }
      if ("java.util.Optional".equals(className))
      {
        if ("get".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.OptionalGetMethod.class;
        else if ("isPresent".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.OptionalIsPresentMethod.class;
        else if ("orElse".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.OptionalOrElseMethod.class;
      }
    }

    return super.getSQLMethodClass(className, methodName, clr);
  }

  /**
   * Load all datastore mappings defined in the associated plugins.
   * We handle RDBMS datastore mappings so refer to rdbms-mapping-class, jdbc-type, sql-type in particular.
   *
   *    SQL Type -> Spanner Type
   *    ---------  -------------
   *    nvarchar -> string
   *    bigint -> int64
   *    binary -> byte
   *    double -> float64
   *    boolean -> bool
   *    date -> date
   *    timestamp -> timestamp
   *    numeric -> numeric
   * @param mgr the PluginManager
   * @param clr the ClassLoaderResolver
   */
  @Override
  protected void loadColumnMappings(PluginManager mgr, ClassLoaderResolver clr)
  {
    // Load up built-in types for this datastore
    registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BitColumnMapping.class, JDBCType.BIT, "BIT", false);
    registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.BOOLEAN, "BOOLEAN", true);
    registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
    registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

    // We are not able to use BYTES type since the spanner BYTES type requires a length while datanucleus does not provide length for single byte java type.
    // So we follow the same design as MySQL adapter.
    registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", true);
    registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

    registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.NVarcharColumnMapping.class, JDBCType.NVARCHAR, "NVARCHAR", true);
    registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);

    registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
    registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);
    registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);

    registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
    registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
    registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
    registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);
    registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);

    registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", true);
    registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
    registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
    registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
    registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

    registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
    registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", true);
    registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
    registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
    registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

    registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
    registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
    registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);

    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.ClobColumnMapping.class, JDBCType.CLOB, "CLOB", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "LONGTEXT", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "MEDIUMTEXT", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "TEXT", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "LONGBLOB", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "MEDIUMBLOB", false);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NVarcharColumnMapping.class, JDBCType.NVARCHAR, "NVARCHAR", true);
    registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NCharColumnMapping.class, JDBCType.NCHAR, "NCHAR", false);

    registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);
    registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", true);

    registerColumnMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", true);

    registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", true);
    registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
    registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
    registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

    registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);
    registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
    registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
    registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

    registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
    registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
    registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", false);
    registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);

    registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
    registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", true);
    registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
    registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);
    registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

    registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
    registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
    registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
    registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "BINARY", false);

    registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
    registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
    registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
    registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "BINARY", false);

    registerColumnMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryStreamColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

    registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
    registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", true);
    registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
    registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
    registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

    super.loadColumnMappings(mgr, clr);
  }
}

