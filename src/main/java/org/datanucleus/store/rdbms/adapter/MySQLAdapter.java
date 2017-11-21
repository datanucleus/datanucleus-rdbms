/**********************************************************************
Copyright (c) 2003 Mike Martin (TJDO) and others. All rights reserved. 
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
2003 Andy Jefferson - added getCreateTableStatement() method and commented
2004 Andy Jefferson - fixed convert expression
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Properties;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedMapping;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the MySQL database.
 * Note that this also supports the MariaDB database.
 */
public class MySQLAdapter extends BaseDatastoreAdapter
{
    /**
     * A string containing the list of MySQL keywords that are not also SQL/92 <i>reserved words</i>, separated by commas.
     * This list is normally obtained dynamically from the driver using DatabaseMetaData.getSQLKeywords(), but MySQL drivers are known to return an incomplete list.
     * <p>
     * This list was produced based on the reserved word list in the MySQL Manual (Version 4.0.10-gamma) at http://www.mysql.com/doc/en/Reserved_words.html.
     */
    public static final String NONSQL92_RESERVED_WORDS =
        "ANALYZE,AUTO_INCREMENT,BDB,BERKELEYDB,BIGINT,BINARY,BLOB,BTREE," +
        "CHANGE,COLUMNS,DATABASE,DATABASES,DAY_HOUR,DAY_MINUTE,DAY_SECOND," +
        "DELAYED,DISTINCTROW,DIV,ENCLOSED,ERRORS,ESCAPED,EXPLAIN,FIELDS," +
        "FORCE,FULLTEXT,FUNCTION,GEOMETRY,HASH,HELP,HIGH_PRIORITY," +
        "HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,INDEX,INFILE,INNODB,KEYS,KILL," +
        "LIMIT,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,LONG,LONGBLOB," +
        "LONGTEXT,LOW_PRIORITY,MASTER_SERVER_ID,MEDIUMBLOB,MEDIUMINT," +
        "MEDIUMTEXT,MIDDLEINT,MINUTE_SECOND,MOD,MRG_MYISAM,OPTIMIZE," +
        "OPTIONALLY,OUTFILE,PURGE,REGEXP,RENAME,REPLACE,REQUIRE,RETURNS," +
        "RLIKE,RTREE,SHOW,SONAME,SPATIAL,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS," +
        "SQL_SMALL_RESULT,SSL,STARTING,STRAIGHT_JOIN,STRIPED,TABLES," +
        "TERMINATED,TINYBLOB,TINYINT,TINYTEXT,TYPES,UNLOCK,UNSIGNED,USE," +
        "USER_RESOURCES,VARBINARY,VARCHARACTER,WARNINGS,XOR,YEAR_MONTH," +
        "ZEROFILL";

    /**
     * Constructor.
     * Overridden so we can add on our own list of NON SQL92 reserved words
     * which is returned incorrectly with the JDBC driver.
     * @param metadata MetaData for the DB
     **/
    public MySQLAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(NONSQL92_RESERVED_WORDS));

        supportedOptions.remove(ALTER_TABLE_DROP_CONSTRAINT_SYNTAX);
        if (datastoreMajorVersion < 4 ||
            (datastoreMajorVersion == 4 && datastoreMinorVersion == 0 && datastoreRevisionVersion < 13))
        {
            supportedOptions.remove(ALTER_TABLE_DROP_FOREIGN_KEY_CONSTRAINT);
        }
        else
        {
            // MySQL version 4.0.13 started supporting this syntax
            supportedOptions.add(ALTER_TABLE_DROP_FOREIGN_KEY_CONSTRAINT);
        }
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(DEFAULT_BEFORE_NULL_IN_COLUMN_OPTIONS);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        if (datastoreMajorVersion < 5 && (datastoreMajorVersion < 4 || datastoreMinorVersion < 1))
        {
            // Support starts at MySQL 4.1
            supportedOptions.remove(EXISTS_SYNTAX);
        }
        else
        {
            supportedOptions.add(EXISTS_SYNTAX);
        }
        if (datastoreMajorVersion < 4)
        {
            supportedOptions.remove(UNION_SYNTAX);
        }
        else
        {
            supportedOptions.add(UNION_SYNTAX);
        }
        supportedOptions.add(BLOB_SET_USING_SETSTRING);
        supportedOptions.add(CLOB_SET_USING_SETSTRING);
        supportedOptions.add(CREATE_INDEXES_BEFORE_FOREIGN_KEYS);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.add(ORDERBY_NULLS_USING_ISNULL);

        // MySQL DATETIME/TIMESTAMP doesn't store millisecs!
        // http://feedblog.org/2007/05/26/why-doesnt-mysql-support-millisecond-datetime-resolution/
        // TODO Actually this is no longer true ... MariaDB 5.3+, MySQL 5.7+ but can have problems storing nanos
        supportedOptions.remove(DATETIME_STORES_MILLISECS);
//      if (driverName.equalsIgnoreCase("mariadb-jdbc"))
//      {
//          // TODO Do this different for MariaDB if it handles it right
//      }

        supportedOptions.add(OPERATOR_BITWISE_AND);
        supportedOptions.add(OPERATOR_BITWISE_OR);
        supportedOptions.add(OPERATOR_BITWISE_XOR);

        supportedOptions.add(NATIVE_ENUM_TYPE);

        supportedOptions.remove(VALUE_GENERATION_UUID_STRING); // MySQL charsets don't seem to allow this
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
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.adapter.MySQLTypeInfo(
            "MEDIUMBLOB", (short)Types.BLOB, 2147483647, null, null, null, 1, false, (short)1, false, false, false, "MEDIUMBLOB", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BLOB, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.MySQLTypeInfo(
            "MEDIUMTEXT", (short)Types.CLOB, 2147483647, null, null, null, 1, true, (short)1, false, false, false, "MEDIUMTEXT", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);
    }

    public String getVendorID()
    {
        return "mysql";
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
 
        // MySQL also allows identifiers with '-' as well as many other unicode characters, but then they need quoting
        if (word != null && word.indexOf('-') >= 0)
        {
            return true;
        }
        return false;
    }

    /**
     * Method to create a column info for the current row.
     * Overrides the dataType for BLOB/CLOB as necessary
     * @param rs ResultSet from DatabaseMetaData.getColumns()
     * @return column info
     */
    public RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs)
    {
        RDBMSColumnInfo info = super.newRDBMSColumnInfo(rs);

        short dataType = info.getDataType();
        String typeName = info.getTypeName();

        // Fix to an issue in the MySQL JDBC driver 3.1.13. For columns of type BLOB, the driver returns -4 but should return 2004 
        if (dataType == Types.LONGVARBINARY && typeName.equalsIgnoreCase("mediumblob"))
        {
            //change it to BLOB, since it is a BLOB
            info.setDataType((short)Types.BLOB);
        }

        // Fix to an issue in the MySQL JDBC driver 3.1.13. For columns of type CLOB, the driver returns -1 but should return 2005 
        if (dataType == Types.LONGVARCHAR && typeName.equalsIgnoreCase("mediumtext"))
        {
            //change it to CLOB, since it is a CLOB
            info.setDataType((short)Types.CLOB);
        }

        return info;
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        SQLTypeInfo info = new org.datanucleus.store.rdbms.adapter.MySQLTypeInfo(rs);

        // The following block originated in TJDO, and was carried across up to DataNucleus 3.0-m4
        // It is now commented out so people can use BINARY/VARBINARY. What is it trying to achieve?
        // Exclude BINARY and VARBINARY since these equate to CHAR(M) BINARY and VARCHAR(M) BINARY respectively, 
        // which aren't true binary types (e.g. trailing space characters are stripped).
      /*String typeName = info.getTypeName();
        if (typeName.equalsIgnoreCase("binary") || typeName.equalsIgnoreCase("varbinary"))
        {
            return null;
        }*/

        return info;
    }

    // ------------------------------- Schema Methods ------------------------------------

    public String getCreateDatabaseStatement(String catalogName, String schemaName)
    {
        return "CREATE DATABASE IF NOT EXISTS " + catalogName;
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        return "DROP DATABASE IF EXISTS " + catalogName;
    }

    /**
     * MySQL, when using AUTO_INCREMENT, requires the primary key specified
     * in the CREATE TABLE, so we do nothing here. 
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        return null;
    }

    /**
     * Method to return the CREATE TABLE statement.
     * Versions before 5 need INNODB table type selecting for them.
     * It seems, MySQL &ge; 5 still needs innodb in order to support transactions.
     * @param table The table
     * @param columns The columns in the table
     * @param props Properties for controlling the table creation
     * @param factory Identifier factory
     * @return The creation statement 
     */
    public String getCreateTableStatement(TableImpl table, Column[] columns, Properties props, IdentifierFactory factory)  
    {
        StringBuilder createStmt = new StringBuilder(super.getCreateTableStatement(table, columns, props, factory));

        // Check for specification of the "engine"
        String engineType = "INNODB";
        if (props != null && props.containsKey("mysql-engine-type"))
        {
            engineType = props.getProperty("mysql-engine-type");
        }
        else if (table.getStoreManager().hasProperty(RDBMSPropertyNames.PROPERTY_RDBMS_MYSQL_ENGINETYPE))
        {
            engineType = table.getStoreManager().getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_MYSQL_ENGINETYPE);
        }
        if (datastoreMajorVersion >= 5 ||
            (datastoreMajorVersion == 4 && datastoreMinorVersion >= 1 && datastoreRevisionVersion >= 2) ||
            (datastoreMajorVersion == 4 && datastoreMinorVersion == 0 && datastoreRevisionVersion >= 18))
        {
            // "ENGINE=" was introduced in 4.1.2 and 4.0.18 (http://dev.mysql.com/doc/refman/4.1/en/create-table.html)
            createStmt.append(" ENGINE=" + engineType);
        }
        else
        {
            createStmt.append(" TYPE=" + engineType);
        }

        // Check for specification of the "collation"
        String collation = null;
        if (props != null && props.contains("mysql-collation"))
        {
            collation = props.getProperty("mysql-collation");
        }
        else if (table.getStoreManager().hasProperty(RDBMSPropertyNames.PROPERTY_RDBMS_MYSQL_COLLATION))
        {
            collation = table.getStoreManager().getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_MYSQL_COLLATION);
        }
        if (collation != null)
        {
            createStmt.append(" COLLATE=").append(collation);
        }

        // Check for specification of the "charset"
        String charset = null;
        if (props != null && props.contains("mysql-character-set"))
        {
            charset = props.getProperty("mysql-character-set");
        }
        else if (table.getStoreManager().hasProperty(RDBMSPropertyNames.PROPERTY_RDBMS_MYSQL_CHARACTERSET))
        {
            charset = table.getStoreManager().getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_MYSQL_CHARACTERSET);
        }
        if (charset != null)
        {
            createStmt.append(" CHARACTER SET=").append(charset);
        }
        return createStmt.toString();
    }

    /**
     * Method to return the DROP TABLE statement.
     * @param table The table
     * @return The drop statement
     **/ 
    public String getDropTableStatement(Table table)
    {
        if (datastoreMajorVersion < 5)
        {
            // Earlier versions of MySQL didn't support the CASCADE keyword, whereas now it does but does nothing
            return "DROP TABLE " + table.toString();
        }
        return super.getDropTableStatement(table);
    }

    /**
     * Accessor for the SQL statement to add a column to a table.
     * @param table The table
     * @param col The column
     * @return The SQL necessary to add the column
     */
    public String getAddColumnStatement(Table table, Column col)
    {
        return "ALTER TABLE " + table.toString() + " ADD COLUMN " + col.getSQLDefinition();
    }

    /**
     * Method to return the basic SQL for a DELETE TABLE statement.
     * Returns the String as <code>DELETE t1 FROM tbl t1</code>. Doesn't include any where clause.
     * @param tbl The SQLTable to delete
     * @return The delete table string
     */
    public String getDeleteTableStatement(SQLTable tbl)
    {
        return "DELETE " + tbl.getAlias() + " FROM " + tbl.toString();
    }

    // ------------------------------- Identity Methods ------------------------------------

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     **/
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        return "SELECT LAST_INSERT_ID()";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     **/
    public String getAutoIncrementKeyword()
    {
        return "AUTO_INCREMENT";
    }

    /**
     * The function to creates a unique value of type uniqueidentifier.
     * MySQL generates 36-character hex uuids.
     * @return The function. e.g. "SELECT uuid()"
     **/
    public String getSelectNewUUIDStmt()
    {
        return "SELECT uuid()";
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
     * restriction of ranges using the LIMUT keyword.
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
            return "LIMIT " + offset + "," + count + " ";
        }
        else if (offset <= 0 && count > 0)
        {
            return "LIMIT " + count + " ";
        }
        else if (offset >= 0 && count < 0)
        {
            // MySQL doesnt allow just offset so use Long.MAX_VALUE as count
            return "LIMIT " + offset + "," + Long.MAX_VALUE + " ";
        }
        else
        {
            return "";
        }
    }

    /**
     * The character for escaping patterns.
     * @return Escape character(s)
     **/
    public String getEscapePatternExpression()
    {
        return "ESCAPE '\\\\'";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#validToIndexMapping(org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping)
     */
    @Override
    public boolean validToIndexMapping(JavaTypeMapping mapping)
    {
        // TODO Improve this to omit BLOB/CLOB only
        if (mapping instanceof SerialisedMapping)
        {
            return false;
        }
        return super.validToIndexMapping(mapping);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLOperationClass(java.lang.String)
     */
    @Override
    public Class getSQLOperationClass(String operationName)
    {
        if ("concat".equals(operationName)) return org.datanucleus.store.rdbms.sql.operation.Concat2Operation.class;
        else if ("numericToString".equals(operationName)) return org.datanucleus.store.rdbms.sql.operation.NumericToString2Operation.class;

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
            if ("DAY_OF_WEEK".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod3.class;
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
                else if ("startsWith".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringStartsWith3Method.class;
                else if ("trim".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrim3Method.class;
                else if ("trimLeft".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrimLeft3Method.class;
                else if ("trimRight".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrimRight3Method.class;
            }
            if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
            {
                if ("getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod3.class;
            }
            if ("java.time.LocalDate".equals(className) && "getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod3.class;
            if ("java.time.LocalDateTime".equals(className) && "getDayOfWeek".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalDayOfWeekMethod3.class;
        }

        return super.getSQLMethodClass(className, methodName, clr);
    }

    /**
     * Load all datastore mappings defined in the associated plugins.
     * We handle RDBMS datastore mappings so refer to rdbms-mapping-class, jdbc-type, sql-type in particular.
     * @param mgr the PluginManager
     * @param clr the ClassLoaderResolver
     */
    protected void loadDatastoreMappings(PluginManager mgr, ClassLoaderResolver clr)
    {
        // Load up built-in types for this datastore
        registerDatastoreMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BitRDBMSMapping.class, JDBCType.BIT, "BIT", true);
        registerDatastoreMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BooleanRDBMSMapping.class, JDBCType.BOOLEAN, "BOOLEAN", false);
        registerDatastoreMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BooleanRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerDatastoreMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerDatastoreMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", true);
        registerDatastoreMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerDatastoreMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", true);
        registerDatastoreMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INTEGER", false);

        registerDatastoreMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerDatastoreMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerDatastoreMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.FloatRDBMSMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerDatastoreMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
        registerDatastoreMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.RealRDBMSMapping.class, JDBCType.REAL, "REAL", false);
        registerDatastoreMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INTEGER", true);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INT", false);
        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerDatastoreMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerDatastoreMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerDatastoreMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", false);

        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", true);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.ClobRDBMSMapping.class, JDBCType.CLOB, "CLOB", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "BLOB", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping.class, JDBCType.LONGVARCHAR, "LONGTEXT", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping.class, JDBCType.LONGVARCHAR, "MEDIUMTEXT", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping.class, JDBCType.LONGVARCHAR, "TEXT", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "LONGBLOB", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "MEDIUMBLOB", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NVarcharRDBMSMapping.class, JDBCType.NVARCHAR, "NVARCHAR", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NCharRDBMSMapping.class, JDBCType.NCHAR, "NCHAR", false);

        registerDatastoreMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping.class, JDBCType.DECIMAL, "DECIMAL", true);
        registerDatastoreMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);

        registerDatastoreMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", true);

        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DateRDBMSMapping.class, JDBCType.DATE, "DATE", true);
        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimeRDBMSMapping.class, JDBCType.TIME, "TIME", true);
        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerDatastoreMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerDatastoreMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerDatastoreMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DateRDBMSMapping.class, JDBCType.DATE, "DATE", false);
        registerDatastoreMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimeRDBMSMapping.class, JDBCType.TIME, "TIME", false);

        registerDatastoreMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerDatastoreMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DateRDBMSMapping.class, JDBCType.DATE, "DATE", false);
        registerDatastoreMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerDatastoreMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimeRDBMSMapping.class, JDBCType.TIME, "TIME", false);
        registerDatastoreMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerDatastoreMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarBinaryRDBMSMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerDatastoreMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "BLOB", false);
        registerDatastoreMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarBinaryRDBMSMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerDatastoreMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BinaryRDBMSMapping.class, JDBCType.BINARY, "BINARY", false);

        registerDatastoreMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarBinaryRDBMSMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerDatastoreMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "BLOB", false);
        registerDatastoreMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarBinaryRDBMSMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerDatastoreMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BinaryRDBMSMapping.class, JDBCType.BINARY, "BINARY", false);

        registerDatastoreMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BinaryStreamRDBMSMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

        super.loadDatastoreMappings(mgr, clr);
    }
}