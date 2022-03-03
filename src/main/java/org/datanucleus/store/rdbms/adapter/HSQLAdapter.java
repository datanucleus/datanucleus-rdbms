/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved.
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
2004 Andy Jefferson - updated to specify the transaction isolation level limitation
2004 Andy Jefferson - changed to use CREATE TABLE of DatabaseAdapter
2004 Andy Jefferson - added MappingManager
2007 R Scott - Sequence support
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
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;

/**
 * Provides methods for adapting SQL language elements to the HSQLDB database.
 */
public class HSQLAdapter extends BaseDatastoreAdapter
{
    /**
     * Constructs a HSQLDB adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public HSQLAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(UNIQUE_IN_END_CREATE_STATEMENTS);
        if (datastoreMajorVersion < 2)
        {
            // HSQLDB 2.0 introduced support for batching and use of getGeneratedKeys
            supportedOptions.remove(STATEMENT_BATCHING);
            supportedOptions.remove(GET_GENERATED_KEYS_STATEMENT);
        }
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(CHECK_IN_CREATE_STATEMENTS);
        supportedOptions.remove(IDENTITY_KEYS_NULL_SPECIFICATION);
        if (datastoreMajorVersion >= 2)
        {
            // HSQLDB 2.0 introduced SELECT ... FOR UPDATE
            supportedOptions.add(LOCK_ROW_USING_SELECT_FOR_UPDATE);
            supportedOptions.add(ORDERBY_NULLS_DIRECTIVES); // Likely came in at 2.0
        }

        // HSQLDB Introduced CHECK in v1.7.2, but ONLY as statements at the end of the CREATE TABLE statement. 
        // We currently don't support anything there other than primary key.
        if (datastoreMajorVersion < 1 ||
            (datastoreMajorVersion == 1 && datastoreMinorVersion < 7) ||
            (datastoreMajorVersion == 1 && datastoreMinorVersion == 7 && datastoreRevisionVersion < 2))
        {
            supportedOptions.remove(CHECK_IN_END_CREATE_STATEMENTS);
        }
        else
        {
            supportedOptions.add(CHECK_IN_END_CREATE_STATEMENTS);
        }

        // Cannot access parent query columns in a subquery (at least in HSQLDB 1.8)
        supportedOptions.remove(ACCESS_PARENTQUERY_IN_SUBQUERY_JOINED);

        supportedOptions.remove(SUBQUERY_IN_HAVING);

        if (datastoreMajorVersion < 1 || (datastoreMajorVersion == 1 && datastoreMinorVersion < 7))
        {
            // HSQLDB before 1.7.* doesn't support foreign keys
            supportedOptions.remove(FK_DELETE_ACTION_CASCADE);
            supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
            supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
            supportedOptions.remove(FK_DELETE_ACTION_NULL);
            supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
            supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
            supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
            supportedOptions.remove(FK_UPDATE_ACTION_NULL);
        }
        else if (datastoreMajorVersion < 2)
        {
            // HSQLDB 2.0 introduced RESTRICT on FKs
            supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
            supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
        }

        if (datastoreMajorVersion < 2)
        {
            // HSQLDB 2.0 introduced support for REPEATABLE_READ
            supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
            if (datastoreMinorVersion <= 7)
            {
                // v1.8 introduced support for READ_UNCOMMITTED, SERIALIZABLE
                supportedOptions.remove(TX_ISOLATION_READ_COMMITTED);
                supportedOptions.remove(TX_ISOLATION_SERIALIZABLE);
            }
        }
        supportedOptions.remove(TX_ISOLATION_NONE);
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
        // CLOB - not present before v2.0
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.adapter.HSQLTypeInfo(
            "LONGVARCHAR", (short)Types.CLOB, 2147483647, "'", "'", null, 1, true, (short)3, false, false, false, "LONGVARCHAR", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);

        // BLOB - not present before v2.0
        sqlType = new org.datanucleus.store.rdbms.adapter.HSQLTypeInfo(
            "LONGVARBINARY", (short)Types.BLOB, 2147483647, "'", "'", null, 1, false, (short)3, false, false, false, "LONGVARBINARY", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BLOB, sqlType, true);

        if (datastoreMajorVersion >= 2)
        {
            // LONGVARBINARY - not present after v2.0
            sqlType = new org.datanucleus.store.rdbms.adapter.HSQLTypeInfo(
                "LONGVARBINARY", (short)Types.LONGVARBINARY, 2147483647, "'", "'", null, 1, false, (short)3, false, false, false, "LONGVARBINARY", (short)0, (short)0, 0);
            addSQLTypeForJDBCType(handler, mconn, (short)Types.LONGVARBINARY, sqlType, true);
        }

        // LONGVARCHAR - not present in 2.0+
        sqlType = new org.datanucleus.store.rdbms.adapter.HSQLTypeInfo(
            "LONGVARCHAR", (short)Types.LONGVARCHAR, 2147483647, "'", "'", null, 1, true, (short)3, false, false, false, "LONGVARCHAR", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.LONGVARCHAR, sqlType, true);

        if (datastoreMajorVersion >= 2 && datastoreMinorVersion >= 4)
        {
            // UUID
            sqlType = new org.datanucleus.store.rdbms.adapter.HSQLTypeInfo(
                "UUID", (short)Types.BINARY, 2147483647, "'", "'", null, 1, false, (short)3, false, false, false, "UUID", (short)0, (short)0, 0);
            addSQLTypeForJDBCType(handler, mconn, (short)Types.BINARY, sqlType, true);
        }

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

        sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.LONGVARBINARY);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a LONGVARBINARY
            }
        }

        sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.LONGVARCHAR);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a LONGVARCHAR
            }
        }
    }

    /**
     * Accessor for the vendor ID for this adapter.
     * @return The vendor ID
     */
    public String getVendorID()
    {
        return "hsql";
    }

    /**
     * Method to return the maximum length of a datastore identifier of the specified type.
     * If no limit exists then returns -1
     * @param identifierType Type of identifier (see IdentifierFactory.TABLE, etc)
     * @return The max permitted length of this type of identifier
     */
    public int getDatastoreIdentifierMaxLength(IdentifierType identifierType)
    {
        if (identifierType == IdentifierType.TABLE)
        {
            return MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.COLUMN)
        {
            return MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.CANDIDATE_KEY)
        {
            return MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.FOREIGN_KEY)
        {
            return MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.INDEX)
        {
            return MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.PRIMARY_KEY)
        {
            return MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.SEQUENCE)
        {
            return MAX_IDENTIFIER_LENGTH;
        }
        else
        {
            return super.getDatastoreIdentifierMaxLength(identifierType);
        }
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        return "DROP SCHEMA IF EXISTS " + schemaName + " CASCADE";
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
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle restriction of ranges using the LIMIT keyword.
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
            // HSQLDB doesn't allow just offset so use Integer.MAX_VALUE as count
            return "LIMIT " + Integer.MAX_VALUE + " OFFSET " + offset + " ";
        }
        else
        {
            return "";
        }
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.adapter.HSQLTypeInfo(rs);
    }

    /**
     * Accessor for the Schema Name for this datastore. HSQLDB 1.7.0 does not support schemas (catalog).
     * @param conn Connection to the datastore
     * @return The schema name
     * @throws SQLException Thrown if error occurs in determining the schema name.
     */
    public String getSchemaName(Connection conn)
    throws SQLException
    {
        return "";
    }

    /**
     * Add a primary key using ALTER TABLE. We use CREATE TABLE for this.
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        return null;
    }

    /**
     * Returns the appropriate SQL to drop the given table.
     * It should return something like:
     * <blockquote><pre>
     * DROP TABLE FOO
     * </pre></blockquote>
     *
     * @param table The table to drop.
     * @return  The text of the SQL statement.
     */
    public String getDropTableStatement(Table table)
    {
        // TODO Now supports CASCADE
        return "DROP TABLE " + table.toString();
    }

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     */
    public String getIdentityLastValueStmt(Table table, String columnName)
    {
        return "CALL IDENTITY()";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @param storeMgr The Store Manager
     * @return The keyword for a column using auto-increment
     */
    public String getIdentityKeyword(StoreManager storeMgr)
    {
        // Note that we don't use "IDENTITY" here since that defaults to
        // GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY and this may not be intended as PK
        return "GENERATED BY DEFAULT AS IDENTITY";
    }

    /**
     * Method to return the INSERT statement to use when inserting into a table that has no columns specified. 
     * This is the case when we have a single column in the table and that column is autoincrement/identity (and so is assigned automatically in the datastore).
     * @param table The table
     * @return The INSERT statement
     */
    public String getInsertStatementForNoColumns(Table table)
    {
        return "INSERT INTO " + table.toString() + " VALUES (null)";
    }

    /**
     * Accessor for whether the specified type is allow to be part of a PK.
     * @param datatype The JDBC type
     * @return Whether it is permitted in the PK
     */
    public boolean isValidPrimaryKeyType(JdbcType datatype)
    {
        if (datatype == JdbcType.BLOB || datatype == JdbcType.CLOB || datatype == JdbcType.LONGVARBINARY || datatype == JdbcType.OTHER || datatype == JdbcType.LONGVARCHAR)
        {
            return false;
        }
        return true;
    }

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "CALL NOW()";
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
    public String getSequenceCreateStmt(String sequenceName, Integer min,Integer max,Integer start,Integer increment,Integer cacheSize)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequenceName);
        if (min != null)
        {
            stmt.append(" START WITH " + min);
        }
        else if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        if (max != null)
        {
            throw new NucleusUserException(Localiser.msg("051022"));
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (cacheSize != null)
        {
            throw new NucleusUserException(Localiser.msg("051023"));
        }

        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence for this datastore.
     * @param sequenceName Name of the sequence 
     * @return The statement for getting the next id for the sequence
     */
    public String getSequenceNextStmt(String sequenceName)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }
        StringBuilder stmt=new StringBuilder("CALL NEXT VALUE FOR ");
        stmt.append(sequenceName);

        return stmt.toString();
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
            if ("avg".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction.class;
            else if ("AVG".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction.class;
            else if ("SECOND".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod5.class;
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
                if ("startsWith".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringStartsWith3Method.class;
                else if ("trim".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrim3Method.class;
                else if ("trimLeft".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrimLeft3Method.class;
                else if ("trimRight".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrimRight3Method.class;
            }
            else if ("java.util.Date".equals(className) || (cls != null && java.util.Date.class.isAssignableFrom(cls)))
            {
                if ("getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod5.class;
            }
            else if ("java.time.LocalTime".equals(className) && "getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod5.class;
            else if ("java.time.LocalDateTime".equals(className) && "getSecond".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.TemporalSecondMethod5.class;
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
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.BOOLEAN, "BOOLEAN", true);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", true);
        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);

        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        if (datastoreMajorVersion >= 2)
        {
            // No FLOAT/REAL type provided by JDBC driver in HSQLDB v2.x
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", false);
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);
        }
        else
        {
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
            registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);
        }

        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", true);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);

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
        registerColumnMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

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

        if (datastoreMajorVersion >= 2 && datastoreMinorVersion >= 4)
        {
            // Native UUID supported from v2.4. Seems to need to be specified using stmt.setObject(...), and retrieved using rs.getObject(), so use OTHER
            registerColumnMapping(java.util.UUID.class.getName(), org.datanucleus.store.rdbms.mapping.column.OtherColumnMapping.class, JDBCType.OTHER, "UUID", false);
            registerColumnMapping(java.util.UUID.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
            registerColumnMapping(java.util.UUID.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        }

        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "BINARY", false);

        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryColumnMapping.class, JDBCType.BINARY, "BINARY", false);

        registerColumnMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryStreamColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

        super.loadColumnMappings(mgr, clr);
    }
}