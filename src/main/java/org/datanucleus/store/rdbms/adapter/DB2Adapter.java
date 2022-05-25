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
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the DB2 database.
 * @see BaseDatastoreAdapter
 */
public class DB2Adapter extends BaseDatastoreAdapter
{
    /**
     * A string containing the list of DB2 keywords 
     * This list is normally obtained dynamically from the driver using "DatabaseMetaData.getSQLKeywords()".
     * Based on database DB2 version 7.
     */
    public static final String DB2_RESERVED_WORDS =
        "ACCESS,ALIAS,ALLOW,ASUTIME,AUDIT,AUX,AUXILIARY,BUFFERPOOL," +
        "CAPTURE,CCSID,CLUSTER,COLLECTION,COLLID,COMMENT,CONCAT," +
        "CONTAINS,COUNT_BIG,CURRENT_LC_PATH,CURRENT_SERVER," +
        "CURRENT_TIMEZONE,DATABASE,DAYS,DB2GENERAL,DB2SQL,DBA," +
        "DBINFO,DBSPACE,DISALLOW,DSSIZE,EDITPROC,ERASE,EXCLUSIVE," +
        "EXPLAIN,FENCED,FIELDPROC,FILE,FINAL,GENERATED,GRAPHIC,HOURS," +
        "IDENTIFIED,INDEX,INTEGRITY,ISOBID,JAVA,LABEL,LC_CTYPE,LINKTYPE," +
        "LOCALE,LOCATORS,LOCK,LOCKSIZE,LONG,MICROSECOND,MICROSECONDS," +
        "MINUTES,MODE,MONTHS,NAME,NAMED,NHEADER,NODENAME,NODENUMBER," +
        "NULLS,NUMPARTS,OBID,OPTIMIZATION,OPTIMIZE,PACKAGE,PAGE," +
        "PAGES,PART,PCTFREE,PCTINDEX,PIECESIZE,PLAN,PRIQTY,PRIVATE," +
        "PROGRAM,PSID,QYERYNO,RECOVERY,RENAME,RESET,RESOURCE,RRN,RUN," +
        "SCHEDULE,SCRATCHPAD,SECONDS,SECQTY,SECURITY,SHARE,SIMPLE," +
        "SOURCE,STANDARD,STATISTICS,STAY,STOGROUP,STORES,STORPOOL," +
        "STYLE,SUBPAGES,SYNONYM,TABLESPACE,TYPE,VALIDPROC,VARIABLE," +
        "VARIANT,VCAT,VOLUMES,WLM,YEARS";
    
    /**
     * Constructs a DB2 adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public DB2Adapter(DatabaseMetaData metadata)
    {
        super(metadata);

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(DB2_RESERVED_WORDS));

        // Update supported options
        supportedOptions.add(LOCK_ROW_USING_SELECT_FOR_UPDATE);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.add(USE_UNION_ALL);
        supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
        supportedOptions.remove(BOOLEAN_COMPARISON);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(NULLS_IN_CANDIDATE_KEYS);
        supportedOptions.remove(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
        supportedOptions.remove(DISTINCT_WITH_SELECT_FOR_UPDATE);
        supportedOptions.remove(GROUPING_WITH_SELECT_FOR_UPDATE);
        supportedOptions.remove(HAVING_WITH_SELECT_FOR_UPDATE);
        supportedOptions.remove(ORDERING_WITH_SELECT_FOR_UPDATE);
        supportedOptions.remove(MULTITABLES_WITH_SELECT_FOR_UPDATE);
        supportedOptions.remove(CREATE_INDEX_COLUMN_ORDERING);

        supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
        supportedOptions.remove(FK_UPDATE_ACTION_NULL);
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
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.adapter.DB2TypeInfo(
            "FLOAT", (short)Types.FLOAT, 53, null, null, null, 1, false, (short)2, false, false, false, null, (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.FLOAT, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.DB2TypeInfo(
            "NUMERIC", (short)Types.NUMERIC, 31, null, null, "PRECISION,SCALE", 1, false, (short)2, false, false, false, null, (short)0, (short)31, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.NUMERIC, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.DB2TypeInfo(
            "BIGINT", (short)Types.BIGINT, 20, null, null, null, 1, false, (short)2, false, true, false, null, (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BIGINT, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.adapter.DB2TypeInfo(
            "XML", (short)Types.SQLXML, 2147483647, null, null, null, 1, false, (short)2, false, false, false, null, (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.SQLXML, sqlType, true);

        // DB2 doesn't have "BIT" JDBC type mapped, so map as SMALLINT
        sqlType = new org.datanucleus.store.rdbms.adapter.DB2TypeInfo(
            "SMALLINT", (short)Types.SMALLINT, 5, null, null, null, 1, false, (short)2, false, true, false, null, (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BIT, sqlType, true);
    }

    public String getVendorID()
    {
        return "db2";
    }
    
    public String getSchemaName(Connection conn) throws SQLException
    {
        Statement stmt = conn.createStatement();
        
        try
        {
            String stmtText = "VALUES (CURRENT SCHEMA)";
            ResultSet rs = stmt.executeQuery(stmtText);

            try
            {
                if (!rs.next())
                {
                    throw new NucleusDataStoreException("No result returned from " + stmtText).setFatal();
                }

                return rs.getString(1).trim();
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

    /**
     * Method to return the maximum length of a datastore identifier of the specified type.
     * If no limit exists then returns -1
     * @param identifierType Type of identifier (see IdentifierFactory.TABLE, etc)
     * @return The max permitted length of this type of identifier
     */
    public int getDatastoreIdentifierMaxLength(IdentifierType identifierType)
    {
        if (identifierType == IdentifierType.CANDIDATE_KEY)
        {
            return 18;
        }
        else if (identifierType == IdentifierType.FOREIGN_KEY)
        {
            return 18;
        }
        else if (identifierType == IdentifierType.INDEX)
        {
            return 18;
        }
        else if (identifierType == IdentifierType.PRIMARY_KEY)
        {
            return 18;
        }
        else
        {
            return super.getDatastoreIdentifierMaxLength(identifierType);
        }
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.adapter.DB2TypeInfo(rs);
    }

    /**
     * Method to create a column info for the current row.
     * Overrides the dataType/columnSize/decimalDigits to cater for DB2 particularities.
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

    public String getDropDatabaseStatement(String schemaName, String catalogName)
    {
        throw new UnsupportedOperationException("DB2 does not support dropping schema with cascade. You need to drop all tables first");
    }

    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    /**
     * Continuation string to use where the SQL statement goes over more than 1
     * line. DB2 doesn't convert newlines into continuation characters and so
     * we just provide a space so that it accepts the statement.
     * @return Continuation string.
     */
    public String getContinuationString()
    {
        return "";
    }

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     */
    public String getIdentityLastValueStmt(Table table, String columnName)
    {
        return "VALUES IDENTITY_VAL_LOCAL()";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @param storeMgr Store Manager
     * @return The keyword for a column using auto-increment
     */
    public String getIdentityKeyword(StoreManager storeMgr)
    {
        return "generated always as identity (start with 1)";
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
    public String getSequenceCreateStmt(String sequenceName, Integer min,Integer max, Integer start,Integer increment, Integer cacheSize)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }
        
        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequenceName);
        stmt.append(" AS INTEGER ");

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
            stmt.append(" NOCACHE");
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
        StringBuilder stmt=new StringBuilder("VALUES NEXTVAL FOR ");
        stmt.append(sequenceName);

        return stmt.toString();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#getRangeByRowNumberColumn()
     */
    public String getRangeByRowNumberColumn()
    {
        return "row_number()over()";
    }

    /**
     * return whether this exception represents a cancelled statement.
     * @param sqle the exception
     * @return whether it is a cancel
     */
    public boolean isStatementCancel(SQLException sqle)
    {
        if (sqle.getErrorCode() == -952)
        {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#isStatementTimeout(java.sql.SQLException)
     */
    @Override
    public boolean isStatementTimeout(SQLException sqle)
    {
        if (sqle.getSQLState() != null && sqle.getSQLState().equals("57014") && (sqle.getErrorCode() == -952 || sqle.getErrorCode() == -905))
        {
            return true;
        }

        return super.isStatementTimeout(sqle);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLOperationClass(java.lang.String)
     */
    @Override
    public Class getSQLOperationClass(String operationName)
    {
        if ("mod".equals(operationName)) return org.datanucleus.store.rdbms.sql.operation.Mod3Operation.class;
        else if ("concat".equals(operationName)) return org.datanucleus.store.rdbms.sql.operation.Concat3Operation.class;

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
        }
        else
        {
            if ("java.lang.String".equals(className))
            {
                if ("concat".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringConcat2Method.class;
                else if ("indexOf".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringIndexOf3Method.class;
                else if ("length".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringLength3Method.class;
                else if ("substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring3Method.class;
                else if ("translate".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTranslateMethod.class;
                else if ("trim".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringTrim2Method.class;
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
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BitColumnMapping.class, JDBCType.BIT, "BIT", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.BOOLEAN, "BOOLEAN", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);

        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);

        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", true);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
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
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.DB2DatalinkColumnMapping.class, JDBCType.DATALINK, "DATALINK", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.SqlXmlColumnMapping.class, JDBCType.SQLXML, "SQLXML", false);
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