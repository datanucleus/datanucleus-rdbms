/**********************************************************************
Copyright (c) 2003 Andy Jefferson and others. All rights reserved.
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
2006 Thomas Corte - updates for Sybase 15
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.Types;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.table.Table;

/**
 * Provides methods for adapting SQL language elements to the Sybase database.
 */
public class SybaseAdapter extends BaseDatastoreAdapter
{
    /**
     * Constructor.
     * @param metadata MetaData for the DB
     */
    public SybaseAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(BOOLEAN_COMPARISON);
        supportedOptions.remove(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.remove(AUTO_INCREMENT_KEYS_NULL_SPECIFICATION);
    }

    public String getVendorID()
    {
        return "sybase";
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("Sybase does not support dropping schema with cascade. You need to drop all tables first");
    }

    /**
     * Accessor for the DROP TABLE statement for Sybase.
     * Sybase doesnt support CASCADE CONSTRAINTS so we just return a simple
     * DROP TABLE table-name
     * @param table The table to drop.
     * @return The DROP TABLE statement
     **/
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

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        SQLTypeInfo info = new SQLTypeInfo(rs);

        // Discard the tinyint type because it doesn't support negative values.
        if (info.getTypeName().toLowerCase().startsWith("tinyint"))
        {
            return null;
        }
        // Discard the longsysname type because it doesn't allow specification of length
        if (info.getTypeName().toLowerCase().startsWith("longsysname"))
        {
            return null;
        }
        return info;
    }

    /**
     * Method to create a column info for the current row.
     * Overrides the dataType/columnSize/decimalDigits to cater for Sybase particularities.
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

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     */
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        return "SELECT @@IDENTITY";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     */
    public String getAutoIncrementKeyword()
    {
        return "IDENTITY";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLMethodClass(java.lang.String, java.lang.String)
     */
    @Override
    public Class getSQLMethodClass(String className, String methodName, ClassLoaderResolver clr)
    {
        if (className != null)
        {
            if ("java.lang.String".equals(className))
            {
                if ("indexOf".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringIndexOf4Method.class;
                else if ("substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring2Method.class;
            }
        }

        return super.getSQLMethodClass(className, methodName, clr);
    }

    /**
     * Load all datastore mappings for this RDBMS database.
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
        registerDatastoreMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INT", false);

        registerDatastoreMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerDatastoreMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerDatastoreMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerDatastoreMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.RealRDBMSMapping.class, JDBCType.REAL, "REAL", false);
        registerDatastoreMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INT", true);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerDatastoreMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INT", true);
        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerDatastoreMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerDatastoreMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerDatastoreMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerDatastoreMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INT", false);
        registerDatastoreMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping.class, JDBCType.TINYINT, "TINYINT", false);

        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", true);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.ClobRDBMSMapping.class, JDBCType.CLOB, "CLOB", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "BLOB", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NVarcharRDBMSMapping.class, JDBCType.NVARCHAR, "NVARCHAR", false);
        registerDatastoreMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NCharRDBMSMapping.class, JDBCType.NCHAR, "NCHAR", false);

        registerDatastoreMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping.class, JDBCType.DECIMAL, "DECIMAL", true);
        registerDatastoreMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);

        registerDatastoreMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", true);

        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DateRDBMSMapping.class, JDBCType.DATE, "DATE", true);
        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimeRDBMSMapping.class, JDBCType.TIME, "TIME", true);
        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

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

        registerDatastoreMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarBinaryRDBMSMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerDatastoreMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "BLOB", false);
        registerDatastoreMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarBinaryRDBMSMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerDatastoreMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarBinaryRDBMSMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerDatastoreMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping.class, JDBCType.BLOB, "BLOB", false);
        registerDatastoreMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarBinaryRDBMSMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerDatastoreMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BinaryStreamRDBMSMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping.class, JDBCType.CHAR, "CHAR", false);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerDatastoreMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping.class, JDBCType.INTEGER, "INT", true);

        super.loadDatastoreMappings(mgr, clr);
    }
}