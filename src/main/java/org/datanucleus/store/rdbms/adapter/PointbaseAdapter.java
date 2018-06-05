/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved.
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
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.Types;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.schema.JDBCTypeInfo;
import org.datanucleus.store.rdbms.schema.RDBMSTypesInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.schema.StoreSchemaHandler;

/**
 * Provides methods for adapting SQL language elements to the Pointbase database.
 */
public class PointbaseAdapter extends BaseDatastoreAdapter
{
    /**
     * Constructor.
     * @param metadata MetaData for the Database
     */
    public PointbaseAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.remove(BOOLEAN_COMPARISON);
        supportedOptions.remove(DEFERRED_CONSTRAINTS); // Pointbase 4.4 certainly doesnt support them
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
        // Based on PointbaseAdapter : PointBase version=5.1 ECF build 295, major=5, minor=1, revision=0
        // Driver name=PointBase JDBC Driver, version=5.1 ECF build 295, major=5, minor=1
        RDBMSTypesInfo typesInfo = (RDBMSTypesInfo)handler.getSchemaData(mconn.getConnection(), "types", null);

        JDBCTypeInfo jdbcType = (JDBCTypeInfo)typesInfo.getChild("9");
        if (jdbcType != null && jdbcType.getNumberOfChildren() > 0)
        {
            // somehow BIGINT is set to 9 in the JDBC driver so add it at its correct value
            SQLTypeInfo dfltTypeInfo = (SQLTypeInfo)jdbcType.getChild("DEFAULT");
            SQLTypeInfo sqlType = new SQLTypeInfo(dfltTypeInfo.getTypeName(),
                (short)Types.BIGINT, dfltTypeInfo.getPrecision(), dfltTypeInfo.getLiteralPrefix(),
                dfltTypeInfo.getLiteralSuffix(), dfltTypeInfo.getCreateParams(), dfltTypeInfo.getNullable(),
                dfltTypeInfo.isCaseSensitive(), dfltTypeInfo.getSearchable(), dfltTypeInfo.isUnsignedAttribute(),
                dfltTypeInfo.isFixedPrecScale(), dfltTypeInfo.isAutoIncrement(), dfltTypeInfo.getLocalTypeName(),
                dfltTypeInfo.getMinimumScale(), dfltTypeInfo.getMaximumScale(), dfltTypeInfo.getNumPrecRadix());
            addSQLTypeForJDBCType(handler, mconn, (short)Types.BIGINT, sqlType, true);
        }

        jdbcType = (JDBCTypeInfo)typesInfo.getChild("16");
        if (jdbcType != null)
        {
            // somehow BOOLEAN is set to 16 in the JDBC driver so add it at its correct location
            SQLTypeInfo dfltTypeInfo = (SQLTypeInfo)jdbcType.getChild("DEFAULT");
            SQLTypeInfo sqlType = new SQLTypeInfo(dfltTypeInfo.getTypeName(),
                (short)Types.BOOLEAN, dfltTypeInfo.getPrecision(), dfltTypeInfo.getLiteralPrefix(),
                dfltTypeInfo.getLiteralSuffix(), dfltTypeInfo.getCreateParams(), dfltTypeInfo.getNullable(),
                dfltTypeInfo.isCaseSensitive(), dfltTypeInfo.getSearchable(), dfltTypeInfo.isUnsignedAttribute(),
                dfltTypeInfo.isFixedPrecScale(), dfltTypeInfo.isAutoIncrement(), dfltTypeInfo.getLocalTypeName(),
                dfltTypeInfo.getMinimumScale(), dfltTypeInfo.getMaximumScale(), dfltTypeInfo.getNumPrecRadix());
            addSQLTypeForJDBCType(handler, mconn, (short)Types.BOOLEAN, sqlType, true);
        }
    }

    /**
     * Accessor for the vendor id.
     * @return The vendor id.
     **/
    public String getVendorID()
    {
        return "pointbase";
    }

    /**
     * Returns the precision value to be used when creating string columns of "unlimited" length.
     * Usually, if this value is needed it is provided in the database metadata. However, for some
     * types in some databases the value must be computed specially.
     * @param typeInfo the typeInfo object for which the precision value is needed.
     * @return  the precision value to be used when creating the column, or -1 if no value should be used.
     */
    public int getUnlimitedLengthPrecisionValue(SQLTypeInfo typeInfo)
    {
        if (typeInfo.getDataType() == Types.BLOB || typeInfo.getDataType() == Types.CLOB)
        {
            return 1 << 31;
        }
        return super.getUnlimitedLengthPrecisionValue(typeInfo);
    }

    /**
     * Load all datastore mappings for this RDBMS database.
     * @param mgr the PluginManager
     * @param clr the ClassLoaderResolver
     */
    protected void loadColumnMappings(PluginManager mgr, ClassLoaderResolver clr)
    {
        // Load up built-in types for this datastore
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BooleanColumnMapping.class, JDBCType.BOOLEAN, "BOOLEAN", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BooleanColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", true);
        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);

        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", true);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);

        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", true);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.ClobColumnMapping.class, JDBCType.CLOB, "CLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NVarcharColumnMapping.class, JDBCType.NVARCHAR, "NVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NCharColumnMapping.class, JDBCType.NCHAR, "NCHAR", false);

        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", true);
        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);

        registerColumnMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", true);

        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DateColumnMapping.class, JDBCType.DATE, "DATE", true);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimeColumnMapping.class, JDBCType.TIME, "TIME", true);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DateColumnMapping.class, JDBCType.DATE, "DATE", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);

        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.DateColumnMapping.class, JDBCType.DATE, "DATE", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerColumnMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BinaryStreamColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.datastore.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

        super.loadColumnMappings(mgr, clr);
    }
}