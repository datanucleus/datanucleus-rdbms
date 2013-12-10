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

import java.sql.DatabaseMetaData;
import java.sql.Types;

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
        else
        {
            return super.getUnlimitedLengthPrecisionValue(typeInfo);
        }
    }
}