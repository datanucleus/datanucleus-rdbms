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

import java.sql.ResultSet;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;

import java.sql.Types;

/**
 * SQL Type info for Google Cloud Spanner datastore.
 */
public class CloudSpannerTypeInfo extends SQLTypeInfo
{

    public CloudSpannerTypeInfo(ResultSet rs)
    {
        super(rs);
    }

    public CloudSpannerTypeInfo(String typeName, short dataType, int precision, String literalPrefix,
            String literalSuffix, String createParams, int nullable, boolean caseSensitive, short searchable,
            boolean unsignedAttribute, boolean fixedPrecScale, boolean autoIncrement, String localTypeName,
            short minimumScale, short maximumScale, int numPrecRadix)
    {
        super(typeName, dataType, precision, literalPrefix, literalSuffix, createParams, nullable, caseSensitive,
                searchable, unsignedAttribute, fixedPrecScale, autoIncrement, localTypeName, minimumScale, maximumScale,
                numPrecRadix);

        fixAllowsPrecisionSpec();
    }

    private void fixAllowsPrecisionSpec()
    {
        if (typeName.equalsIgnoreCase("STRING") || typeName.equalsIgnoreCase("BYTES"))
        {
            // Only BYTES and STRING spanner types support "STRING(number)"
            allowsPrecisionSpec = true;
        }
        else
        {
            allowsPrecisionSpec = false;
        }

    }

    public boolean isCompatibleWith(RDBMSColumnInfo colInfo)
    {
        int expected = getDataType();
        int actual = colInfo.getDataType();

        return super.isCompatibleWith(colInfo) || (isStringType(expected) && isStringType(actual)) || (isBooleanType(expected) && isBooleanType(
            actual)) || (isByteType(expected) && isByteType(actual));

    }

    private static boolean isByteType(int type)
    {
        switch (type)
        {
            case Types.BLOB :
            case Types.BINARY :
            case Types.VARBINARY :
            case Types.LONGVARBINARY :
                return true;
            default :
                return false;
        }
    }

    private static boolean isBooleanType(int type)
    {
        switch (type)
        {
            case Types.BIT :
            case Types.BOOLEAN :
                return true;
            default :
                return false;
        }
    }

    /**
     * Tests whether or not the given JDBC type is a Cloud Spanner "string" type.
     * <p/>
     * For Spanner all character related types are indeed string
     */
    private static boolean isStringType(int type)
    {
        switch (type)
        {
            case Types.CHAR :
            case Types.VARCHAR :
            case Types.LONGVARCHAR :
            case Types.NCHAR :
            case Types.NVARCHAR :
            case Types.LONGNVARCHAR :
            case Types.CLOB :
            case Types.NCLOB :
                return true;

            default :
                return false;
        }
    }
}
