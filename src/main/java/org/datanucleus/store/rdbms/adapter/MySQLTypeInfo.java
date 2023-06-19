/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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

import java.sql.ResultSet;
import java.sql.Types;

import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;

/**
 * SQL Type info for MySQL datastores.
 */
public class MySQLTypeInfo extends SQLTypeInfo
{
    /**
     * Constructor
     * @param rs Result set from the database metadata
     */
    public MySQLTypeInfo(ResultSet rs)
    {
        super(rs);
        if (typeName.equalsIgnoreCase("FLOAT"))
        {
            // MySQL maps FLOAT to JDBC type REAL, so reset it
            dataType = (short)Types.FLOAT;
        }
        else if (typeName.equalsIgnoreCase("CHAR"))
        {
            // Use the BINARY qualifier to specify case-sensitive treatment of CHAR.
            // The M in (M) will get replaced by the actual precision in Column.getSQLDefinition().
            typeName = "CHAR(M) BINARY";
            createParams = "";
        }
        else if (typeName.equalsIgnoreCase("VARCHAR"))
        {
            // Use the BINARY qualifier to specify case-sensitive treatment of VARCHAR.
            // The M in (M) will get replaced by the actual precision in Column.getSQLDefinition().
            typeName = "VARCHAR(M) BINARY";
            createParams = "";
        }
        fixAllowsPrecisionSpec();
    }

    public MySQLTypeInfo(String typeName, short dataType, int precision, String literalPrefix,
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
        if (typeName.equalsIgnoreCase("LONG VARCHAR") ||
            typeName.equalsIgnoreCase("TINYBLOB") ||
            typeName.equalsIgnoreCase("MEDIUMBLOB") ||
            typeName.equalsIgnoreCase("LONGBLOB") ||
            typeName.equalsIgnoreCase("BLOB") ||
            typeName.equalsIgnoreCase("TINYTEXT") ||
            typeName.equalsIgnoreCase("MEDIUMTEXT") ||
            typeName.equalsIgnoreCase("LONGTEXT") ||
            typeName.equalsIgnoreCase("TEXT"))
        {
            // Some MySQL types don't allow precision specification i.e "LONG VARCHAR(...)" is illegal
            allowsPrecisionSpec = false;
        }
    }

    /**
     * Utility to check the compatibility of this type with the supplied Column type.
     * @param colInfo The Column type
     * @return Whether they are compatible
     **/
    public boolean isCompatibleWith(RDBMSColumnInfo colInfo)
    {
        if (super.isCompatibleWith(colInfo))
        {
            return true;
        }

        short colDataType = colInfo.getDataType();
        if (isStringType(dataType) && isStringType(colDataType))
        {
            return true;
        }

        if (dataType == Types.BIT)
        {
            // Columns created as BIT come back as TINYINT(1)
            int colSize = colInfo.getColumnSize();
            return colDataType == Types.TINYINT && colSize == 1;
        }

        if ((dataType == Types.BLOB && colDataType == Types.LONGVARBINARY) ||
            (dataType == Types.LONGVARBINARY && colDataType == Types.BLOB))
        {
            // Provide BLOB since MySQL JDBC is too lazy to.
            return true;
        }
        if ((dataType == Types.CLOB && colDataType == Types.LONGVARCHAR) ||
            (dataType == Types.LONGVARCHAR && colDataType == Types.CLOB))
        {
            // Provide CLOB since MySQL JDBC is too lazy to.
            return true;
        }

        return false;
    }


    /**
     * Tests whether or not the given JDBC type is a MySQL "string" type.
     * <p>
     * MySQL likes to interchange CHAR and VARCHAR at its own discretion, and
     * automatically upgrades types to bigger types if necessary.
     * In addition, we use the BINARY qualifier on CHAR and VARCHAR to get
     * case-sensitive treatment.
     * Taken together it means we really can't distinguish one string/binary
     * type from another so we treat them all as one big happy string family.
     */
    private static boolean isStringType(int type)
    {
        switch (type)
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return true;

            default:
                return false;
        }
    }
}