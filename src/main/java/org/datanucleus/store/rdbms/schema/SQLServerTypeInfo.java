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
package org.datanucleus.store.rdbms.schema;

import java.sql.ResultSet;
import java.sql.Types;

/**
 * SQL Type info for SQLServer datastores.
 */
public class SQLServerTypeInfo extends SQLTypeInfo
{
    /** sql type NVARCHAR **/
    public static final int NVARCHAR = -9;
    /** sql type NTEXT **/
    public static final int NTEXT = -10;
    /** sql type UNIQUEIDENTIFIER **/
    public static final int UNIQUEIDENTIFIER = -11;

    /**
     * Constructs a type information object from the current row of the given result set.
     * @param rs The result set returned from DatabaseMetaData.getTypeInfo().
     */
    public SQLServerTypeInfo(ResultSet rs)
    {
        super(rs);

        // unique identifiers does not allow any precision
        if (typeName.equalsIgnoreCase("uniqueidentifier"))
        {
            allowsPrecisionSpec = false;
        }
    }

    public SQLServerTypeInfo(String typeName, short dataType, int precision, String literalPrefix,
            String literalSuffix, String createParams, int nullable, boolean caseSensitive, short searchable,
            boolean unsignedAttribute, boolean fixedPrecScale, boolean autoIncrement, String localTypeName,
            short minimumScale, short maximumScale, int numPrecRadix)
    {
        super(typeName, dataType, precision, literalPrefix, literalSuffix, createParams, nullable, caseSensitive,
            searchable, unsignedAttribute, fixedPrecScale, autoIncrement, localTypeName, minimumScale, maximumScale,
            numPrecRadix);
    }

    public boolean isCompatibleWith(RDBMSColumnInfo colInfo)
    {
        if (super.isCompatibleWith(colInfo))
            return true;

        short colDataType = colInfo.getDataType();
        switch (dataType)
        {
            case Types.VARCHAR:
                return colDataType == NVARCHAR;
            case Types.LONGVARCHAR:
                return colDataType == NTEXT;
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case UNIQUEIDENTIFIER:
                return (colDataType == Types.VARBINARY) || 
                    (colDataType == Types.LONGVARBINARY) || 
                    (colDataType == UNIQUEIDENTIFIER);
            default:
                return false;
        }
    }
}