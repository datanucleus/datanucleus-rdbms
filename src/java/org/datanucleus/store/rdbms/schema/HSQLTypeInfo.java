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
 * SQL Type info for HSQLDB datastores.
 */
public class HSQLTypeInfo extends SQLTypeInfo
{
    /** The maximum precision we allow to be reported. */
    public static final int MAX_PRECISION = Integer.MAX_VALUE;

    /**
     * Constructs a type information object from the current row of the given result set.
     * @param rs The result set returned from DatabaseMetaData.getTypeInfo().
     */
    public HSQLTypeInfo(ResultSet rs)
    {
        super(rs);
        if (typeName.equalsIgnoreCase("varchar") || typeName.equalsIgnoreCase("char"))
        {
            /*
             * VARCHAR gets an arbitrary maximum precision assigned to it.
             * Requests fo anything larger than this will be converted to
             * LONGVARCHAR (i.e. TEXT) in StringMapping.
             */
            precision = MAX_PRECISION;
        }
        else if (typeName.equalsIgnoreCase("numeric"))
        {
            precision = MAX_PRECISION;
        }
        else if (typeName.equalsIgnoreCase("text"))
        {
            /* TEXT is equated to Types.LONGVARCHAR (as it should be). */
            dataType = (short)Types.LONGVARCHAR;
        }

        if (precision > MAX_PRECISION)
        {
            precision = MAX_PRECISION;
        }
    }

    public HSQLTypeInfo(String typeName, short dataType, int precision, String literalPrefix,
            String literalSuffix, String createParams, int nullable, boolean caseSensitive, short searchable,
            boolean unsignedAttribute, boolean fixedPrecScale, boolean autoIncrement, String localTypeName,
            short minimumScale, short maximumScale, int numPrecRadix)
    {
        super(typeName, dataType, precision, literalPrefix, literalSuffix, createParams, nullable, caseSensitive,
            searchable, unsignedAttribute, fixedPrecScale, autoIncrement, localTypeName, minimumScale, maximumScale,
            numPrecRadix);
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
        if ((dataType == Types.CLOB && colDataType == Types.LONGVARCHAR) ||
            (dataType == Types.LONGVARCHAR && colDataType == Types.CLOB))
        {
            // Provide CLOB since HSQL JDBC is too lazy to. They then often return it as opposite type
            return true;
        }
        if ((dataType == Types.BLOB && colDataType == Types.LONGVARBINARY) ||
            (dataType == Types.LONGVARBINARY && colDataType == Types.BLOB))
        {
            // Provide BLOB since HSQL JDBC is too lazy to. They then often return it as opposite type
            return true;
        }

        return false;
    }
}