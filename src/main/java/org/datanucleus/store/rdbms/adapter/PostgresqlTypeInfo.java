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
 * SQL Type info for PostgreSQL datastores.
 */
public class PostgresqlTypeInfo extends SQLTypeInfo
{
    /** The maximum precision we allow to be reported. */
    public static final int MAX_PRECISION = 65000;

    /**
     * Constructs a type information object from the current row of the given result set.
     * @param rs The result set returned from DatabaseMetaData.getTypeInfo().
     */
    public PostgresqlTypeInfo(ResultSet rs)
    {
        super(rs);
        /*
         * Not really sure what we should do about the precision field.  None of
         * the drivers that I've tried return a sensible value for it (the 7.2
         * driver returned -1 for all types, the 7.3 driver returned 9 (!) for
         * all types, both tried against a 7.2.1 server).  The best I can think
         * to do for now is make sure it has a decent value for those types
         * where I know JPOX cares, namely VARCHAR and NUMERIC.
         */
        if (typeName.equalsIgnoreCase("varchar") || typeName.equalsIgnoreCase("char"))
        {
            /*
             * VARCHAR gets an arbitrary maximum precision assigned to it.
             * Requests fo anything larger than this will be converted to
             * LONGVARCHAR (i.e. TEXT) in StringMapping.
             */
            precision = MAX_PRECISION;
            // Question : What should the upper limit on CHAR be really ? JDBC driver returns "9" !
        }
        else if (typeName.equalsIgnoreCase("numeric"))
        {
            precision = 64;    // pulled from thin air
        }
        else if (typeName.equalsIgnoreCase("text"))
        {
            // "text" is equated to Types.LONGVARCHAR
            dataType = (short)Types.LONGVARCHAR;
        }
        else if (typeName.equalsIgnoreCase("bytea"))
        {
            // "bytea" is now equated to Types.LONGVARBINARY
            dataType = (short)Types.LONGVARBINARY;
        }
        else if (typeName.equalsIgnoreCase("float8"))
        {
            allowsPrecisionSpec = false;
        }

        if (precision > MAX_PRECISION)
        {
            precision = MAX_PRECISION;
        }
    }

    public PostgresqlTypeInfo(String typeName, short dataType, int precision, String literalPrefix,
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
            // Provide CLOB since Postgresql JDBC is too lazy to.
            return true;
        }
        if ((dataType == Types.BLOB && colDataType == Types.LONGVARBINARY) ||
            (dataType == Types.LONGVARBINARY && colDataType == Types.BLOB))
        {
            // Provide BLOB since Postgresql JDBC is too lazy to.
            return true;
        }

        return false;
    }
}