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
 * SQL Type info for H2 datastores.
 */
public class H2TypeInfo extends SQLTypeInfo
{
    /**
     * Constructs a type information object from the current row of the given result set.
     * @param rs The result set returned from DatabaseMetaData.getTypeInfo().
     */
    public H2TypeInfo(ResultSet rs)
    {
        super(rs);
    }

    /**
     * Utility to check the compatibility of this type with the supplied Column
     * type.
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
        if ((dataType == Types.CHAR && colDataType == Types.VARCHAR) ||
            (dataType == Types.VARCHAR && colDataType == Types.CHAR))
        {
            // H2 treats CHAR and VARCHAR the same. At least up to and including Feb 2007
            return true;
        }

        return false;
    }
}