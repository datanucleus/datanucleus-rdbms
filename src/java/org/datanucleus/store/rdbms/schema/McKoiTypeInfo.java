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

/**
 * SQL Type info for McKoi datastores.
 */
public class McKoiTypeInfo extends SQLTypeInfo
{
    /** The maximum precision we allow to be reported. McKoi supports up to 1 billion characters */
    public static final int MAX_PRECISION = Integer.MAX_VALUE;

    /**
     * Constructs a type information object from the current row of the given result set.
     * @param rs The result set returned from DatabaseMetaData.getTypeInfo().
     */
    public McKoiTypeInfo(ResultSet rs)
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
        if (precision > MAX_PRECISION)
        {
            precision = MAX_PRECISION;
        }
    }
}