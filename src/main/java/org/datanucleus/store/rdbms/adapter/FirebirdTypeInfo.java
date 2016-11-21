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

import org.datanucleus.store.rdbms.schema.SQLTypeInfo;

/**
 * SQL Type info for Firebird datastores.
 */
public class FirebirdTypeInfo extends SQLTypeInfo
{
    /** The maximum precision we allow for DECIMAL. */
    public static final int MAX_PRECISION_DECIMAL = 18;

    /**
     * Constructs a type information object from the current row of the given result set.
     * @param rs The result set returned from DatabaseMetaData.getTypeInfo().
     */
    public FirebirdTypeInfo(ResultSet rs)
    {
        super(rs);
        if (typeName.equalsIgnoreCase("decimal"))
        {
            precision = MAX_PRECISION_DECIMAL;
        }
    }
}