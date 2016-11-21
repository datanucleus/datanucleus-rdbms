/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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
 * Type info for NuoDB.
 */
public class NuoDBTypeInfo extends SQLTypeInfo
{
    public NuoDBTypeInfo(String typeName, short dataType, int precision, String literalPrefix, String literalSuffix, String createParams,
            int nullable, boolean caseSensitive, short searchable, boolean unsignedAttribute, boolean fixedPrecScale,
            boolean autoIncrement, String localTypeName, short minimumScale, short maximumScale, int numPrecRadix)
    {
        super(typeName, dataType, precision, literalPrefix, literalSuffix, createParams, nullable, caseSensitive, searchable,
                unsignedAttribute, fixedPrecScale, autoIncrement, localTypeName, minimumScale, maximumScale, numPrecRadix);
    }

    public NuoDBTypeInfo(ResultSet rs)
    {
        super(rs);
    }
}