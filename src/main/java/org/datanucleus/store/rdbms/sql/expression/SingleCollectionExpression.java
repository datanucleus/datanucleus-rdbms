/**********************************************************************
Copyright (c) 2015 Renato Garcia and others. All rights reserved.
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
package org.datanucleus.store.rdbms.sql.expression;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SingleCollectionMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Cover-all expression to represent "single collection" mapped types.
 * Note that we should split this up if we ever want to have methods on particular types.
 * For example we could have OptionalExpression and provide methods "isPresent" and "get".
 */
public class SingleCollectionExpression extends DelegatedExpression
{
    protected SQLExpression[] wrappedExpressions;

    public SingleCollectionExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
        SingleCollectionMapping wcm = (SingleCollectionMapping) mapping;

        JavaTypeMapping wrappedMapping = wcm.getWrappedMapping();
        if (wrappedMapping != null)
        {
            delegate = stmt.getSQLExpressionFactory().newExpression(stmt, table, wrappedMapping);
        }
    }
}
