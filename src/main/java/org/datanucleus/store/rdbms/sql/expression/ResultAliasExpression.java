/**********************************************************************
Copyright (c) 2015 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Expression for use in an ordering clause representing a result alias.
 * JPQL allows specification of a result clause with alias, and to be able to order by exactly that alias.
 */
public class ResultAliasExpression extends SQLExpression
{
    protected String aliasName;

    public ResultAliasExpression(SQLStatement stmt, String aliasName)
    {
        super(stmt, null, null);
        this.aliasName = aliasName;
    }

    public String getResultAlias()
    {
        return aliasName;
    }
}
