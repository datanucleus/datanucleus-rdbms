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
package org.datanucleus.store.rdbms.sql.operation;

import java.util.ArrayList;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;

/**
 * Implementation of CONCAT, using SQL CONCAT function.
 * Results in <pre>CONCAT(expr1, expr2)</pre>
 */
public class Concat2Operation implements SQLOperation
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.operation.SQLOperation#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    public SQLExpression getExpression(SQLExpression expr, SQLExpression expr2)
    {
        RDBMSStoreManager storeMgr = expr.getSQLStatement().getRDBMSManager();
        JavaTypeMapping m = storeMgr.getSQLExpressionFactory().getMappingForType(String.class, false);
        ArrayList args = new ArrayList();
        args.add(expr);
        args.add(expr2);
        return new StringExpression(expr.getSQLStatement(), m, "CONCAT", args);
    }
}