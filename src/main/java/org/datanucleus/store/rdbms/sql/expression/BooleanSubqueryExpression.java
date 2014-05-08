/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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
 * Boolean expression to wrap a subquery.
 * The subquery will be a "SELECT 1 FROM ..." resulting in SQL like
 * <pre>{keyword} (SELECT 1 FROM ...)</pre>
 * where the keyword is supplied (e.g EXISTS).
 * e.g <pre>EXISTS (SELECT 1 FROM TBL WHERE TBL.ID = A0.ID)</pre>
 */
public class BooleanSubqueryExpression extends BooleanExpression
{
    public BooleanSubqueryExpression(SQLStatement stmt, String keyword, SQLStatement subStmt)
    {
        super(stmt, null);

        // SQL for this expression should be the subquery, within brackets (for clarity)
        st.append(keyword).append(" (");
        st.append(subStmt);
        st.append(")");
    }
}