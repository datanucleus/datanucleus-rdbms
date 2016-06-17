/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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

import java.util.Iterator;
import java.util.List;

/**
 * Expression representing the SQL construct "IN (expr1, expr2, ...)".
 */
public class InExpression extends BooleanExpression
{
    boolean negated = false;
    SQLExpression expr;
    SQLExpression[] exprs;

    /**
     * Constructor for an IN expression.
     * @param expr The expression that is contained.
     * @param exprs The expressions that it is contained in
     */
    public InExpression(SQLExpression expr, SQLExpression[] exprs)
    {
        super(expr.getSQLStatement(), expr.getSQLStatement().getSQLExpressionFactory().getMappingForType(boolean.class, false));

        this.expr = expr;
        this.exprs = exprs;
        setStatement();
    }

    /**
     * Constructor for an IN expression.
     * @param expr The expression that is contained.
     * @param exprList List of expressions that it is contained in
     */
    public InExpression(SQLExpression expr, List<SQLExpression> exprList)
    {
        super(expr.getSQLStatement(), expr.getSQLStatement().getSQLExpressionFactory().getMappingForType(boolean.class, false));

        this.expr = expr;
        this.exprs = new SQLExpression[exprList.size()];
        Iterator<SQLExpression> exprListIter = exprList.iterator();
        int i = 0;
        while (exprListIter.hasNext())
        {
            this.exprs[i++] = exprListIter.next();
        }
        setStatement();
    }

    public BooleanExpression not()
    {
        negated = !negated;
        setStatement();
        return this;
    }

    protected void setStatement()
    {
        st.clearStatement();
        st.append(expr);
        if (negated)
        {
            st.append(" NOT");
        }
        st.append(" IN(");
        for (int i=0;i<exprs.length;i++)
        {
            if (i > 0)
            {
                st.append(",");
            }
            st.append(exprs[i]);
        }
        st.append(")");
    }
}