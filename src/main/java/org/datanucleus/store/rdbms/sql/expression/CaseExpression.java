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

import org.datanucleus.store.query.expression.Expression;

/**
 * Expression representing the SQL construct 
 * <pre>CASE WHEN expr1 THEN val1 WHEN expr2 THEN val2 ELSE valN END</pre>.
 */
public class CaseExpression extends SQLExpression
{
    SQLExpression[] whenExprs;
    SQLExpression[] actionExprs;
    SQLExpression elseExpr;

    public CaseExpression(SQLExpression[] whenExprs, SQLExpression[] actionExprs, SQLExpression elseExpr)
    {
        super(whenExprs[0].getSQLStatement(), null, null);

        this.whenExprs = whenExprs;
        this.actionExprs = actionExprs;
        this.elseExpr = elseExpr;

        st.clearStatement();
        st.append("CASE");
        if (actionExprs == null || whenExprs.length != actionExprs.length || whenExprs.length == 0)
        {
            throw new IllegalArgumentException("CaseExpression must have equal number of WHEN and THEN expressions");
        }

        mapping = null;
        for (int i=0;i<whenExprs.length;i++)
        {
            st.append(" WHEN ").append(whenExprs[i]).append(" THEN ").append(actionExprs[i]);
        }

        if (elseExpr != null)
        {
            st.append(" ELSE ").append(elseExpr);
        }
        st.append(" END");
        st.encloseInParentheses();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#eq(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            // Cater for using "IS NULL" rather than "= NULL"
            return expr.eq(this);
        }
        return new BooleanExpression(this, Expression.OP_EQ, expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#ne(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression ne(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            // Cater for using "IS NOT NULL" rather than "!= NULL"
            return expr.ne(this);
        }
        return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#ge(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression ge(SQLExpression expr)
    {
        return new BooleanExpression(this, Expression.OP_GTEQ, expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#gt(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression gt(SQLExpression expr)
    {
        return new BooleanExpression(this, Expression.OP_GT, expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#le(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression le(SQLExpression expr)
    {
        return new BooleanExpression(this, Expression.OP_LTEQ, expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#lt(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression lt(SQLExpression expr)
    {
        return new BooleanExpression(this, Expression.OP_LT, expr);
    }
}