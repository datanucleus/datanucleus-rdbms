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

import java.util.List;

import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * String expression to wrap a subquery.
 * The subquery SQLStatement should return a String value.
 */
public class StringSubqueryExpression extends StringExpression implements SubqueryExpressionComponent
{
    SQLStatement subStatement;

    public StringSubqueryExpression(SQLStatement stmt, SQLStatement subStmt)
    {
        super(stmt, null, null);
        this.subStatement = subStmt;

        // SQL for this expression should be the subquery, within brackets (for clarity)
        st.append("(");
        st.append(subStmt);
        st.append(")");
    }

    public SQLStatement getSubqueryStatement()
    {
        return subStatement;
    }

    public void setKeyword(String keyword)
    {
        st.clearStatement();
        st.append(keyword).append(" (").append(subStatement).append(")");
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        BooleanExpression eqExpr = super.eq(expr);
        eqExpr.encloseInParentheses();
        return eqExpr;
    }

    public BooleanExpression ne(SQLExpression expr)
    {
        BooleanExpression eqExpr = super.ne(expr);
        eqExpr.encloseInParentheses();
        return eqExpr;
    }

    public BooleanExpression lt(SQLExpression expr)
    {
        BooleanExpression eqExpr = super.lt(expr);
        eqExpr.encloseInParentheses();
        return eqExpr;
    }
    
    public BooleanExpression le(SQLExpression expr)
    {
        BooleanExpression eqExpr = super.le(expr);
        eqExpr.encloseInParentheses();
        return eqExpr;
    }

    public BooleanExpression gt(SQLExpression expr)
    {
        BooleanExpression eqExpr = super.gt(expr);
        eqExpr.encloseInParentheses();
        return eqExpr;
    }

    public BooleanExpression ge(SQLExpression expr)
    {
        BooleanExpression eqExpr = super.ge(expr);
        eqExpr.encloseInParentheses();
        return eqExpr;
    }

    @Override
    public SQLExpression invoke(String methodName, List<SQLExpression> args)
    {
        if (methodName.equals("contains"))
        {
            return new BooleanExpression(args.get(0), Expression.OP_IN, this);
        }
        return super.invoke(methodName, args);
    }
}