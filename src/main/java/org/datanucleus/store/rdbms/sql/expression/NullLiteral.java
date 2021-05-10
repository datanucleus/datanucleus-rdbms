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
package org.datanucleus.store.rdbms.sql.expression;

import org.datanucleus.store.query.compiler.CompilationComponent;
import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.NullMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of a Null literal in a Query.
 */
public class NullLiteral extends SQLExpression implements SQLLiteral
{
    /**
     * Constructor for a null literal with a value (i.e null!).
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value Null!
     * @param parameterName Name of the parameter that this represents if any (JDBC "?") NOT USED
     */
    public NullLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, new NullMapping(stmt.getRDBMSManager()));
        st.append("NULL");
    }

    public Object getValue()
    {
        return null;
    }

    public SQLExpression add(SQLExpression expr)
    {
        return this;
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false), true);
        }
        else if (expr instanceof ObjectExpression)
        {
            return expr.eq(this);
        }
        if (stmt.getQueryGenerator() != null && stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.UPDATE)
        {
            // Special case : UPDATE clause needs "x = NULL"
            return new BooleanExpression(expr, Expression.OP_EQ, this);
        }

        return new BooleanExpression(expr, Expression.OP_IS, this);
    }

    public BooleanExpression ne(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false), false);
        }
        else if (expr instanceof ObjectExpression)
        {
            return expr.ne(this);
        }

        return new BooleanExpression(expr, Expression.OP_ISNOT, this);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#setNotParameter()
     */
    public void setNotParameter()
    {
        if (parameterName == null)
        {
            return;
        }
        parameterName = null;
    }
}