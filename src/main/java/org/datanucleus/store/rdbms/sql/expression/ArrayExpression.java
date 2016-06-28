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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of array expression.
 */
public class ArrayExpression extends SQLExpression
{
    /** Expressions for all elements in the array. **/ 
    protected List<SQLExpression> elementExpressions;

    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    public ArrayExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Constructor for an SQL expression for an array expression specified in the query.
     * @param stmt The statement
     * @param mapping The mapping for the field
     * @param exprs element expressions
     */
    public ArrayExpression(SQLStatement stmt, JavaTypeMapping mapping, SQLExpression[] exprs)
    {
        super(stmt, null, mapping);
        elementExpressions = new ArrayList<>();
        for (int i=0;i<exprs.length;i++)
        {
            elementExpressions.add(exprs[i]);
        }
    }

    public List<SQLExpression> getElementExpressions()
    {
        return elementExpressions;
    }

    public SQLExpression invoke(String methodName, List args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, "ARRAY",
            methodName, this, args);
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            // Support comparison with null
            return expr.eq(this);
        }
        return super.eq(expr);
    }

    public BooleanExpression ne(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            // Support comparison with null
            return expr.ne(this);
        }
        return super.ne(expr);
    }
}