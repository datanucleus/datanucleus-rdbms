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

import java.util.List;
import java.util.Map;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * An expression that represents some Map field in a query candidate
 * class, or a Map field in an object linked from the candidate class
 * by navigation.
 * <p>
 * When navigated through using containsKey(expr) the keys of the Map are
 * relationally joined onto the query statement. When navigated through using
 * containsValue(expr) the values of the Map are relationally joined onto the
 * query statement. These 2 methods are required for JDO 2.0, whilst the
 * isEmpty() and contains() are JDO 1.0.1. containsEntry() is an extension.
 * </p>
 */
public class MapExpression extends SQLExpression
{
    /**
     * Constructor.
     * @param stmt The SQL Statement
     * @param table Table containing the map field
     * @param mapping The java field mapping
     **/
    public MapExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    public SQLExpression invoke(String methodName, List args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, Map.class.getName(), 
            methodName, this, args);
    }

    /**
     * Method to return the expression for comparing a map with a value.
     * Only supports comparisons with null currently.
     * @param expr The value to compare with.
     * @return The expression of equality
     */
    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return (BooleanExpression)invoke("isEmpty", null);
        }
        return super.eq(expr);
    }

    /**
     * Method to return the expression for comparing a map with a value.
     * Only supports comparisons with null currently.
     * @param expr The value to compare with.
     * @return The expression of inequality
     */
    public BooleanExpression ne(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return invoke("isEmpty", null).not();
        }
        return super.ne(expr);
    }
}