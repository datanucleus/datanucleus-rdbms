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

import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of an expression for an unbound variable.
 * This is used where we have a variable in use in a query and at the point of needing it we
 * haven't yet bound the variable. For example, in the following query
 * <pre>this.names.contains(var) &amp;&amp; var == someValue</pre>
 * in the first clause the "var" is unbound when passing in to the InvokeExpression, so it is passed
 * in as an UnboundExpression, and in that contains() method will be bound to the collection (element)
 * table (a join added).
 */
public class UnboundExpression extends SQLExpression
{
    protected String variableName;

    /**
     * Constructor for an SQL expression for an unbound variable.
     * @param stmt The statement
     * @param variableName name of the variable
     */
    public UnboundExpression(SQLStatement stmt, String variableName)
    {
        super(stmt, null, null);
        this.variableName = variableName;
    }

    /**
     * Accessor for the variable name
     * @return Variable name that this represents
     */
    public String getVariableName()
    {
        return variableName;
    }
}