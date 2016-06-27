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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * SQL Expression for creating a new object (in the result clause of a query).
 */
public class NewObjectExpression extends SQLExpression
{
    /** The class that we need to construct an instance of. */
    Class newClass = null;

    /** The argument expressions to use in the constructor. */
    List<SQLExpression> ctrArgExprs = null;

    /**
     * @param stmt SQLStatement that this is part of
     * @param cls Class that we create an instance of
     * @param args argument SQL expressions
     */
    public NewObjectExpression(SQLStatement stmt, Class cls, List<SQLExpression> args)
    {
        super(stmt, null, null);

        this.newClass = cls;
        if (args != null)
        {
            ctrArgExprs = new ArrayList<>();
            ctrArgExprs.addAll(args);
        }
    }

    public Class getNewClass()
    {
        return newClass;
    }

    public List<SQLExpression> getConstructorArgExpressions()
    {
        return ctrArgExprs;
    }
}