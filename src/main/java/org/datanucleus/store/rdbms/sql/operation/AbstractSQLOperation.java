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
package org.datanucleus.store.rdbms.sql.operation;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;

/**
 * Abstract representation for SQLOperations.
 */
public abstract class AbstractSQLOperation implements SQLOperation
{
    protected SQLExpressionFactory exprFactory;

    public void setExpressionFactory(SQLExpressionFactory exprFactory)
    {
        this.exprFactory = exprFactory;
    }

    /**
     * Accessor for the mapping for a particular class.
     * For use by subclasses to generate the mapping for use in the returned SQLExpression.
     * @param cls The class we want a mapping to represent
     * @return The mapping
     */
    protected JavaTypeMapping getMappingForClass(Class cls)
    {
        return exprFactory.getMappingForType(cls, true);
    }
}