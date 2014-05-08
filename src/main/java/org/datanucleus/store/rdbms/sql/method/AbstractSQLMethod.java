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
package org.datanucleus.store.rdbms.sql.method;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.util.Localiser;

/**
 * Abstract implementation of an SQLMethod. Provides localisation and convenience methods where required.
 */
public abstract class AbstractSQLMethod implements SQLMethod
{
    /** Localiser for messages */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    protected SQLStatement stmt;
    protected SQLExpressionFactory exprFactory;
    protected ClassLoaderResolver clr;

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#setStatement(org.datanucleus.store.rdbms.sql.SQLStatement)
     */
    public void setStatement(SQLStatement stmt)
    {
        this.stmt = stmt;
        this.exprFactory = stmt.getSQLExpressionFactory();
        if (stmt.getQueryGenerator() == null)
        {
            throw new NucleusException("Attempt to use SQLMethod with an SQLStatement which doesn't have a QueryGenerator assigned");
        }
        this.clr = stmt.getQueryGenerator().getClassLoaderResolver();
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