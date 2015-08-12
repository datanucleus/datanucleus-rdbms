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
package org.datanucleus.store.rdbms.query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLLiteral;
import org.datanucleus.store.rdbms.sql.expression.UnboundExpression;

/**
 * Interface for a generator of query statements.
 */
public interface QueryGenerator
{
    /**
     * Accessor for the query language that this query pertains to.
     * Can be used to decide how to handle the input.
     * @return The query language
     */
    public String getQueryLanguage();

    /**
     * Accessor for the ClassLoader resolver to use when looking up classes.
     * @return The classloader resolver
     */
    public ClassLoaderResolver getClassLoaderResolver();

    /**
     * Accessor for the ExecutionContext for this query.
     * @return ExecutionContext
     */
    public ExecutionContext getExecutionContext();

    /**
     * Accessor for the current query component being compiled.
     * @return Component being compiled (if any)
     */
    public CompilationComponent getCompilationComponent();

    /**
     * Accessor for a property affecting the query compilation.
     * This can be something like whether there is an OR in the filter, which can then impact
     * on the type of SQL used.
     * @param name The property name
     * @return Its value
     */
    public Object getProperty(String name);

    /**
     * Method to instruct the generator to convert the provided parameter expression to just be
     * a literal using the value of the parameter (hence the statement cannot be precompilable since
     * the value needs to be known).
     * @param paramLiteral The parameter expression
     */
    public void useParameterExpressionAsLiteral(SQLLiteral paramLiteral);

    /**
     * Accessor for the type of a variable if already known (declared?).
     * @param varName Name of the variable
     * @return The type if it is known
     */
    public Class getTypeOfVariable(String varName);

    /**
     * Method to bind the specified variable to the table and mapping.
     * @param varName Variable name
     * @param cmd Metadata for this variable type
     * @param sqlTbl Table for this variable
     * @param mapping The mapping of this variable in the table
     */
    public void bindVariable(String varName, AbstractClassMetaData cmd, SQLTable sqlTbl, JavaTypeMapping mapping);

    /**
     * Method to bind the specified unbound variable (as cross join).
     * @param expr Unbound expression
     * @param type The type to bind as
     * @return The bound expression to use instead
     */
    public SQLExpression bindVariable(UnboundExpression expr, Class type);

    /**
     * Accessor for whether the query has explicit joins. A JPQL query has explicit joins, whereas a
     * JDOQL query has variables and hence implicit joins.
     * If not then has implicit joins, meaning that they could potentially be rebound later
     * if prematurely bound in a particular way.
     * @return Whether the query has explicit joins
     */
    public boolean hasExplicitJoins();

    public boolean processingOnClause();

    /**
     * Method to bind the specified parameter to the defined type.
     * If the parameter is already bound (declared in the query perhaps, or bound via an earlier usage) then 
     * does nothing.
     * @param paramName Name of the parameter
     * @param type The type (or subclass)
     */
    public void bindParameter(String paramName, Class type);

    /**
     * Convenience method to resolve a class name.
     * @param className The class name
     * @return The class it relates to (if found)
     */
    public Class resolveClass(String className);

    /**
     * Accessor for whether the query being generated has the specified extension.
     * @param key Extension name
     * @return Whether it is present
     */
    public boolean hasExtension(String key);

    /**
     * Accessor for the value of the specified query extension (or null if not defined).
     * @param key Extension name
     * @return The value for the extension
     */
    public Object getValueForExtension(String key);
}