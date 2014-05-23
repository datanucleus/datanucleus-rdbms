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

import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.AggregateNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.NumericSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;
import org.datanucleus.util.Localiser;

/**
 * Expression handler to invoke an SQL aggregated function.
 * <ul>
 * <li>The expression should be null and will return an AggregateExpression
 *     <pre>{functionName}({argExpr})</pre> when processing a result clause</li>
 * <li>If the compilation component is something else then will generate a subquery expression</li>
 * </ul>
 */
public abstract class SimpleNumericAggregateMethod extends AbstractSQLMethod
{
    protected abstract String getFunctionName();

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (expr != null)
        {
            throw new NucleusException(Localiser.msg("060002", getFunctionName(), expr));
        }
        if (args == null || args.size() != 1)
        {
            throw new NucleusException(getFunctionName() + " is only supported with a single argument");
        }

        if (stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.RESULT ||
            stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.HAVING)
        {
            // FUNC(argExpr)
            // Use same java type as the argument
            SQLExpression argExpr = args.get(0);
            JavaTypeMapping m = argExpr.getJavaTypeMapping();
            return new AggregateNumericExpression(stmt, m, getFunctionName(), args);
        }
        else
        {
            // Handle as Subquery "SELECT AVG(expr) FROM tbl"
            SQLExpression argExpr = args.get(0);
            SQLStatement subStmt = new SQLStatement(stmt, stmt.getRDBMSManager(),
                argExpr.getSQLTable().getTable(), argExpr.getSQLTable().getAlias(), null);
            subStmt.setClassLoaderResolver(clr);

            JavaTypeMapping mapping =
                stmt.getRDBMSManager().getMappingManager().getMappingWithDatastoreMapping(String.class, false, false, clr);
            String aggregateString = getFunctionName() + "(" + argExpr.toSQLText() + ")";
            SQLExpression aggExpr = exprFactory.newLiteral(subStmt, mapping, aggregateString);
            ((StringLiteral)aggExpr).generateStatementWithoutQuotes();
            subStmt.select(aggExpr, null);

            JavaTypeMapping subqMapping = exprFactory.getMappingForType(Integer.class, false);
            SQLExpression subqExpr = new NumericSubqueryExpression(stmt, subStmt);
            subqExpr.setJavaTypeMapping(subqMapping);
            return subqExpr;
        }
    }

    protected abstract Class getClassForMapping();
}