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
package org.datanucleus.store.rdbms.sql.method;

import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.AggregateNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.NumericSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Expression handler to invoke the SQL AVG aggregation function.
 * For use in evaluating AVG({expr}) where the RDBMS supports this function.
 * Returns a NumericExpression "AVG({numericExpr})".
 */
public class AvgFunction extends SimpleNumericAggregateMethod
{
    protected String getFunctionName()
    {
        return "AVG";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List args)
    {
        if (expr != null)
        {
            throw new NucleusException(LOCALISER.msg("060002", getFunctionName(), expr));
        }
        if (args == null || args.size() != 1)
        {
            throw new NucleusException(getFunctionName() + " is only supported with a single argument");
        }

        // Set the return type (double, for JDOQL and JPQL)
        Class returnType = Double.class;
        
        if (stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.RESULT ||
            stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.HAVING)
        {
            // FUNC(argExpr)
            JavaTypeMapping m = getMappingForClass(returnType);

             return getAggregateExpression(args, m);
        }
        else
        {
            // Handle as Subquery "SELECT AVG(expr) FROM tbl"
            SQLExpression argExpr = (SQLExpression)args.get(0);
            SQLStatement subStmt = new SQLStatement(stmt, stmt.getRDBMSManager(),
                argExpr.getSQLTable().getTable(), argExpr.getSQLTable().getAlias(), null);
            subStmt.setClassLoaderResolver(clr);

            JavaTypeMapping mapping =
                stmt.getRDBMSManager().getMappingManager().getMappingWithDatastoreMapping(String.class, false, false, clr);
            SQLExpression aggExpr = getAggregateExpression(args, mapping);
            subStmt.select(aggExpr, null);

            JavaTypeMapping subqMapping = exprFactory.getMappingForType(returnType, false);
            SQLExpression subqExpr = new NumericSubqueryExpression(stmt, subStmt);
            subqExpr.setJavaTypeMapping(subqMapping);
            return subqExpr;
        }
    }

    protected SQLExpression getAggregateExpression(List args, JavaTypeMapping m)
    {
        return new AggregateNumericExpression(stmt, m, getFunctionName(), args);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SimpleAggregateMethod#getClassForMapping()
     */
    @Override
    protected Class getClassForMapping()
    {
        return double.class;
    }
}
