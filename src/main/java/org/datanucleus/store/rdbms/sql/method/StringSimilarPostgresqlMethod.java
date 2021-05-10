/**********************************************************************
Copyright (c) 2011 KC Berg and others. All rights reserved.
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

import org.datanucleus.store.query.expression.Expression.DyadicOperator;
import org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;

/**
 * Support for a String.matches like functionality but using Postgresql's "SIMILAR TO" operator.
 * This is mapped to the Java function "String.similarTo" as an extension so it is available for use.
 * A unit test fails if trying to use it as String.matches.
 */
public class StringSimilarPostgresqlMethod extends StringMatchesMethod 
{
    public static final DyadicOperator OP_SIMILAR_TO = new DyadicOperator("SIMILAR TO", 3, false);
    
    protected BooleanExpression getExpressionForStringExpressionInput(SQLStatement stmt, SQLExpression expr, SQLExpression regExpr, SQLExpression escapeExpr)
    {
        return getBooleanLikeExpression(stmt, expr, regExpr, escapeExpr);
    }

    protected BooleanExpression getBooleanLikeExpression(SQLStatement stmt, SQLExpression expr, SQLExpression regExpr, SQLExpression escapeExpr)
    {
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        BooleanExpression similarToExpr = new BooleanExpression(stmt, exprFactory.getMappingForType(boolean.class, false));
        SQLText sql = similarToExpr.toSQLText();
        sql.clearStatement();
        if (OP_SIMILAR_TO.isHigherThanLeftSide(expr.getLowestOperator()))
        {
            sql.append("(").append(expr).append(")");
        }
        else
        {
            sql.append(expr);
        }

        sql.append(" SIMILAR TO ");

        if (OP_SIMILAR_TO.isHigherThanRightSide(regExpr.getLowestOperator()))
        {
            sql.append("(").append(regExpr).append(")");
        }
        else
        {
            sql.append(regExpr);
        }

        BaseDatastoreAdapter dba = (BaseDatastoreAdapter) stmt.getRDBMSManager().getDatastoreAdapter();
        if (escapeExpr != null)
        {
            if (escapeExpr instanceof CharacterLiteral)
            {
                String chr = "" + ((CharacterLiteral)escapeExpr).getValue();
                if (chr.equals(dba.getEscapeCharacter()))
                {
                    // If the escape character specified matches the Java character then apply the known working ESCAPE
                    // This is because some datastore JDBC drivers require additional "\" characters to allow
                    // for Java usage
                    sql.append(dba.getEscapePatternExpression());
                }
                else
                {
                    sql.append(" ESCAPE " + escapeExpr);
                }
            }
            else
            {
                sql.append(" ESCAPE " + escapeExpr);
            }
        }
        else
        {
            sql.append(" " + dba.getEscapePatternExpression());
        }

        return similarToExpr;
    }
}