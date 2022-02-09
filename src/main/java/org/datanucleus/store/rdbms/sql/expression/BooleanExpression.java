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

import org.datanucleus.store.query.compiler.CompilationComponent;
import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of a Boolean expression in a Query.
 * Can be represented in one of the following ways in the datastore
 * <ul>
 * <li>As String-based, so stored as "Y", "N"</li>
 * <li>As numeric-based, so stored as 1, 0</li>
 * <li>As boolean-based, so stored as true, false</li>
 * </ul>
 * A boolean expression has "closure" if it represents a boolean SQL expression (i.e "fld == val").
 * If it doesn't have "closure" then it represents a clause like "booleanFld" or "false" or "true".
 */
public class BooleanExpression extends SQLExpression
{
    boolean hasClosure = false;

    /**
     * Constructor for a boolean expression for the specified mapping using the specified SQL text.
     * @param stmt The statement
     * @param mapping the mapping associated to this expression
     * @param sql The SQL text that will return a boolean
     */
    public BooleanExpression(SQLStatement stmt, JavaTypeMapping mapping, String sql)
    {
        super(stmt, null, mapping);

        st.clearStatement();
        st.append(sql);
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [,argN])" such as STARTS_WITH(arg1, arg2).
     * @param stmt The statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     */
    public BooleanExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args)
    {
        super(stmt, mapping, functionName, args, null);
        hasClosure = true;
    }

    /**
     * Constructor for a boolean expression for the specified mapping of the table.
     * The boolean expression DOESN'T have closure using this constructor.
     * @param stmt The statement
     * @param table The table this mapping belongs to
     * @param mapping the mapping associated to this expression
     */    
    public BooleanExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Constructor for a boolean expression for the specified mapping of the table.
     * The boolean expression has closure using this constructor.
     * @param stmt The statement
     * @param mapping the mapping associated to this expression
     */    
    public BooleanExpression(SQLStatement stmt, JavaTypeMapping mapping)
    {
        super(stmt, null, mapping);
        hasClosure = true;
    }

    /**
     * Perform an operation <pre>op</pre> on expression <pre>expr1</pre>.
     * The boolean expression has closure using this constructor.
     * @param op operator
     * @param expr1 operand
     */
    public BooleanExpression(Expression.MonadicOperator op, SQLExpression expr1)
    {
        super(op, expr1);
        hasClosure = true;
    }

    /**
     * Perform an operation <pre>op</pre> between <pre>expr1</pre> and <pre>expr2</pre>.
     * The boolean expression has closure using this constructor.
     * @param expr1 the first expression
     * @param op the operator between operands
     * @param expr2 the second expression
     */
    public BooleanExpression(SQLExpression expr1, Expression.DyadicOperator op, SQLExpression expr2)
    {
        super(expr1, op, expr2);

        // Cater for some specific situations where we definitely want a boolean mapping and maybe the superclass didn't create the right one
        if (op == Expression.OP_EQ || op == Expression.OP_GT || op == Expression.OP_GTEQ ||
             op == Expression.OP_NOTEQ || op == Expression.OP_LT || op == Expression.OP_LTEQ)
        {
            mapping = stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false);
        }
        else if (op == Expression.OP_IS || op == Expression.OP_ISNOT)
        {
            if (expr1 instanceof NullLiteral || expr2 instanceof NullLiteral)
            {
                // Comparison with null
                mapping = stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false);
            }
        }
        hasClosure = true;
    }

    public boolean hasClosure()
    {
        return hasClosure;
    }

    public BooleanExpression and(SQLExpression expr)
    {
        if (expr instanceof BooleanLiteral)
        {
            return expr.and(this);
        }
        else if (expr instanceof BooleanExpression)
        {
            // Enforce closure. Maybe ought to refer to DatastoreAdapter.BOOLEAN_COMPARISON
            BooleanExpression left = this;
            BooleanExpression right = (BooleanExpression) expr;
            if (!left.hasClosure())
            {
                left = left.eq(new BooleanLiteral(stmt, mapping, Boolean.TRUE));
            }
            if (!right.hasClosure())
            {
                right = right.eq(new BooleanLiteral(stmt, mapping, Boolean.TRUE));
            }

            if (stmt.getQueryGenerator() != null && stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.UPDATE)
            {
                // Special case : UPDATE clause AND should be replaced by boolean expression with "," separating the clauses
                BooleanExpression boolExpr = new BooleanExpression(stmt, null, mapping);
                boolExpr.st.append(left);
                boolExpr.st.append(',');
                boolExpr.st.append(right);
                return boolExpr;
            }
            return new BooleanExpression(left, Expression.OP_AND, right);
        }
        else
        {
            return super.and(expr);
        }
    }

    public BooleanExpression eor(SQLExpression expr)
    {
        if (expr instanceof BooleanLiteral)
        {
            return expr.eor(this);
        }
        else if (expr instanceof BooleanExpression)
        {
            if (stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BOOLEAN_COMPARISON))
            {
                return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
            }

            return and(expr.not()).ior(not().and(expr));
        }
        else
        {
            return super.eor(expr);
        }
    }

    public BooleanExpression ior(SQLExpression expr)
    {
        if (expr instanceof BooleanLiteral)
        {
            return expr.ior(this);
        }
        else if (expr instanceof BooleanExpression)
        {
            // Enforce closure. Maybe ought to refer to DatastoreAdapter.BOOLEAN_COMPARISON
            BooleanExpression left = this;
            BooleanExpression right = (BooleanExpression) expr;
            if (!left.hasClosure())
            {
                left = left.eq(new BooleanLiteral(stmt, mapping, Boolean.TRUE));
            }
            if (!right.hasClosure())
            {
                right = right.eq(new BooleanLiteral(stmt, mapping, Boolean.TRUE));
            }
            return new BooleanExpression(left, Expression.OP_OR, right);
        }
        else
        {
            return super.ior(expr);
        }
    }

    public BooleanExpression not()
    {
        if (!hasClosure)
        {
            return new BooleanExpression(this, Expression.OP_EQ, new BooleanLiteral(stmt, mapping, Boolean.FALSE, null));
        }

        return new BooleanExpression(Expression.OP_NOT, this);
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof BooleanLiteral || expr instanceof NullLiteral)
        {
            return expr.eq(this);
        }
        else if (expr instanceof BooleanExpression)
        {
            ColumnMapping colMapping = mapping.getColumnMapping(0);
            if (colMapping.isStringBased())
            {
                // Persisted using "Y", "N"
                return new BooleanExpression(new CharacterExpression(stmt, table, mapping),
                    Expression.OP_EQ,
                    new CharacterExpression(stmt, expr.table, expr.mapping));
            }
            else if (colMapping.isIntegerBased() || (colMapping.isBitBased() &&
                    !stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BIT_IS_REALLY_BOOLEAN)))
            {
                // Persisted using "1", "0"
                return new BooleanExpression(new NumericExpression(stmt, table, mapping), 
                    Expression.OP_EQ,
                    new NumericExpression(stmt, expr.table, expr.mapping));
            }
            else if (stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BOOLEAN_COMPARISON))
            {
                return new BooleanExpression(this, Expression.OP_EQ, expr);
            }
            else
            {
                return and(expr).ior(not().and(expr.not()));
            }
        }
        else
        {
            return super.eq(expr);
        }
    }

    public BooleanExpression ne(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof BooleanLiteral || expr instanceof NullLiteral)
        {
            return expr.ne(this);
        }
        else if (expr instanceof BooleanExpression)
        {
            ColumnMapping colMapping = mapping.getColumnMapping(0);
            if (colMapping.isStringBased())
            {
                // Persisted using "Y", "N"
                return new BooleanExpression(new CharacterExpression(stmt, table, mapping), 
                    Expression.OP_NOTEQ,
                    new CharacterExpression(stmt, expr.table, expr.mapping));
            }
            else if (colMapping.isIntegerBased() || (colMapping.isBitBased() &&
                    !stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BIT_IS_REALLY_BOOLEAN)))
            {
                // Persisted using "1", "0"
                return new BooleanExpression(new NumericExpression(stmt, table, mapping), 
                    Expression.OP_NOTEQ,
                    new NumericExpression(stmt, expr.table, expr.mapping));
            }
            else if (stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BOOLEAN_COMPARISON))
            {
                return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
            }
            else
            {
                return and(expr.not()).ior(not().and(expr));
            }
        }
        else
        {
            return super.ne(expr);
        }
    }

    public BooleanExpression in(SQLExpression expr, boolean not)
    {
        ColumnMapping colMapping = mapping.getColumnMapping(0);
        if (colMapping.isStringBased())
        {
            return new BooleanExpression(new CharacterExpression(stmt, table, mapping), 
                (not ? Expression.OP_NOTIN : Expression.OP_IN), expr);
        }

        return new BooleanExpression(this, (not ? Expression.OP_NOTIN : Expression.OP_IN), expr);
    }

    public SQLExpression invoke(String methodName, List args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, Boolean.class.getName(), 
            methodName, this, args);
    }

    public BooleanExpression neg()
    {
        return new BooleanExpression(Expression.OP_NEG, this);
    }
}