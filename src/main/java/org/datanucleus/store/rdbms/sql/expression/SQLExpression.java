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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.query.expression.Expression.Operator;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;

/**
 * Base expression for SQL.
 * The principle here is that any expression (be it a field, literal, parameter, variable etc)
 * has a type and so needs a JavaTypeMapping to control reading/writing of that type.
 * An expression will typically have an SQLTable (in the overall statement) that it refers to.
 * A literal will not have an SQLTable since it represents a value.
 * The actual SQL for the expression is embodied in the SQLText field. Construction typically sets
 * the SQL for the respective expression.
 */
public abstract class SQLExpression
{
    /** The SQL statement that this is part of. */
    protected SQLStatement stmt;

    /** Table in the SQL statement that this mapping applies to. */
    protected SQLTable table;

    /** Mapping for this expression, defining how it is get/set. */
    protected JavaTypeMapping mapping;

    /** The Statement Text representing the SQL for this expression. */
    protected final SQLText st = new SQLText();

    // TODO Add javadoc for this - operator precedence ??
    protected Operator lowestOperator = null;

    /** Sub-expressions, where we have a field with multiple columns for example. */
    protected ColumnExpressionList subExprs;

    /** Parameter name that this represents (if this is a parameter, "?" in JDBC). */
    protected String parameterName = null;

    /** Optional additional WHERE expression that is built whilst generating this expression, and will be utilised when forming a BooleanExpression from this expression. TODO Implement this */
//    protected SQLExpression whereExpression;

    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    protected SQLExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        this.stmt = stmt;
        this.table = table;
        this.mapping = mapping;
        if (table != null)
        {
            this.subExprs = new ColumnExpressionList();
            if (mapping != null)
            {
                for (int i=0; i<mapping.getNumberOfColumnMappings(); i++)
                {
                    ColumnExpression colExpr = new ColumnExpression(stmt, table, mapping.getColumnMapping(i).getColumn());
                    subExprs.addExpression(colExpr);
                }
            }
            st.append(subExprs);
        }
    }

    /**
     * Perform an operation "op" on expression "expr1".
     * @param op operator
     * @param expr1 operand
     */
    protected SQLExpression(Expression.MonadicOperator op, SQLExpression expr1)
    {
        st.append(op.toString());

        if (op.isHigherThan(expr1.lowestOperator) || (op == Expression.OP_NOT && expr1.lowestOperator == Expression.OP_NOT))
        {
            st.append('(').append(expr1).append(')');
        }
        else
        {
            st.append(expr1);
        }

        stmt = expr1.stmt;
        mapping = expr1.mapping;
        lowestOperator = op;
    }

    /**
     * Perform an operation "op" between "expr1" and "expr2".
     * @param expr1 the first expression
     * @param op the operator between operands
     * @param expr2 the second expression
     */
    protected SQLExpression(SQLExpression expr1, Expression.DyadicOperator op, SQLExpression expr2)
    {
        stmt = expr1.stmt;
        mapping = (expr1.mapping != null ? expr1.mapping : expr2.mapping);

        lowestOperator = op;
        if (op == Expression.OP_CONCAT)
        {
            // Check if there is an overload for "concat" for this statement
            try
            {
                SQLExpression concatExpr = stmt.getSQLExpressionFactory().invokeOperation("concat", expr1, expr2);
                // enclose within parentheses to make sure the concat expression is first evaluated
                st.append(concatExpr.encloseInParentheses());
                return;
            }
            catch (UnsupportedOperationException uoe)
            {
            }
        }
        else if (op == Expression.OP_MOD)
        {
            // Check if there is an overload for "mod" for this statement
            try
            {
                SQLExpression modExpr = stmt.getSQLExpressionFactory().invokeOperation("mod", expr1, expr2);
                // enclose within parentheses to make sure the mod expression is first evaluated
                st.append(modExpr.encloseInParentheses());
                return;
            }
            catch (UnsupportedOperationException uoe)
            {
            }
        }

        if (op.isHigherThanLeftSide(expr1.lowestOperator))
        {
            st.append('(').append(expr1).append(')');
        }
        else
        {
            st.append(expr1);
        }

        st.append(op.toString());

        if (op.isHigherThanRightSide(expr2.lowestOperator))
        {
            st.append('(').append(expr2).append(')');
        }
        else
        {
            st.append(expr2);
        }

        // some databases use a ESCAPE expression with LIKE when using an escaped pattern
        if (op == Expression.OP_LIKE && stmt.getRDBMSManager().getDatastoreAdapter().supportsOption(DatastoreAdapter.ESCAPE_EXPRESSION_IN_LIKE_PREDICATE))
        {
            // TODO Remove the need for this code
            if (expr2 instanceof SQLLiteral)
            {
                DatastoreAdapter dba = stmt.getRDBMSManager().getDatastoreAdapter();
                st.append(' ');
                st.append(dba.getEscapePatternExpression());
                st.append(' ');
            }
        }
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [AS type] [,argN [AS typeN]])".
     * Number of objects in types (when supplied) is assumed to be the same as the number of arguments.
     * @param stmt The statement to use for this expression
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     * @param types List of String/SQLExpression for the types of arguments
     */
    protected SQLExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List<SQLExpression> args, List types)
    {
        if (types != null && args != null && args.size() != types.size())
        {
            throw new NucleusException("Number of arguments (" + args.size() + ") and their types (" + types.size() + ") are inconsistent");
        }

        this.stmt = stmt;
        if (stmt == null && args != null && args.size() > 0)
        {
            this.stmt = args.get(0).stmt;
        }

        this.mapping = mapping;
        st.append(functionName).append('(');

        if (args != null)
        {
            Iterator<SQLExpression> argIter = args.listIterator();
            Iterator typesIter = (types != null ? types.listIterator() : null);
            while (argIter.hasNext())
            {
                SQLExpression argExpr = argIter.next();
                st.append(argExpr);

                if (typesIter != null)
                {
                    Object argType = typesIter.next();
                    st.append(" AS ");
                    if (argType instanceof SQLExpression)
                    {
                        st.append((SQLExpression)argType);
                    }
                    else
                    {
                        st.append(argType.toString());
                    }
                }

                if (argIter.hasNext())
                {
                    st.append(",");
                }
            }
        }

        st.append(')');
    }

//    public SQLExpression getWhereExpression()
//    {
//        return whereExpression;
//    }

    public Operator getLowestOperator()
    {
        return lowestOperator;
    }

    public int getNumberOfSubExpressions()
    {
        return (subExprs != null ? subExprs.size() : 1);
    }

    public ColumnExpression getSubExpression(int index)
    {
        if (subExprs == null)
        {
            return null;
        }
        else if (index < 0 || index >= subExprs.size())
        {
            return null;
        }
        return subExprs.getExpression(index);
    }

    public SQLStatement getSQLStatement()
    {
        return stmt;
    }

    public boolean isParameter()
    {
        return parameterName != null;
    }

    public String getParameterName()
    {
        return parameterName;
    }

    public JavaTypeMapping getJavaTypeMapping()
    {
        return mapping;
    }

    public void setJavaTypeMapping(JavaTypeMapping mapping)
    {
        this.mapping = mapping;
        if (parameterName != null)
        {
            this.st.changeMappingForParameter(parameterName, mapping);
        }
    }

    public SQLTable getSQLTable()
    {
        return table;
    }

    /**
     * Method to return the SQL form of this expression.
     * @return The SQL
     */
    public SQLText toSQLText()
    {
        return st;
    }

    /**
     * Method to request the enclosure of this expression within parentheses.
     * @return the enclosed expression
     */
    public SQLExpression encloseInParentheses()
    {
        st.encloseInParentheses();
        return this;
    }

    /**
     * Conditional AND. Evaluates its right-hand operand only if the value of its left-hand operand is true.
     * @param expr the right-hand operand
     * @return the result value is true if both operand values are true; otherwise, the result is false.
     */
    public BooleanExpression and(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "&&", expr);
    }

    /**
     * Exclusive OR
     * @param expr the right-hand operand
     * @return the result value is the bitwise exclusive OR of the operand values.
     */
    public BooleanExpression eor(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "^", expr);
    }

    /**
     * Conditional OR. Evaluates its right-hand operand only if the value of its left-hand operand is false. 
     * @param expr the right-hand operand
     * @return the result value is false if both operand values are false; otherwise, the result is true.
     */
    public BooleanExpression ior(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "||", expr);
    }

    /**
     * Logical complement 
     * @return the result value is false if operand is true; otherwise, the result is true.
     */    
    public BooleanExpression not()
    {
        throw new IllegalExpressionOperationException("!", this);
    }
    
    /**
     * Equality operator (equals to)
     * @param expr the right-hand operand
     * @return The type of an equality expression is a boolean
     */
    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof DelegatedExpression)
        {
            return this.eq(((DelegatedExpression)expr).getDelegate());
        }
        throw new IllegalExpressionOperationException(this, "==", expr);
    }

    /**
     * Not equality operator (not equals to)
     * @param expr the right-hand operand
     * @return The type of an equality expression is a boolean
     */
    public BooleanExpression ne(SQLExpression expr)
    {
        if (expr instanceof DelegatedExpression)
        {
            return this.ne(((DelegatedExpression)expr).getDelegate());
        }
        throw new IllegalExpressionOperationException(this, "!=", expr);
    }

    /**
     * Relational operator (lower than)
     * @param expr the right-hand operand
     * @return true if the value of the left-hand operand is less than the value of the right-hand operand, 
     *     and otherwise is false.
     */    
    public BooleanExpression lt(SQLExpression expr)
    {
        if (expr instanceof DelegatedExpression)
        {
            return this.lt(((DelegatedExpression)expr).getDelegate());
        }
        throw new IllegalExpressionOperationException(this, "<", expr);
    }

    /**
     * Relational operator (lower than or equals)
     * @param expr the right-hand operand
     * @return true if the value of the left-hand operand is less than or equal to the value of the 
     *     right-hand operand, and otherwise is false.
     */    
    public BooleanExpression le(SQLExpression expr)
    {
        if (expr instanceof DelegatedExpression)
        {
            return this.le(((DelegatedExpression)expr).getDelegate());
        }
        throw new IllegalExpressionOperationException(this, "<=", expr);
    }

    /**
     * Relational operator (greater than)
     * @param expr the right-hand operand
     * @return true if the value of the left-hand operand is greater than the value of the right-hand 
     *     operand, and otherwise is false.
     */    
    public BooleanExpression gt(SQLExpression expr)
    {
        if (expr instanceof DelegatedExpression)
        {
            return this.gt(((DelegatedExpression)expr).getDelegate());
        }
        throw new IllegalExpressionOperationException(this, ">", expr);
    }

    /**
     * Relational operator (greater than or equals)
     * @param expr the right-hand operand
     * @return true if the value of the left-hand operand is greater than or equal the value of the 
     *     right-hand operand, and otherwise is false.
     */    
    public BooleanExpression ge(SQLExpression expr)
    {
        if (expr instanceof DelegatedExpression)
        {
            return this.ge(((DelegatedExpression)expr).getDelegate());
        }
        throw new IllegalExpressionOperationException(this, ">=", expr);
    }

    /**
     * In expression. Return true if this is contained by <code>expr</code>
     * @param expr the right-hand expression
     * @param not Whether we really want "not in"
     * @return true if the left-hand expression is contained by the right-hand expression. 
     *     Otherwise the result is false.
     */
    public BooleanExpression in(SQLExpression expr, boolean not)
    {
        throw new IllegalExpressionOperationException(this, "in", expr);
    }

    /**
     * Additive Operator. The binary + operator performs addition when applied to two operands 
     * of numeric type, producing the sum of the operands. If the type of either operand of a + operator is 
     * String, then the operation is string concatenation.
     * @param expr the right-hand operand
     * @return If one of the operands is String, the returned value is the string concatenation; 
     *     The sum of two operands of numeric type. The left-hand operand is the minuend and the right-hand 
     *     operand is the subtrahend;
     */
    public SQLExpression add(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "+", expr);
    }

    /**
     * Additive Operator. The binary - operator subtracts right-hand operand from left-hand operand.
     * @param expr the right-hand operand
     * @return The binary - operator performs subtraction when applied to two operands of numeric type 
     *     producing the difference of its operands; the left-hand operand is the minuend and the right-hand 
     *     operand is the subtrahend.
     */
    public SQLExpression sub(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "-", expr);
    }

    /**
     * Multiplication Operator 
     * @param expr the right-hand operator
     * @return The binary * operator performs multiplication, producing the product of its operands. 
     */
    public SQLExpression mul(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "*", expr);
    }

    /**
     * Division Operator. The left-hand operand is the dividend and the right-hand operand is the divisor.
     * @param expr the right-hand operator
     * @return The binary / operator performs division, producing the quotient of its operands
     */
    public SQLExpression div(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "/", expr);
    }

    /**
     * Remainder Operator. The left-hand operand is the dividend and the right-hand operand is the divisor.
     * @param expr the right-hand operator
     * @return The binary % operator is said to yield the remainder of its operands from an implied division
     */
    public SQLExpression mod(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException(this, "%", expr);
    }

    /**
     * Unary Minus Operator
     * @return the type of the unary minus expression is the promoted type of the operand.
     */
    public SQLExpression neg()
    {
        throw new IllegalExpressionOperationException("-", this);
    }

    /**
     * Bitwise Complement Operator
     * @return the type of the unary bitwise complement expression is the promoted type of the operand.
     */
    public SQLExpression com()
    {
        throw new IllegalExpressionOperationException("~", this);
    }

    /**
     * Distinct operator.
     * @return converts the expression into "DISTINCT (expr)"
     */
    public SQLExpression distinct()
    {
        st.prepend("DISTINCT (");
        st.append(")");
        return this;
    }

    /**
     * BITWISE AND operation.
     * @param expr expression representing the bitset
     * @return the bitwise AND expression
     */
    public SQLExpression bitAnd(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException("Bitwise AND on " + expr, this);
    }

    /**
     * BITWISE OR operation.
     * @param expr expression representing the bitset
     * @return the bitwise OR expression
     */
    public SQLExpression bitOr(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException("Bitwise OR on " + expr, this);
    }

    /**
     * A cast expression converts, at run time, a value of one type to a similar value of another type;
     * or confirms, at compile time, that the type of an expression is boolean; or checks, at run time, 
     * that a reference value refers to an object whose class is compatible with a specified reference type.
     * The type of the operand expression must be converted to the type explicitly named by the cast operator.
     * @param expr expression representing the type to cast to
     * @return the converted value
     */
    public SQLExpression cast(SQLExpression expr)
    {
        throw new IllegalExpressionOperationException("cast to " + expr, this);
    }

    /**
     * An "is" (instanceOf) expression, providing a BooleanExpression whether this expression
     * is an instanceof the provided type.
     * @param expr the expression representing the type
     * @param not Whether we really want "!instanceof"
     * @return Whether this expression is an instance of the provided type
     */
    public BooleanExpression is(SQLExpression expr, boolean not)
    {
        throw new IllegalExpressionOperationException("instanceof " + expr, this);
    }

    /**
     * Invocation of a method on this expression.
     * @param methodName name of the method to invoke
     * @param args Args to this method (if any)
     * @return the converted value
     */
    public SQLExpression invoke(String methodName, List args)
    {
        throw new IllegalExpressionOperationException("." + methodName, this);
    }

    public static class ColumnExpressionList
    {
        private List<ColumnExpression> exprs = new ArrayList();

        public void addExpression(ColumnExpression expression)
        {
            exprs.add(expression);
        }

        public ColumnExpression getExpression(int index)
        {
            return exprs.get(index);
        }

        public int size()
        {
            return exprs.size();
        }

        /**
         * Returns a string with a series of expression comma separated.
         * BNF notation: [ expr [, expr]]
         * @return the expression list comma separated 
         */
        public String toString()
        {
            StringBuilder expr = new StringBuilder();
            int size = exprs.size();
            for (int i=0; i<size; i++)
            {
                expr.append(getExpression(i).toString());
                if (i < (size-1))
                {
                    expr.append(',');
                }
            }
            return expr.toString();
        }

        public ColumnExpression[] toArray()
        {
            return exprs.toArray(new ColumnExpression[exprs.size()]);
        }
    }
}