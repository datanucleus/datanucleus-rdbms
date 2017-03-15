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
package org.datanucleus.store.rdbms.sql;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.expression.JoinExpression;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a join in an SQL statement.
 * The join is of a type (see ANSI SQL), and with inner/left outer/right outer is accompanied by
 * join condition(s), joining from the source table to the target table via columns. Additionally
 * other conditions can be applied to restrict the join (such as discriminator).
 */
public class SQLJoin
{
    public enum JoinType
    {
        NON_ANSI_JOIN,
        INNER_JOIN,
        LEFT_OUTER_JOIN,
        RIGHT_OUTER_JOIN,
        CROSS_JOIN
    }

    /** Type of join to perform. */
    private JoinType type;

    /** Table we are joining to. This is always set irrespective the type of join. */
    private SQLTable targetTable;

    /** The current table that we are joining from to introduce this table. */
    private SQLTable sourceTable;

    /** Optional condition for the join. */
    private BooleanExpression condition;

    /**
     * Constructor for a join.
     * @param type Type of join (one of the defined types in this class).
     * @param targetTbl Target table that we are joining to
     * @param sourceTbl Table we are joining from
     * @param condition Join condition
     */
    public SQLJoin(JoinType type, SQLTable targetTbl, SQLTable sourceTbl, BooleanExpression condition)
    {
        if (type != JoinType.NON_ANSI_JOIN && 
            type != JoinType.INNER_JOIN && 
            type != JoinType.LEFT_OUTER_JOIN && 
            type != JoinType.RIGHT_OUTER_JOIN && 
            type != JoinType.CROSS_JOIN)
        {
            throw new NucleusException("Unsupported join type specified : " + type);
        }
        else if (targetTbl == null)
        {
            throw new NucleusException("Specification of join must supply the table reference");
        }

        this.type = type;
        this.targetTable = targetTbl;
        this.sourceTable = sourceTbl;
        this.condition = condition;
    }

    public JoinType getType()
    {
        return type;
    }

    public void setType(JoinType type)
    {
        this.type = type;
    }

    /**
     * Accessor for the table we are joining to.
     * @return The table joined to
     */
    public SQLTable getTargetTable()
    {
        return targetTable;
    }

    /**
     * Accessor for the table we are joining from.
     * @return The table we join from to bring in this other table
     */
    public SQLTable getSourceTable()
    {
        return sourceTable;
    }

    /**
     * Accessor for the conditions of the join.
     * These conditions can include
     * @return The conditions
     */
    public BooleanExpression getCondition()
    {
        return condition;
    }

    /**
     * Method to update the join "condition" to AND the provided expression.
     * @param expr The expression to add to the join "condition"
     */
    public void addAndCondition(BooleanExpression expr)
    {
        condition = (condition != null) ? condition.and(expr) : expr;
    }

    public String toString()
    {
        if (type == JoinType.CROSS_JOIN)
        {
            return "JoinType: CROSSJOIN " + type + " tbl=" + targetTable;
        }
        else if (type == JoinType.INNER_JOIN || type == JoinType.LEFT_OUTER_JOIN)
        {
            return "JoinType: " + (type == JoinType.INNER_JOIN ? "INNERJOIN" : "OUTERJOIN") + " tbl=" + targetTable + " joinedTbl=" + sourceTable;
        }
        return super.toString();
    }

    public SQLText toSQLText(DatastoreAdapter dba, boolean lock)
    {
        SQLText st = new SQLText();

        if (type != JoinType.NON_ANSI_JOIN)
        {
            if (type == JoinType.INNER_JOIN)
            {
                st.append("INNER JOIN ");
            }
            else if (type == JoinType.LEFT_OUTER_JOIN)
            {
                st.append("LEFT OUTER JOIN ");
            }
            else if (type == JoinType.RIGHT_OUTER_JOIN)
            {
                st.append("RIGHT OUTER JOIN ");
            }
            else if (type == JoinType.CROSS_JOIN)
            {
                st.append("CROSS JOIN ");
            }
            st.append(targetTable.toString());

            if (type == JoinType.INNER_JOIN || type == JoinType.LEFT_OUTER_JOIN || type == JoinType.RIGHT_OUTER_JOIN)
            {
                if (condition != null)
                {
                    st.append(" ON ");
                    st.append(condition.toSQLText());
                }
                else
                {
                    // No "on" condition so join as 1=0 (i.e no join, likely part of a polymorphic join or some such)
                    st.append(" ON 1=0");
                    NucleusLogger.DATASTORE_RETRIEVE.debug("Join condition has no 'on' condition defined! table=" + targetTable + 
                        " type=" + type + " joinedTable=" + sourceTable + " : so using ON clause as 1=0");
                }
            }

            if (lock && dba.supportsOption(DatastoreAdapter.LOCK_OPTION_PLACED_WITHIN_JOIN))
            {
                st.append(" WITH ").append(dba.getSelectWithLockOption());
            }
        }
        else
        {
            st.append("" + targetTable);
        }
        return st;
    }

    public static JoinType getJoinTypeForJoinExpressionType(JoinExpression.JoinType ejt)
    {
        if (ejt == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_INNER || ejt == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_INNER_FETCH)
        {
            return JoinType.INNER_JOIN;
        }
        else if (ejt == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_LEFT_OUTER || ejt == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_LEFT_OUTER_FETCH)
        {
            return JoinType.LEFT_OUTER_JOIN;
        }
        else if (ejt == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_RIGHT_OUTER || ejt == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_RIGHT_OUTER_FETCH)
        {
            return JoinType.RIGHT_OUTER_JOIN;
        }
        return null;
    }
}