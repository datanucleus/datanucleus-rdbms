/**********************************************************************
Copyright (c) 2015 Andy Jefferson and others. All rights reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.query.NullOrderingType;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.AggregateExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanLiteral;
import org.datanucleus.store.rdbms.sql.expression.ResultAliasExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * SQL SELECT Statement representation.
 * This will create a statement like
 * <pre>
 * SELECT {expr}, {expr}, ...
 * FROM {tblExpr} [joinInfo {tblExpr} ON ...] ...
 * WHERE {boolExpr} [AND|OR] {boolExpr} ...
 * GROUP BY {expr}, {expr}
 * HAVING {boolExpr}
 * ORDER BY {expr} [ASC|DESC], {expr} [ASC|DESC], ...
 * </pre>
 * and also supports UNIONs between SQLStatements, and having sub-queries of other SQLStatements.
 */
public class SelectStatement extends SQLStatement
{
    /** Whether to make use of any UNIONs on this statement (for when we just want to use this statement on its own). */
    protected boolean allowUnions = true;

    /** List of unioned SelectStatements (if any). */
    protected List<SelectStatement> unions = null;

    /** Whether the statement is distinct. */
    protected boolean distinct = false;

    /** List of selected items, including any alias to use. */
    protected List<SelectedItem> selectedItems = new ArrayList<>();

    /** whether there is an aggregate expression present in the select **/
    protected boolean aggregated = false;

    /** Expression(s) for the GROUP BY clause. */
    protected List<SQLExpression> groupingExpressions = null;

    /** Having clause. */
    protected BooleanExpression having;

    /** Expressions for any ORDER BY clause. */
    protected SQLExpression[] orderingExpressions = null;

    /** Directions for any ORDER BY expressions (1 for each orderingExpressions entry). */
    protected boolean[] orderingDirections = null;

    /** Directives for null handling of any ORDER BY expressions (1 for each orderingExpressions entry). */
    protected NullOrderingType[] orderNullDirectives = null;

    /** The offset for any range restriction. */
    protected long rangeOffset = -1;

    /** The number of records to be retrieved in any range restriction. */
    protected long rangeCount = -1;

    protected class SelectedItem
    {
        SQLText sqlText;
        String alias;
        boolean primary = true;
        public SelectedItem(SQLText st, String alias, boolean primary)
        {
            this.sqlText = st;
            this.alias = alias;
            this.primary = primary;
        }
        public SQLText getSQLText()
        {
            return sqlText;
        }
        public String getAlias()
        {
            return alias;
        }
        public boolean isPrimary()
        {
            return primary;
        }
        public void setAlias(String alias)
        {
            this.alias = alias;
        }
        public int hashCode()
        {
            return sqlText.hashCode() ^ (alias != null ? alias.hashCode() : 0);
        }
        public boolean equals(Object other)
        {
            if (other == null || !(other instanceof SelectedItem))
            {
                return false;
            }
            SelectedItem otherItem = (SelectedItem)other;
            if (!sqlText.equals(otherItem.sqlText))
            {
                return false;
            }
            if ((alias != null && !alias.equals(otherItem.alias)) || (otherItem.alias != null && !otherItem.alias.equals(alias)))
            {
                return false;
            }
            return true;
        }
    }

    /**
     * Constructor for a SELECT statement.
     * @param rdbmsMgr Store Manager
     * @param table The primary table to DELETE
     * @param alias Alias for the primary table
     * @param tableGroupName Group name for the primary table
     */
    public SelectStatement(RDBMSStoreManager rdbmsMgr, Table table, DatastoreIdentifier alias, String tableGroupName)
    {
        super(null, rdbmsMgr, table, alias, tableGroupName, null);
    }

    /**
     * Constructor for a SELECT statement.
     * @param rdbmsMgr Store Manager
     * @param table The primary table to DELETE
     * @param alias Alias for the primary table
     * @param tableGroupName Group name for the primary table
     * @param extensions Any extensions (optional)
     */
    public SelectStatement(RDBMSStoreManager rdbmsMgr, Table table, DatastoreIdentifier alias, String tableGroupName, Map<String, Object> extensions)
    {
        super(null, rdbmsMgr, table, alias, tableGroupName, extensions);
    }

    /**
     * Constructor for a SELECT statement, maybe as a subquery.
     * @param parentStmt Parent statement when this is a subquery SELECT.
     * @param rdbmsMgr Store Manager
     * @param table The primary table to DELETE
     * @param alias Alias for the primary table
     * @param tableGroupName Group name for the primary table
     */
    public SelectStatement(SQLStatement parentStmt, RDBMSStoreManager rdbmsMgr, Table table, DatastoreIdentifier alias, String tableGroupName)
    {
        super(parentStmt, rdbmsMgr, table, alias, tableGroupName, null);
    }

    /**
     * Constructor for a SELECT statement, maybe as a subquery.
     * @param parentStmt Parent statement when this is a subquery SELECT.
     * @param rdbmsMgr Store Manager
     * @param table The primary table to DELETE
     * @param alias Alias for the primary table
     * @param tableGroupName Group name for the primary table
     * @param extensions Any extensions (optional)
     */
    public SelectStatement(SQLStatement parentStmt, RDBMSStoreManager rdbmsMgr, Table table, DatastoreIdentifier alias, String tableGroupName, Map<String, Object> extensions)
    {
        super(parentStmt, rdbmsMgr, table, alias, tableGroupName, extensions);
    }

    /**
     * Accessor for whether the statement restricts the results to distinct.
     * @return Whether results are distinct
     */
    public boolean isDistinct()
    {
        return this.distinct;
    }

    /**
     * Mutator for whether the query returns distinct results.
     * @param distinct Whether to return distinct
     */
    public void setDistinct(boolean distinct)
    {
        invalidateStatement();
        this.distinct = distinct;
    }

    /**
     * Accessor for the number of selected items in the SELECT clause.
     * @return Number of selected items
     */
    public int getNumberOfSelects()
    {
        return selectedItems.size();
    }

    /**
     * Select an expression.
     * This will be used when adding aggregates to the select clause (e.g "COUNT(*)").
     * @param expr The expression to add to the select statement 
     * @param alias Optional alias for this selected expression
     * @return The index(es) of the expression in the select
     */
    public int[] select(SQLExpression expr, String alias)
    {
        if (expr == null)
        {
            throw new NucleusException("Expression to select is null");
        }

        invalidateStatement();

        boolean primary = true;
        if (expr instanceof AggregateExpression)
        {
            aggregated = true;
            primary = false;
        }
        else if (expr.getSQLTable() == null || expr.getJavaTypeMapping() == null)
        {
            primary = false;
        }

        int[] selected = new int[expr.getNumberOfSubExpressions()];
        if (expr.getNumberOfSubExpressions() > 1)
        {
            for (int i=0;i<expr.getNumberOfSubExpressions();i++)
            {
                selected[i] = selectItem(expr.getSubExpression(i).toSQLText(), alias != null ? (alias + i) : null, primary);
            }
        }
        else
        {
            selected[0] = selectItem(expr.toSQLText(), alias, primary);
        }

        if (unions != null && allowUnions)
        {
            // Apply the select to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.select(expr, alias);
            }
        }

        return selected;
    }

    /**
     * Add a select clause for the specified field (via its mapping).
     * If an alias is supplied and there are more than 1 column for this mapping then they will have
     * names like "{alias}_n" where n is the column number (starting at 0).
     * @param table The SQLTable to select from (null implies the primary table)
     * @param mapping The mapping for the field
     * @param alias optional alias
     * @param applyToUnions Whether to apply to unions
     * @return The column index(es) in the statement for the specified field (1 is first).
     */
    public int[] select(SQLTable table, JavaTypeMapping mapping, String alias, boolean applyToUnions)
    {
        if (mapping == null)
        {
            throw new NucleusException("Mapping to select is null");
        }
        else if (table == null)
        {
            // Default to the primary table if not specified
            table = primaryTable;
        }
        if (mapping.getTable() != table.getTable())
        {
            throw new NucleusException("Table being selected from (\"" + table.getTable() + 
                "\") is inconsistent with the column selected (\"" + mapping.getTable() + "\")");
        }

        invalidateStatement();

        ColumnMapping[] mappings = mapping.getColumnMappings();
        int[] selected = new int[mappings.length];
        for (int i=0;i<selected.length;i++)
        {
            DatastoreIdentifier colAlias = null;
            if (alias != null)
            {
                String name = (selected.length > 1) ? (alias + "_" + i) : alias;
                colAlias = rdbmsMgr.getIdentifierFactory().newColumnIdentifier(name);
            }

            SQLColumn col = new SQLColumn(table, mappings[i].getColumn(), colAlias);
            selected[i] = selectItem(new SQLText(col.getColumnSelectString()), alias != null ? colAlias.toString() : null, true);
        }

        if (applyToUnions && unions != null && allowUnions)
        {
            // Apply the select to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.select(table, mapping, alias);
            }
        }

        return selected;
    }

    /**
     * Add a select clause for the specified field (via its mapping) and apply to unions.
     * If an alias is supplied and there are more than 1 column for this mapping then they will have
     * names like "{alias}_n" where n is the column number (starting at 0).
     * @param table The SQLTable to select from (null implies the primary table)
     * @param mapping The mapping for the field
     * @param alias optional alias
     * @return The column index(es) in the statement for the specified field (1 is first).
     */
    public int[] select(SQLTable table, JavaTypeMapping mapping, String alias)
    {
        return select(table, mapping, alias, true);
    }

    /**
     * Add a select clause for the specified column.
     * @param table The SQLTable to select from (null implies the primary table)
     * @param column The column
     * @param alias Optional alias
     * @return The column index in the statement for the specified column (1 is first).
     */
    public int select(SQLTable table, Column column, String alias)
    {
        if (column == null)
        {
            throw new NucleusException("Column to select is null");
        }
        else if (table == null)
        {
            // Default to the primary table if not specified
            table = primaryTable;
        }
        if (column.getTable() != table.getTable())
        {
            throw new NucleusException("Table being selected from (\"" + table.getTable() + 
                "\") is inconsistent with the column selected (\"" + column.getTable() + "\")");
        }

        invalidateStatement();

        DatastoreIdentifier colAlias = null;
        if (alias != null)
        {
            colAlias = rdbmsMgr.getIdentifierFactory().newColumnIdentifier(alias);
        }
        SQLColumn col = new SQLColumn(table, column, colAlias);
        int position = selectItem(new SQLText(col.getColumnSelectString()), alias != null ? colAlias.toString() : null, true);

        if (unions != null && allowUnions)
        {
            // Apply the select to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.select(table, column, alias);
            }
        }

        return position;
    }

    /**
     * Internal method to find the position of an item in the select list and return the position
     * if found (first position is 1). If the item is not found then it is added and the new position returned.
     * @param st SQLText for this selected item
     * @param alias Any alias (optional)
     * @param primary Whether this selected item is a primary component (i.e column)
     * @return Position in the selectedItems list (first position is 1)
     */
    protected int selectItem(SQLText st, String alias, boolean primary)
    {
        SelectedItem item = new SelectedItem(st, alias, primary);
        if (selectedItems.contains(item))
        {
            // Already have a select item with this exact name so just return with that
            return selectedItems.indexOf(item) + 1;
        }

        // Check for same but previously had no alias, and now does, so put alias on existing
        for (SelectedItem currentItem : selectedItems)
        {
            if (currentItem.getSQLText().toSQL().equals(st.toSQL()) && currentItem.getAlias() == null && alias != null && currentItem.isPrimary() == primary)
            {
                // Same but no alias on existing item, so add the alias
                currentItem.setAlias(alias);
                return selectedItems.indexOf(currentItem)+1;
            }
        }

        int numberSelected = selectedItems.size();
        for (int i=0;i<numberSelected;i++)
        {
            SelectedItem selectedItem = selectedItems.get(i);
            if (selectedItem.getSQLText().equals(st))
            {
                // We already have the same column but different alias
                return (i+1);
            }
        }

        // The item doesn't exist so add it and return its new position
        selectedItems.add(item);
        return selectedItems.indexOf(item) + 1;
    }

    /**
     * Method to find the JOIN for the specified table and add the specified 'and' condition to the JOIN as an 'ON' clause.
     * @param sqlTbl The table
     * @param andCondition The 'ON' condition to add
     * @param applyToUnions Whether to apply to unions (see SelectStatement)
     */
    public void addAndConditionToJoinForTable(SQLTable sqlTbl, BooleanExpression andCondition, boolean applyToUnions)
    {
        SQLJoin join = getJoinForTable(sqlTbl);
        if (join != null)
        {
            join.addAndCondition(andCondition);
        }

        if (unions != null && applyToUnions)
        {
            // Apply the select to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.addAndConditionToJoinForTable(sqlTbl, andCondition, applyToUnions);
            }
        }
    }

    /**
     * Method to add a grouping expression to the query.
     * Adds the grouping to any unioned queries
     * @param expr The expression
     */
    public void addGroupingExpression(SQLExpression expr)
    {
        invalidateStatement();

        if (groupingExpressions == null)
        {
            groupingExpressions = new ArrayList<>();
        }
        groupingExpressions.add(expr);
        aggregated = true;

        if (unions != null && allowUnions)
        {
            // Apply the grouping to all unions
            Iterator<SelectStatement> i = unions.iterator();
            while (i.hasNext())
            {
                i.next().addGroupingExpression(expr);
            }
        }
    }

    /**
     * Mutator for the "having" expression.
     * @param expr Boolean expression for the having clause
     */
    public void setHaving(BooleanExpression expr)
    {
        invalidateStatement();

        having = expr;
        aggregated = true;

        if (unions != null && allowUnions)
        {
            // Apply the having to all unions
            Iterator<SelectStatement> i = unions.iterator();
            while (i.hasNext())
            {
                i.next().setHaving(expr);
            }
        }
    }

    /**
     * Mutator for the ordering criteria.
     * @param exprs The expressions to order by
     * @param descending Whether each expression is ascending/descending
     */
    public void setOrdering(SQLExpression[] exprs, boolean[] descending)
    {
        setOrdering(exprs, descending, null);
    }

    /**
     * Mutator for the ordering criteria.
     * @param exprs The expressions to order by
     * @param descending Whether each expression is ascending/descending
     * @param nullOrders Ordering for nulls (if provided)
     */
    public void setOrdering(SQLExpression[] exprs, boolean[] descending, NullOrderingType[] nullOrders)
    {
        if (exprs != null && descending != null && exprs.length != descending.length)
        {
            throw new NucleusException(Localiser.msg("052503", "" + exprs.length, "" + descending.length)).setFatal();
        }

        invalidateStatement();

        orderingExpressions = exprs;
        orderingDirections = descending;
        orderNullDirectives = nullOrders;
    }

    /**
     * Method to add a range constraint on any SELECT.
     * This typically will use LIMIT/OFFSET where they are supported by the underlying RDBMS.
     * @param offset The offset to start from
     * @param count The number of records to return
     */
    public void setRange(long offset, long count)
    {
        invalidateStatement();

        this.rangeOffset = offset;
        this.rangeCount = count;
    }

    public SQLText getSQLText()
    {
        if (sql != null)
        {
            return sql;
        }

        DatastoreAdapter dba = getDatastoreAdapter();
        boolean lock = false;
        Boolean val = (Boolean)getValueForExtension(EXTENSION_LOCK_FOR_UPDATE);
        if (val != null)
        {
            lock = val.booleanValue();
        }

        boolean addAliasToAllSelects = false;
        if (rangeOffset > 0 || rangeCount > -1)
        {
            if (dba.getRangeByRowNumberColumn2().length() > 0)
            {
                // Doing "SELECT * FROM (...)" so to be safe we need alias on all selects
                addAliasToAllSelects = true;
            }
        }

        // SELECT ..., ..., ...
        sql = new SQLText("SELECT ");

        if (distinct)
        {
            sql.append("DISTINCT ");
        }

        addOrderingColumnsToSelect();

        if (selectedItems.isEmpty())
        {
            // Nothing selected so select all
            sql.append("*");
        }
        else
        {
            int autoAliasNum = 0;
            Iterator<SelectedItem> selectItemIter = selectedItems.iterator();
            while (selectItemIter.hasNext())
            {
                SelectedItem selectedItem = selectItemIter.next();
                SQLText selectedST = selectedItem.getSQLText();
                sql.append(selectedST);

                if (selectedItem.getAlias() != null)
                {
                    sql.append(" AS " + rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase(selectedItem.getAlias()));
                }
                else
                {
                    if (addAliasToAllSelects)
                    {
                        // This query needs an alias on all selects, so add "DN_{X}"
                        sql.append(" AS ").append(rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase("DN_" + autoAliasNum));
                        autoAliasNum++;
                    }
                }

                if (selectItemIter.hasNext())
                {
                    sql.append(',');
                }
            }
            if ((rangeOffset > -1 || rangeCount > -1) && dba.getRangeByRowNumberColumn().length() > 0)
            {
                // Add a ROW NUMBER column if supported as the means of handling ranges by the RDBMS
                sql.append(',').append(dba.getRangeByRowNumberColumn()).append(" rn");
            }
        }

        // FROM ...
        sql.append(" FROM ");
        sql.append(primaryTable.toString());
        if (lock && dba.supportsOption(DatastoreAdapter.LOCK_ROW_USING_OPTION_AFTER_FROM))
        {
            // Add locking after FROM where supported
            sql.append(" WITH ").append(dba.getSelectWithLockOption());
        }
        if (joins != null)
        {
            sql.append(getSqlForJoins(lock));
        }

        // WHERE ...
        if (where != null)
        {
            sql.append(" WHERE ").append(where.toSQLText());
        }

        // GROUP BY ...
        if (groupingExpressions != null)
        {
            List<SQLText> groupBy = new ArrayList<>();
            Iterator<SQLExpression> groupIter = groupingExpressions.iterator();
            while (groupIter.hasNext())
            {
                SQLExpression expr = groupIter.next();
                boolean exists = false;
                String exprSQL = expr.toSQLText().toSQL();
                for (SQLText st : groupBy)
                {
                    String sql = st.toSQL();
                    if (sql.equals(exprSQL))
                    {
                        exists = true;
                        break;
                    }
                }
                if (!exists)
                {
                    groupBy.add(expr.toSQLText());
                }
            }

            if (dba.supportsOption(DatastoreAdapter.GROUP_BY_REQUIRES_ALL_SELECT_PRIMARIES))
            {
                // Check that all select items are represented in the grouping for those RDBMS that need that
                for (SelectedItem selItem : selectedItems)
                {
                    if (selItem.isPrimary())
                    {
                        boolean exists = false;
                        String selItemSQL = selItem.getSQLText().toSQL();
                        for (SQLText st : groupBy)
                        {
                            String sql = st.toSQL();
                            if (sql.equals(selItemSQL))
                            {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists)
                        {
                            groupBy.add(selItem.getSQLText());
                        }
                    }
                }
            }

            if (groupBy.size() > 0 && aggregated)
            {
                sql.append(" GROUP BY ");
                for (int i=0; i<groupBy.size(); i++)
                {
                    if (i > 0)
                    {
                        sql.append(',');
                    }
                    sql.append(groupBy.get(i));
                }
            }
        }

        // HAVING ...
        if (having != null)
        {
            sql.append(" HAVING ").append(having.toSQLText());
        }

        if (unions != null && allowUnions)
        {
            // Add on any UNIONed statements
            if (!dba.supportsOption(DatastoreAdapter.UNION_SYNTAX))
            {
                throw new NucleusException(Localiser.msg("052504", "UNION")).setFatal();
            }

            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                if (dba.supportsOption(DatastoreAdapter.USE_UNION_ALL))
                {
                    sql.append(" UNION ALL ");
                }
                else
                {
                    sql.append(" UNION ");
                }

                SelectStatement stmt = unionIter.next();
                SQLText unionSql = stmt.getSQLText();
                sql.append(unionSql);
            }
        }

        // ORDER BY ...
        SQLText orderStmt = generateOrderingStatement();
        if (orderStmt != null)
        {
            sql.append(" ORDER BY ").append(orderStmt);
        }

        // RANGE
        if (rangeOffset > -1 || rangeCount > -1)
        {
            // Add a LIMIT clause to end of statement if supported by the adapter
            String limitClause = dba.getRangeByLimitEndOfStatementClause(rangeOffset, rangeCount, orderStmt != null);
            if (limitClause.length() > 0)
            {
                sql.append(" ").append(limitClause);
            }
        }

        if (lock)
        {
            if (dba.supportsOption(DatastoreAdapter.LOCK_ROW_USING_SELECT_FOR_UPDATE))
            {
                // Add any required locking based on the RDBMS capability
                if (distinct && !dba.supportsOption(DatastoreAdapter.DISTINCT_WITH_SELECT_FOR_UPDATE))
                {
                    NucleusLogger.QUERY.warn(Localiser.msg("052502"));
                }
                else if (groupingExpressions != null && !dba.supportsOption(DatastoreAdapter.GROUPING_WITH_SELECT_FOR_UPDATE))
                {
                    NucleusLogger.QUERY.warn(Localiser.msg("052506"));
                }
                else if (having != null && !dba.supportsOption(DatastoreAdapter.HAVING_WITH_SELECT_FOR_UPDATE))
                {
                    NucleusLogger.QUERY.warn(Localiser.msg("052507"));
                }
                else if (orderingExpressions != null && !dba.supportsOption(DatastoreAdapter.ORDERING_WITH_SELECT_FOR_UPDATE))
                {
                    NucleusLogger.QUERY.warn(Localiser.msg("052508"));
                }
                else if (joins != null && !joins.isEmpty() && !dba.supportsOption(DatastoreAdapter.MULTITABLES_WITH_SELECT_FOR_UPDATE))
                {
                    NucleusLogger.QUERY.warn(Localiser.msg("052509"));
                }
                else
                {
                    sql.append(" " + dba.getSelectForUpdateText());
                    if (dba.supportsOption(DatastoreAdapter.LOCK_ROW_USING_SELECT_FOR_UPDATE_NOWAIT))
                    {
                        Boolean nowait = (Boolean) getValueForExtension(EXTENSION_LOCK_FOR_UPDATE_NOWAIT);
                        if (nowait != null)
                        {
                            sql.append(" NOWAIT");
                        }
                    }
                }
            }
            else if (!dba.supportsOption(DatastoreAdapter.LOCK_ROW_USING_OPTION_AFTER_FROM) &&
                     !dba.supportsOption(DatastoreAdapter.LOCK_ROW_USING_OPTION_WITHIN_JOIN))
            {
                NucleusLogger.QUERY.warn("Requested locking of query statement, but this RDBMS doesn't support a convenient mechanism");
            }
        }

        if (rangeOffset > 0 || rangeCount > -1)
        {
            if (dba.getRangeByRowNumberColumn2().length() > 0)
            {
                // Oracle-specific using ROWNUM. Creates a query of the form
                // SELECT * FROM (
                //     SELECT subq.*, ROWNUM rn FROM (
                //         SELECT x1, x2, ... FROM ... WHERE ... ORDER BY ...
                //     ) subq
                // ) WHERE rn > {offset} AND rn <= {count}
                SQLText userSql = sql;

                // SELECT all columns of userSql, plus ROWNUM, with the FROM being the users query
                SQLText innerSql = new SQLText("SELECT subq.*");
                innerSql.append(',').append(dba.getRangeByRowNumberColumn2()).append(" rn");
                innerSql.append(" FROM (").append(userSql).append(") subq ");

                // Put that query as the FROM of the outer query, and apply the ROWNUM restrictions
                SQLText outerSql = new SQLText("SELECT * FROM (").append(innerSql).append(") ");
                outerSql.append("WHERE ");
                if (rangeOffset > 0)
                {
                    outerSql.append("rn > " + rangeOffset);
                    if (rangeCount > -1)
                    {
                        outerSql.append(" AND rn <= " + (rangeCount+rangeOffset));
                    }
                }
                else
                {
                    outerSql.append(" rn <= " + rangeCount);
                }

                sql = outerSql;
            }
            else if (dba.getRangeByRowNumberColumn().length() > 0)
            {
                // DB2-specific ROW_NUMBER weirdness. Creates a query of the form
                // SELECT subq.x1, subq.x2, ... FROM (
                //     SELECT x1, x2, ..., {keyword} rn FROM ... WHERE ... ORDER BY ...) subq
                // WHERE subq.rn >= {offset} AND subq.rn < {count}
                // This apparently works for DB2 (unverified, but claimed by IBM employee)
                SQLText userSql = sql;
                sql = new SQLText("SELECT ");
                Iterator<SelectedItem> selectedItemIter = selectedItems.iterator();
                while (selectedItemIter.hasNext())
                {
                    SelectedItem selectedItemExpr = selectedItemIter.next();
                    sql.append("subq.");
                    String selectedCol = selectedItemExpr.getSQLText().toSQL();
                    if (selectedItemExpr.getAlias() != null)
                    {
                        selectedCol = rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase(selectedItemExpr.getAlias());
                    }
                    else
                    {
                        // strip out qualifier when encountered from column name since we are adding a new qualifier above.
                        // NOTE THAT THIS WILL FAIL IF THE ORIGINAL QUERY HAD "A0.COL1, B0.COL1" IN THE SELECT
                        int dotIndex = selectedCol.indexOf(".");
                        if (dotIndex > 0) 
                        {
                            // Remove qualifier name and the dot
                            selectedCol = selectedCol.substring(dotIndex+1);
                        }
                    }

                    sql.append(selectedCol);
                    if (selectedItemIter.hasNext())
                    {
                        sql.append(',');
                    }
                }
                sql.append(" FROM (").append(userSql).append(") subq WHERE ");
                if (rangeOffset > 0)
                {
                    sql.append("subq.rn").append(">").append("" + rangeOffset);
                }
                if (rangeCount > 0)
                {
                    if (rangeOffset > 0)
                    {
                        sql.append(" AND ");
                    }
                    sql.append("subq.rn").append("<=").append("" + (rangeCount + rangeOffset));
                }
            }
        }

        return sql;
    }

    /**
     * Convenience method to reorder the joins to be in logical order.
     * If a join needed to be changed during the generation process, it will have been removed and then the replacement added later. 
     * This method reorders the joins so that the joins are only relative to "known" tables.
     * @param joinsToAdd List of joins
     * @return The ordered list of joins
     */
    private List<SQLJoin> reorderJoins(List<SQLJoin> joinsToAdd)
    {
        List<SQLJoin> orderedJoins = new ArrayList<>();
        if (joinsToAdd == null)
        {
            requiresJoinReorder = false;
            return orderedJoins;
        }

        while (joinsToAdd.size() > 0)
        {
            Iterator<SQLJoin> joinIter = joinsToAdd.iterator();
            int origSize = joinsToAdd.size();
            while (joinIter.hasNext())
            {
                SQLJoin join = joinIter.next();
                if (join.getType() == JoinType.CROSS_JOIN)
                {
                    // Cross joins don't relate to any other table so are fine
                    orderedJoins.add(join);
                    joinIter.remove();
                }
                else if (join.getType() == JoinType.NON_ANSI_JOIN)
                {
                    // Non-ANSI joins use the WHERE clause so are fine
                    orderedJoins.add(join);
                    joinIter.remove();
                }
                else if (join.getSourceTable().equals(primaryTable))
                {
                    // Joins to the primary table are fine
                    orderedJoins.add(join);
                    joinIter.remove();
                }
                else
                {
                    Iterator<SQLJoin> knownJoinIter = orderedJoins.iterator();
                    boolean valid = false;
                    while (knownJoinIter.hasNext())
                    {
                        SQLJoin currentJoin = knownJoinIter.next();
                        if (join.getSourceTable().equals(currentJoin.getTargetTable()))
                        {
                            valid = true;
                            break;
                        }
                    }
                    if (valid)
                    {
                        // Only used known joins so fine
                        orderedJoins.add(join);
                        joinIter.remove();
                    }
                }
            }

            if (joinsToAdd.size() == origSize)
            {
                // Somehow the user has ended up with a circular pattern of joins
                throw new NucleusException("Unable to reorder joins for SQL statement since circular!" +
                    " Consider reordering the components in the WHERE clause : affected joins - " + StringUtils.collectionToString(joinsToAdd));
            }
        }

        requiresJoinReorder = false;
        return orderedJoins;
    }

    /**
     * Convenience method to return the JOIN clause implied by the "joins" List.
     * @param lock Whether to add locking on the join clause (only for some RDBMS)
     * @return The SQL for the join clause
     */
    protected SQLText getSqlForJoins(boolean lock)
    {
        // TODO Consider performing a reorder in more situations to cater for use of implicit joins potentially not being in optimum order
        if (requiresJoinReorder)
        {
            List<SQLJoin> theJoins = reorderJoins(joins);
            joins = theJoins;
        }

        SQLText sql = new SQLText();
        DatastoreAdapter dba = getDatastoreAdapter();
        Iterator<SQLJoin> iter = joins.iterator();
        while (iter.hasNext())
        {
            SQLJoin join = iter.next();
            if (join.getType() == JoinType.CROSS_JOIN)
            {
                if (dba.supportsOption(DatastoreAdapter.ANSI_CROSSJOIN_SYNTAX))
                {
                    // ANSI-92 style joins, separate joins by space
                    sql.append(" ").append(join.toSQLText(dba, lock));
                }
                else if (dba.supportsOption(DatastoreAdapter.CROSSJOIN_ASINNER11_SYNTAX))
                {
                    sql.append(" INNER JOIN " + join.getTargetTable() + " ON 1=1");
                }
                else
                {
                    // "ANSI-86" style cross join, separate join by comma
                    sql.append(",").append(join.getTargetTable().toString());
                }
            }
            else
            {
                if (dba.supportsOption(DatastoreAdapter.ANSI_JOIN_SYNTAX))
                {
                    // ANSI-92 style joins, separate joins by space
                    sql.append(" ").append(join.toSQLText(dba, lock));
                }
                else
                {
                    // "ANSI-86" style joins, separate joins by comma
                    sql.append(",").append(join.toSQLText(dba, lock));
                }
            }
        }
        return sql;
    }

    /** Positions of order columns in the SELECT (for datastores that require ordering using those). */
    private int[] orderingColumnIndexes;

    /**
     * Convenience method to generate the ordering statement to add to the overall query statement.
     * @return The ordering statement
     */
    protected SQLText generateOrderingStatement()
    {
        SQLText orderStmt = null;
        if (orderingExpressions != null && orderingExpressions.length > 0)
        {
            DatastoreAdapter dba = getDatastoreAdapter();
            if (dba.supportsOption(DatastoreAdapter.ORDERBY_USING_SELECT_COLUMN_INDEX))
            {
                // Order using the indexes of the ordering columns in the SELECT
                orderStmt = new SQLText();
                for (int i=0; i<orderingExpressions.length; ++i)
                {
                    if (i > 0)
                    {
                        orderStmt.append(',');
                    }
                    orderStmt.append(Integer.toString(orderingColumnIndexes[i]));
                    if (orderingDirections[i])
                    {
                        orderStmt.append(" DESC");
                    }
                    if (orderNullDirectives != null && orderNullDirectives[i] != null && dba.supportsOption(DatastoreAdapter.ORDERBY_NULLS_DIRECTIVES))
                    {
                        // Apply "NULLS [FIRST | LAST]" since supported by this datastore
                        orderStmt.append(" " + (orderNullDirectives[i] == NullOrderingType.NULLS_FIRST ? "NULLS FIRST" : "NULLS LAST"));
                    }
                }
            }
            else
            {
                // Order using column aliases "NUCORDER{i}"
                orderStmt = new SQLText();
                boolean needsSelect = dba.supportsOption(DatastoreAdapter.INCLUDE_ORDERBY_COLS_IN_SELECT);
                if (parent != null)
                {
                    // Don't select ordering columns with subqueries, since we will select just the required column(s)
                    needsSelect = false;
                }

                for (int i=0; i<orderingExpressions.length; ++i)
                {
                    SQLExpression orderExpr = orderingExpressions[i];
                    boolean orderDirection = orderingDirections[i];
                    NullOrderingType orderNullDirective = (orderNullDirectives != null ? orderNullDirectives[i] : null);

                    if (i > 0)
                    {
                        orderStmt.append(',');
                    }

                    if (needsSelect && !aggregated)
                    {
                        if (orderExpr instanceof ResultAliasExpression)
                        {
                            String orderStr = ((ResultAliasExpression)orderExpr).getResultAlias();
                            orderStr = rdbmsMgr.getIdentifierFactory().getIdentifierTruncatedToAdapterColumnLength(orderStr); // make sure it is truncated for the datastore limits
                            orderStr = rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase(orderStr); // put it in the case of the datastore
                            addOrderComponent(orderStmt, orderStr, orderExpr, orderDirection, orderNullDirective, dba);
                        }
                        else
                        {
                            // Order by the "NUCORDER?" if we need them to be selected and it isn't an aggregate
                            String orderString = "NUCORDER" + i;
                            if (orderExpr.getNumberOfSubExpressions() == 1)
                            {
                                String orderStr = rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase(orderString);
                                addOrderComponent(orderStmt, orderStr, orderExpr, orderDirection, orderNullDirective, dba);
                            }
                            else
                            {
                                ColumnMapping[] mappings = orderExpr.getJavaTypeMapping().getColumnMappings();
                                for (int j=0;j<mappings.length;j++)
                                {
                                    String orderStr = rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase(orderString + "_" + j);
                                    addOrderComponent(orderStmt, orderStr, orderExpr, orderDirection, orderNullDirective, dba);

                                    if (j < mappings.length-1)
                                    {
                                        orderStmt.append(',');
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        if (orderExpr instanceof ResultAliasExpression)
                        {
                            String orderStr = ((ResultAliasExpression)orderExpr).getResultAlias();
                            orderStr = rdbmsMgr.getIdentifierFactory().getIdentifierTruncatedToAdapterColumnLength(orderStr); // make sure it is truncated for the datastore limits
                            orderStr = rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase(orderStr); // put it in the case of the datastore
                            addOrderComponent(orderStmt, orderStr, orderExpr, orderDirection, orderNullDirective, dba);
                        }
                        else
                        {
                            // Order by the "THIS.COLUMN" otherwise
                            addOrderComponent(orderStmt, orderExpr.toSQLText().toSQL(), orderExpr, orderDirection, orderNullDirective, dba);
                        }
                    }
                }
            }
        }
        return orderStmt;
    }

    protected void addOrderComponent(SQLText orderST, String orderString, SQLExpression orderExpr, boolean orderDirection, NullOrderingType orderNullDirective, DatastoreAdapter dba)
    {
        String orderParam = dba.getOrderString(rdbmsMgr, orderString, orderExpr);

        if (orderNullDirective != null)
        {
            if (dba.supportsOption(DatastoreAdapter.ORDERBY_NULLS_DIRECTIVES))
            {
                // Apply "NULLS [FIRST | LAST]" directly since supported by this datastore
                orderST.append(orderParam).append(orderDirection ? " DESC" : "").append(orderNullDirective == NullOrderingType.NULLS_FIRST ? " NULLS FIRST" : " NULLS LAST");
            }
            else if (dba.supportsOption(DatastoreAdapter.ORDERBY_NULLS_USING_CASE_NULL))
            {
                // "(CASE WHEN {param} IS NULL THEN 1 ELSE 0 END) [ASC|DESC], {param} [ASC|DESC]"
                // NOTE : This only works because SQLServer is the only adapter using CASE and it seemingly doesn't need orderString
				String caseWhenOrderParam = orderExpr.toSQLText().toSQL();
				if (orderExpr instanceof ResultAliasExpression)
				{
					SelectStatement orderExprStmt = (SelectStatement) orderExpr.getSQLStatement();
					String selectAlias = ((ResultAliasExpression) orderExpr).getResultAlias();
					for (int i = 0; i < orderExprStmt.selectedItems.size(); i++)
					{
						SelectedItem item = orderExprStmt.selectedItems.get(i);
						if (selectAlias.equalsIgnoreCase(item.getAlias()))
						{
							caseWhenOrderParam = item.getSQLText().toSQL();
							break;
						}
					}
				}
                orderST.append("(CASE WHEN " + caseWhenOrderParam + " IS NULL THEN 1 ELSE 0 END)").append(orderNullDirective == NullOrderingType.NULLS_FIRST ? " DESC" : " ASC").append(",");

                orderST.append(orderParam).append(orderDirection ? " DESC" : "");
            }
            else if (dba.supportsOption(DatastoreAdapter.ORDERBY_NULLS_USING_COLUMN_IS_NULL))
            {
                if (orderExpr.getSQLTable() != null)
                {
                    // Datastore requires nulls ordering using "{col} IS NULL" extra ordering clause. Note : don't do this when the ordering component is not a simple column
                    orderST.append(orderParam).append(" IS NULL").append(orderNullDirective == NullOrderingType.NULLS_FIRST ? " DESC" : " ASC").append(",");
                }

                orderST.append(orderParam).append(orderDirection ? " DESC" : "");
            }
            else if (dba.supportsOption(DatastoreAdapter.ORDERBY_NULLS_USING_ISNULL))
            {
                if (orderExpr.getSQLTable() != null)
                {
                    // Datastore requires nulls ordering using ISNULL extra ordering clause. Note : don't do this when the ordering component is not a simple column
                    orderST.append("ISNULL(").append(orderParam).append(")").append(orderNullDirective == NullOrderingType.NULLS_FIRST ? " DESC" : " ASC").append(",");
                }

                orderST.append(orderParam).append(orderDirection ? " DESC" : "");
            }
            else
            {
                NucleusLogger.DATASTORE_RETRIEVE.warn("Query contains NULLS directive yet this datastore doesn't provide any support for handling this. Nulls directive will be ignored");
                orderST.append(orderParam).append(orderDirection ? " DESC" : "");
            }
        }
        else
        {
            orderST.append(orderParam).append(orderDirection ? " DESC" : "");
        }
    }

    /**
     * Convenience method to add any necessary columns to the SELECT that are needed
     * by the ordering constraint.
     */
    protected void addOrderingColumnsToSelect()
    {
        if (orderingExpressions != null && parent == null) // Don't do this for subqueries, since we will be selecting just the necessary column(s)
        {
            // Add any ordering columns to the SELECT
            DatastoreAdapter dba = getDatastoreAdapter();
            if (dba.supportsOption(DatastoreAdapter.ORDERBY_USING_SELECT_COLUMN_INDEX))
            {
                // Order using the indexes of the ordering columns in the SELECT
                orderingColumnIndexes = new int[orderingExpressions.length];

                // Add the ordering columns to the selected list, saving the positions
                for (int i=0; i<orderingExpressions.length; ++i)
                {
                    orderingColumnIndexes[i] = selectItem(orderingExpressions[i].toSQLText(), null, !aggregated);
                    if (unions != null && allowUnions)
                    {
                        Iterator<SelectStatement> iterator = unions.iterator();
                        while (iterator.hasNext())
                        {
                            SelectStatement stmt = iterator.next();
                            stmt.selectItem(orderingExpressions[i].toSQLText(), null, !aggregated);
                        }
                    }
                }
            }
            else if (dba.supportsOption(DatastoreAdapter.INCLUDE_ORDERBY_COLS_IN_SELECT))
            {
                // Order using column aliases "NUCORDER{i}"
                for (int i=0; i<orderingExpressions.length; ++i)
                {
                    if (orderingExpressions[i] instanceof ResultAliasExpression)
                    {
                        // Nothing to do since this is ordering by a result alias
                    }
                    else if (orderingExpressions[i].getNumberOfSubExpressions() == 1 || aggregated)
                    {
                        String orderExprAlias = rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase("NUCORDER" + i);
                        if (unions != null && allowUnions)
                        {
                            Iterator<SelectStatement> iterator = unions.iterator();
                            while (iterator.hasNext())
                            {
                                SelectStatement stmt = iterator.next();
                                stmt.selectItem(orderingExpressions[i].toSQLText(), aggregated ? null : orderExprAlias, !aggregated);
                            }
                        }

                        selectItem(orderingExpressions[i].toSQLText(), aggregated ? null : orderExprAlias, !aggregated);
                    }
                    else
                    {
                        JavaTypeMapping m = orderingExpressions[i].getJavaTypeMapping();

                        ColumnMapping[] mappings = m.getColumnMappings();
                        for (int j=0;j<mappings.length;j++)
                        {
                            String alias = rdbmsMgr.getIdentifierFactory().getIdentifierInAdapterCase("NUCORDER" + i + "_" + j);
                            DatastoreIdentifier aliasId = rdbmsMgr.getIdentifierFactory().newColumnIdentifier(alias);
                            SQLColumn col = new SQLColumn(orderingExpressions[i].getSQLTable(), mappings[j].getColumn(), aliasId);
                            selectItem(new SQLText(col.getColumnSelectString()), alias, !aggregated);

                            if (unions != null && allowUnions)
                            {
                                Iterator<SelectStatement> iterator = unions.iterator();
                                while (iterator.hasNext())
                                {
                                    SelectStatement stmt = iterator.next();
                                    stmt.selectItem(new SQLText(col.getColumnSelectString()), alias, !aggregated);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void setAllowUnions(boolean flag)
    {
        allowUnions = flag;
    }

    public int getNumberOfUnions()
    {
        if (unions == null || !allowUnions)
        {
            return 0;
        }

        int number = unions.size();
        Iterator<SelectStatement> unionIterator = unions.iterator();
        while (unionIterator.hasNext())
        {
            SelectStatement unioned = unionIterator.next();
            number += unioned.getNumberOfUnions();
        }
        return number;
    }

    /**
     * Accessor for the unioned statements.
     * @return The unioned SQLStatements
     */
    public List<SelectStatement> getUnions()
    {
        return allowUnions ? unions : null;
    }

    /**
     * Method to union this SQL statement with another SQL statement.
     * @param stmt The other SQL statement to union
     */
    public void union(SelectStatement stmt)
    {
        invalidateStatement();
        if (unions == null)
        {
            unions = new ArrayList<>();
        }
        unions.add(stmt);
    }

    /**
     * Convenience accessor for whether all unions of this statement are for the same primary table.
     * @return Whether all unions have the same primary table
     */
    public boolean allUnionsForSamePrimaryTable()
    {
        if (unions != null && allowUnions)
        {
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SQLStatement unionStmt = unionIter.next();
                if (!unionStmt.getPrimaryTable().equals(primaryTable))
                {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public SQLTable join(JoinType joinType, SQLTable sourceTable, JavaTypeMapping sourceMapping, JavaTypeMapping sourceParentMapping,
            Table target, String targetAlias, JavaTypeMapping targetMapping, JavaTypeMapping targetParentMapping, Object[] discrimValues, String tableGrpName, boolean applyToUnions,
            SQLJoin parentJoin)
    {
        invalidateStatement();

        // Create the SQLTable to join to.
        if (tables == null)
        {
            tables = new HashMap<>();
        }
        if (tableGrpName == null)
        {
            tableGrpName = "Group" + tableGroups.size();
        }
        if (targetAlias == null)
        {
            targetAlias = namer.getAliasForTable(this, target, tableGrpName);
        }
        if (sourceTable == null)
        {
            sourceTable = primaryTable;
        }
        DatastoreIdentifier targetId = rdbmsMgr.getIdentifierFactory().newTableIdentifier(targetAlias);
        SQLTable targetTbl = new SQLTable(this, target, targetId, tableGrpName);
        putSQLTableInGroup(targetTbl, tableGrpName, joinType);

        // Generate the join condition to use
        BooleanExpression joinCondition = getJoinConditionForJoin(sourceTable, sourceMapping, sourceParentMapping, targetTbl, targetMapping, targetParentMapping, discrimValues);

        addJoin(joinType, sourceTable, targetTbl, joinCondition, parentJoin);

        if (unions != null && applyToUnions)
        {
            // Apply the join to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.join(joinType, sourceTable, sourceMapping, sourceParentMapping, target, targetAlias, targetMapping, targetParentMapping, discrimValues, tableGrpName, true, parentJoin);
            }
        }

        return targetTbl;
    }

    @Override
    public SQLTable join(JoinType joinType, SQLTable sourceTable, Table target, String targetAlias, String tableGrpName, BooleanExpression joinCondition, boolean applyToUnions)
    {
        invalidateStatement();

        // Create the SQLTable to join to.
        if (tables == null)
        {
            tables = new HashMap<>();
        }
        if (tableGrpName == null)
        {
            tableGrpName = "Group" + tableGroups.size();
        }
        if (targetAlias == null)
        {
            targetAlias = namer.getAliasForTable(this, target, tableGrpName);
        }
        if (sourceTable == null)
        {
            sourceTable = primaryTable;
        }
        DatastoreIdentifier targetId = rdbmsMgr.getIdentifierFactory().newTableIdentifier(targetAlias);
        SQLTable targetTbl = new SQLTable(this, target, targetId, tableGrpName);
        putSQLTableInGroup(targetTbl, tableGrpName, joinType);

        addJoin(joinType, sourceTable, targetTbl, joinCondition, null);

        if (unions != null && applyToUnions)
        {
            // Apply the join to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.join(joinType, sourceTable, target, targetAlias, tableGrpName, joinCondition, true);
            }
        }

        return targetTbl;
    }

    @Override
    public String removeCrossJoin(SQLTable targetSqlTbl)
    {
        if (joins == null)
        {
            return null;
        }

        Iterator<SQLJoin> joinIter = joins.iterator();
        while (joinIter.hasNext())
        {
            SQLJoin join = joinIter.next();
            if (join.getTargetTable().equals(targetSqlTbl) && join.getType() == JoinType.CROSS_JOIN)
            {
                joinIter.remove();
                requiresJoinReorder = true;
                tables.remove(join.getTargetTable().alias.getName());
                String removedAliasName = join.getTargetTable().alias.getName();

                if (unions != null)
                {
                    // Apply the join removal to all unions
                    Iterator<SelectStatement> unionIter = unions.iterator();
                    while (unionIter.hasNext())
                    {
                        SelectStatement stmt = unionIter.next();
                        stmt.removeCrossJoin(targetSqlTbl);
                    }
                }

                return removedAliasName;
            }
        }

        return null;
    }

    /**
     * Method to add an AND condition to the WHERE clause.
     * @param expr The condition
     * @param applyToUnions whether to apply this and to any UNIONs in the statement
     */
    public void whereAnd(BooleanExpression expr, boolean applyToUnions)
    {
        if (expr instanceof BooleanLiteral && !expr.isParameter() && (Boolean)((BooleanLiteral)expr).getValue())
        {
            // Where condition is "TRUE" so omit
            return;
        }

        super.whereAnd(expr, false);

        if (unions != null && allowUnions && applyToUnions)
        {
            // Apply the where to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.whereAnd(expr, true);
            }
        }
    }

    /**
     * Method to add an OR condition to the WHERE clause.
     * @param expr The condition
     * @param applyToUnions Whether to apply to unions
     */
    public void whereOr(BooleanExpression expr, boolean applyToUnions)
    {
        super.whereOr(expr, false);

        if (unions != null && allowUnions && applyToUnions)
        {
            // Apply the where to all unions
            Iterator<SelectStatement> unionIter = unions.iterator();
            while (unionIter.hasNext())
            {
                SelectStatement stmt = unionIter.next();
                stmt.whereOr(expr, true);
            }
        }
    }
}
