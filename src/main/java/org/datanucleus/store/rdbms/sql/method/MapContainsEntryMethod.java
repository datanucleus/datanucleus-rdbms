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
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.BooleanLiteral;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.MapExpression;
import org.datanucleus.store.rdbms.sql.expression.MapLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.UnboundExpression;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Method for evaluating {mapExpr}.containsEntry(keyExpr, valueExpr).
 * Returns a BooleanExpression.
 */
public class MapContainsEntryMethod implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() != 2)
        {
            throw new NucleusException(Localiser.msg("060016", "containsValue", "MapExpression", 2));
        }

        MapExpression mapExpr = (MapExpression)expr;
        SQLExpression keyExpr = args.get(0);
        SQLExpression valExpr = args.get(1);

        if (keyExpr.isParameter())
        {
            // Key is a parameter so make sure its type is set
            AbstractMemberMetaData mmd = mapExpr.getJavaTypeMapping().getMemberMetaData();
            if (mmd != null && mmd.getMap() != null)
            {
                Class keyCls = stmt.getQueryGenerator().getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
                stmt.getQueryGenerator().bindParameter(keyExpr.getParameterName(), keyCls);
            }
        }
        if (valExpr.isParameter())
        {
            // Value is a parameter so make sure its type is set
            AbstractMemberMetaData mmd = mapExpr.getJavaTypeMapping().getMemberMetaData();
            if (mmd != null && mmd.getMap() != null)
            {
                Class valCls = stmt.getQueryGenerator().getClassLoaderResolver().classForName(mmd.getMap().getValueType());
                stmt.getQueryGenerator().bindParameter(valExpr.getParameterName(), valCls);
            }
        }

        if (mapExpr instanceof MapLiteral)
        {
            MapLiteral lit = (MapLiteral)mapExpr;
            Map map = (Map)lit.getValue();
            if (map == null || map.size() == 0)
            {
                return new BooleanLiteral(stmt, expr.getJavaTypeMapping(), Boolean.FALSE);
            }
            // TODO Handle this
            return lit.getValueLiteral().invoke("contains", args);
        }

        if (stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.FILTER)
        {
            boolean needsSubquery = getNeedsSubquery(stmt);

            // TODO Check if *this* "containsEntry" is negated, not any of them (and remove above check)
            if (needsSubquery)
            {
                NucleusLogger.QUERY.debug("MapContainsEntry on " + mapExpr + "(" + keyExpr + "," + valExpr + ") using SUBQUERY");
                return containsAsSubquery(stmt, mapExpr, keyExpr, valExpr);
            }
            NucleusLogger.QUERY.debug("MapContainsEntry on " + mapExpr + "(" + keyExpr + "," + valExpr + ") using INNERJOIN");
            return containsAsInnerJoin(stmt, mapExpr, keyExpr, valExpr);
        }
        return containsAsSubquery(stmt, mapExpr, keyExpr, valExpr);
    }

    /**
     * Convenience method to decide if we handle the contains() by using a subquery, or otherwise
     * via an inner join. If there is an OR or a NOT in the query then uses a subquery.
     * @param stmt SQLStatement
     * @return Whether to use a subquery
     */
    protected boolean getNeedsSubquery(SQLStatement stmt)
    {
        // TODO Check if *this* "contains" is negated, not just any of them (and remove above check)
        boolean needsSubquery = false;
        Boolean hasOR = (Boolean)stmt.getQueryGenerator().getProperty("Filter.OR");
        if (hasOR != null && hasOR.booleanValue())
        {
            needsSubquery = true;
        }
        Boolean hasNOT = (Boolean)stmt.getQueryGenerator().getProperty("Filter.NOT");
        if (hasNOT != null && hasNOT.booleanValue())
        {
            needsSubquery = true;
        }
        return needsSubquery;
    }

    /**
     * Method to return an expression for Map.containsEntry using INNER JOIN to the element.
     * This is only for use when there are no "!containsEntry" and no "OR" operations.
     * Creates SQL by adding INNER JOIN to the join table (where it exists), and also to the value table
     * adding an AND condition on the value (with value of the valueExpr).
     * Returns a BooleanExpression "TRUE" (since the INNER JOIN will guarantee if the entry is
     * contained of not).
     * @param stmt SQLStatement
     * @param mapExpr Map expression
     * @param keyExpr Expression for the key
     * @param valExpr Expression for the value
     * @return Contains expression
     */
    protected SQLExpression containsAsInnerJoin(SQLStatement stmt, MapExpression mapExpr, SQLExpression keyExpr, SQLExpression valExpr)
    {
        boolean keyIsUnbound = (keyExpr instanceof UnboundExpression);
        String keyVarName = null;
        if (keyIsUnbound)
        {
            keyVarName = ((UnboundExpression)keyExpr).getVariableName();
            NucleusLogger.QUERY.debug(">> Map.containsEntry binding unbound variable " + keyVarName + " using INNER JOIN");
            // TODO What if the variable is declared as a subtype, handle this see CollectionContainsMethod
        }
        boolean valIsUnbound = (valExpr instanceof UnboundExpression);
        String valVarName = null;
        if (valIsUnbound)
        {
            valVarName = ((UnboundExpression)valExpr).getVariableName();
            NucleusLogger.QUERY.debug(">> Map.containsEntry binding unbound variable " + valVarName + " using INNER JOIN");
            // TODO What if the variable is declared as a subtype, handle this see CollectionContainsMethod
        }

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        ClassLoaderResolver clr = stmt.getQueryGenerator().getClassLoaderResolver();
        AbstractMemberMetaData mmd = mapExpr.getJavaTypeMapping().getMemberMetaData();
        AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr);
        AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr);
        if (mmd.getMap().getMapType() == MapType.MAP_TYPE_JOIN)
        {
            // Map formed in join table - add join to join table, then to key/value tables (if present)
            MapTable mapTbl = (MapTable)storeMgr.getTable(mmd);
            SQLTable joinSqlTbl = stmt.join(JoinType.INNER_JOIN, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping(), 
                mapTbl, null, mapTbl.getOwnerMapping(), null, null);
            if (valCmd != null)
            {
                DatastoreClass valTbl = storeMgr.getDatastoreClass(valCmd.getFullClassName(), clr);
                SQLTable valSqlTbl = stmt.join(JoinType.INNER_JOIN, joinSqlTbl, mapTbl.getValueMapping(), valTbl, null, valTbl.getIdMapping(), null, null);

                SQLExpression valIdExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getIdMapping());
                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(valVarName, valCmd, valIdExpr.getSQLTable(), valIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
            else
            {
                SQLExpression valIdExpr = exprFactory.newExpression(stmt, joinSqlTbl, mapTbl.getValueMapping());
                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(valVarName, null, valIdExpr.getSQLTable(), valIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }

            if (keyCmd != null)
            {
                DatastoreClass keyTbl = storeMgr.getDatastoreClass(keyCmd.getFullClassName(), clr);
                SQLTable keySqlTbl = stmt.join(JoinType.INNER_JOIN, joinSqlTbl, mapTbl.getKeyMapping(), keyTbl, null, keyTbl.getIdMapping(), null, null);

                SQLExpression keyIdExpr = exprFactory.newExpression(stmt, keySqlTbl, keyTbl.getIdMapping());
                if (keyIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(keyVarName, keyCmd, keyIdExpr.getSQLTable(), 
                        keyIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to key
                    stmt.whereAnd(keyIdExpr.eq(keyExpr), true);
                }
            }
            else
            {
                SQLExpression keyIdExpr = exprFactory.newExpression(stmt, joinSqlTbl, mapTbl.getKeyMapping());
                if (keyIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(keyVarName, null, keyIdExpr.getSQLTable(), keyIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to key
                    stmt.whereAnd(keyIdExpr.eq(keyExpr), true);
                }
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
        {
            // Map formed in value table - add join to value table, then to key table (if present)
            DatastoreClass valTbl = storeMgr.getDatastoreClass(valCmd.getFullClassName(), clr);
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = valTbl.getMemberMapping(valCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = valTbl.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
            }
            SQLTable valSqlTbl = stmt.join(JoinType.INNER_JOIN, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping(), valTbl, null, ownerMapping, null, null);

            SQLExpression valIdExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getIdMapping());
            if (valIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(valVarName, valCmd, valIdExpr.getSQLTable(), valIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to value
                stmt.whereAnd(valIdExpr.eq(valExpr), true);
            }

            if (keyCmd != null)
            {
                // Add inner join to key table
                AbstractMemberMetaData valKeyMmd = valCmd.getMetaDataForMember(mmd.getKeyMetaData().getMappedBy());
                DatastoreClass keyTbl = storeMgr.getDatastoreClass(keyCmd.getFullClassName(), clr);
                SQLTable keySqlTbl = stmt.join(JoinType.INNER_JOIN, valSqlTbl, valTbl.getMemberMapping(valKeyMmd), keyTbl, null, keyTbl.getIdMapping(), null, null);

                SQLExpression keyIdExpr = exprFactory.newExpression(stmt, keySqlTbl, keyTbl.getIdMapping());
                if (keyIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(keyVarName, keyCmd, keyIdExpr.getSQLTable(), keyIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restriction to key
                    stmt.whereAnd(keyIdExpr.eq(keyExpr), true);
                }
            }
            else
            {
                AbstractMemberMetaData valKeyMmd = valCmd.getMetaDataForMember(mmd.getKeyMetaData().getMappedBy());
                SQLExpression keyIdExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getMemberMapping(valKeyMmd));
                if (keyIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(keyVarName, keyCmd, keyIdExpr.getSQLTable(), keyIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restriction to key
                    stmt.whereAnd(keyIdExpr.eq(keyExpr), true);
                }
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
        {
            // Map formed in key table - add join to key table then to value table (if present)
            DatastoreClass keyTbl = storeMgr.getDatastoreClass(keyCmd.getFullClassName(), clr);
            AbstractMemberMetaData keyValMmd = keyCmd.getMetaDataForMember(mmd.getValueMetaData().getMappedBy());
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = keyTbl.getMemberMapping(keyCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = keyTbl.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
            }
            SQLTable keySqlTbl = stmt.join(JoinType.INNER_JOIN, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping(), keyTbl, null, ownerMapping, null, null);

            SQLExpression keyIdExpr = exprFactory.newExpression(stmt, keySqlTbl, keyTbl.getIdMapping());
            if (keyIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(keyVarName, keyCmd, keyIdExpr.getSQLTable(), keyIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to key
                stmt.whereAnd(keyIdExpr.eq(keyExpr), true);
            }

            if (valCmd != null)
            {
                // Add inner join to value table
                DatastoreClass valTbl = storeMgr.getDatastoreClass(valCmd.getFullClassName(), clr);
                SQLTable valSqlTbl = stmt.join(JoinType.INNER_JOIN, keySqlTbl, keyTbl.getMemberMapping(keyValMmd), valTbl, null, valTbl.getIdMapping(), null, null);

                SQLExpression valIdExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getIdMapping());
                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(valVarName, valCmd, valIdExpr.getSQLTable(), valIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
            else
            {
                SQLExpression valIdExpr = exprFactory.newExpression(stmt, keySqlTbl, keyTbl.getMemberMapping(keyValMmd));
                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(valVarName, null, valIdExpr.getSQLTable(), valIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
        }

        JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
        return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, true));
    }

    /**
     * Method to return an expression for Map.containsEntry using a subquery "EXISTS".
     * This is for use when there are "!contains" or "OR" operations in the filter.
     * Creates the following SQL,
     * <ul>
     * <li><b>Map using join table</b>
     * <pre>
     * SELECT 1 FROM JOIN_TBL A0_SUB 
     * WHERE A0_SUB.JOIN_OWN_ID = A0.ID
     * AND A0_SUB.JOIN_VAL_ID = {valExpr}
     * AND Ao_SUB.JOIN_KEY_ID = {keyExpr}
     * </pre>
     * </li>
     * <li><b>Map with key stored in value</b>
     * <pre>
     * SELECT 1 FROM VAL_TABLE A0_SUB
     * WHERE B0.JOIN_OWN_ID = A0.ID 
     * AND A0_SUB.ID = {valExpr}
     * AND A0_SUB.KEY_ID = {keyExpr}
     * </pre>
     * </li>
     * <li><b>Map of value stored in key</b>
     * <pre>
     * SELECT 1 FROM KEY_TABLE A0_SUB
     * WHERE A0_SUB.OWN_ID = A0.ID 
     * AND A0_SUB.VAL_ID = {valExpr}
     * AND A0_SUB.ID = {keyExpr}
     * </pre>
     * </li>
     * </ul>
     * and returns a BooleanSubqueryExpression ("EXISTS (subquery)")
     * @param stmt SQLStatement
     * @param mapExpr Map expression
     * @param keyExpr Expression for the key
     * @param valExpr Expression for the value
     * @return Contains expression
     */
    protected SQLExpression containsAsSubquery(SQLStatement stmt, MapExpression mapExpr, SQLExpression keyExpr, SQLExpression valExpr)
    {
        boolean keyIsUnbound = (keyExpr instanceof UnboundExpression);
        String keyVarName = null;
        if (keyIsUnbound)
        {
            keyVarName = ((UnboundExpression)keyExpr).getVariableName();
            NucleusLogger.QUERY.debug(">> Map.containsEntry binding unbound variable " + keyVarName +
                " using SUBQUERY");
            // TODO What if the variable is declared as a subtype, handle this see CollectionContainsMethod
        }
        boolean valIsUnbound = (valExpr instanceof UnboundExpression);
        String valVarName = null;
        if (valIsUnbound)
        {
            valVarName = ((UnboundExpression)valExpr).getVariableName();
            NucleusLogger.QUERY.debug(">> Map.containsEntry binding unbound variable " + valVarName +
                " using SUBQUERY");
            // TODO What if the variable is declared as a subtype, handle this see CollectionContainsMethod
        }

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        ClassLoaderResolver clr = stmt.getQueryGenerator().getClassLoaderResolver();
        AbstractMemberMetaData mmd = mapExpr.getJavaTypeMapping().getMemberMetaData();
        AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr);
        AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr);
        MapTable joinTbl = (MapTable)storeMgr.getTable(mmd);
        SelectStatement subStmt = null;
        if (mmd.getMap().getMapType() == MapType.MAP_TYPE_JOIN)
        {
            // JoinTable Map
            subStmt = new SelectStatement(stmt, storeMgr, joinTbl, null, null);
            subStmt.setClassLoaderResolver(clr);
            JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
            subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

            // Restrict to collection owner
            JavaTypeMapping ownerMapping = ((JoinTable)joinTbl).getOwnerMapping();
            SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(),
                mapExpr.getSQLTable().getTable().getIdMapping());
            subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

            SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(),
                joinTbl.getValueMapping());
            if (valIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(valVarName, valCmd, valIdExpr.getSQLTable(), 
                    valIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to value TODO Add join to valueTbl if present
                subStmt.whereAnd(valIdExpr.eq(valExpr), true);
            }

            SQLExpression keyIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(),
                joinTbl.getKeyMapping());
            if (keyIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(keyVarName, keyCmd, keyIdExpr.getSQLTable(), 
                    keyIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to key TODO Add join to keyTbl if present
                subStmt.whereAnd(keyIdExpr.eq(keyExpr), true);
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
        {
            // Key stored in value table
            DatastoreClass valTbl = storeMgr.getDatastoreClass(mmd.getMap().getValueType(), clr);
            AbstractMemberMetaData valKeyMmd =
                valCmd.getMetaDataForMember(mmd.getKeyMetaData().getMappedBy());

            subStmt = new SelectStatement(stmt, storeMgr, valTbl, null, null);
            subStmt.setClassLoaderResolver(clr);
            JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
            subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

            // Restrict to map owner (on value table)
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = valTbl.getMemberMapping(valCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = valTbl.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
            }
            SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(),
                mapExpr.getSQLTable().getTable().getIdMapping());
            subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

            SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(),
                valTbl.getIdMapping());
            if (valIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(valVarName, valCmd, valIdExpr.getSQLTable(), 
                    valIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to value
                subStmt.whereAnd(valIdExpr.eq(valExpr), true);
            }

            SQLExpression keyIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(),
                valTbl.getMemberMapping(valKeyMmd));
            if (keyIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(keyVarName, keyCmd, keyIdExpr.getSQLTable(), 
                    keyIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to key TODO Add join to keyTbl if present
                subStmt.whereAnd(keyIdExpr.eq(keyExpr), true);
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
        {
            DatastoreClass keyTbl = storeMgr.getDatastoreClass(mmd.getMap().getKeyType(), clr);
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = keyTbl.getMemberMapping(keyCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = keyTbl.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
            }

            AbstractMemberMetaData keyValMmd =
                keyCmd.getMetaDataForMember(mmd.getValueMetaData().getMappedBy());

            subStmt = new SelectStatement(stmt, storeMgr, keyTbl, null, null);
            subStmt.setClassLoaderResolver(clr);
            JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
            subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

            // Restrict to map owner (on key table)
            SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(),
                mapExpr.getSQLTable().getTable().getIdMapping());
            subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

            SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), 
                keyTbl.getMemberMapping(keyValMmd));
            if (valIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(valVarName, valCmd, valIdExpr.getSQLTable(), 
                    valIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to value TODO Add join to valTbl if present
                subStmt.whereAnd(valIdExpr.eq(valExpr), true);
            }

            JavaTypeMapping keyMapping = keyTbl.getIdMapping();
            SQLExpression keyIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), keyMapping);
            if (keyIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(keyVarName, keyCmd, keyIdExpr.getSQLTable(), 
                    keyIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to key
                subStmt.whereAnd(keyIdExpr.eq(keyExpr), true);
            }
        }

        return new BooleanSubqueryExpression(stmt, "EXISTS", subStmt);
    }
}