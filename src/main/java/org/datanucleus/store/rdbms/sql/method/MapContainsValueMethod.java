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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.ByteExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.EnumExpression;
import org.datanucleus.store.rdbms.sql.expression.InExpression;
import org.datanucleus.store.rdbms.sql.expression.MapExpression;
import org.datanucleus.store.rdbms.sql.expression.MapLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.store.rdbms.sql.expression.UnboundExpression;
import org.datanucleus.store.rdbms.sql.expression.MapLiteral.MapValueLiteral;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Method for evaluating {mapExpr}.containsValue(valueExpr).
 * Returns a BooleanExpression.
 */
public class MapContainsValueMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0 || args.size() > 1)
        {
            throw new NucleusException(Localiser.msg("060016", "containsValue", "MapExpression", 1));
        }

        MapExpression mapExpr = (MapExpression)expr;
        SQLExpression valExpr = args.get(0);

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
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            if (map == null || map.size() == 0)
            {
                return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
            }

            boolean useInExpression = false;
            List<SQLExpression> mapValExprs = lit.getValueLiteral().getValueExpressions();
            if (mapValExprs != null && !mapValExprs.isEmpty())
            {
                // Make sure the the map key(s) are compatible with the keyExpr
                boolean incompatible = true;
                Class elemtype = clr.classForName(valExpr.getJavaTypeMapping().getType());
                Iterator<SQLExpression> mapKeyExprIter = mapValExprs.iterator();
                while (mapKeyExprIter.hasNext())
                {
                    SQLExpression mapKeyExpr = mapKeyExprIter.next();
                    Class mapKeyType = clr.classForName(mapKeyExpr.getJavaTypeMapping().getType());
                    if (valueTypeCompatible(elemtype, mapKeyType))
                    {
                        incompatible = false;
                        break;
                    }
                }
                if (incompatible)
                {
                    // The provided element type isn't assignable to any of the input collection elements!
                    return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
                }

                // Check if we should compare using an "IN (...)" expression
                SQLExpression mapKeyExpr = mapValExprs.get(0);
                if (mapKeyExpr instanceof StringExpression || mapKeyExpr instanceof NumericExpression ||
                    mapKeyExpr instanceof TemporalExpression || mapKeyExpr instanceof CharacterExpression ||
                    mapKeyExpr instanceof ByteExpression || mapKeyExpr instanceof EnumExpression)
                {
                    useInExpression = true;
                }
            }
            if (useInExpression)
            {
                // Return "key IN (val1, val2, ...)"
                SQLExpression[] exprs = (mapValExprs != null ? mapValExprs.toArray(new SQLExpression[mapValExprs.size()]) : null);
                return new InExpression(valExpr, exprs);
            }

            // TODO If valExpr is a parameter and mapExpr is derived from a parameter ?
            MapValueLiteral mapValueLiteral = lit.getValueLiteral();
            BooleanExpression bExpr = null;
            List<SQLExpression> elementExprs = mapValueLiteral.getValueExpressions();
            for (int i=0; i<elementExprs.size(); i++)
            {
                if (bExpr == null)
                {
                    bExpr = (elementExprs.get(i)).eq(valExpr); 
                }
                else
                {
                    bExpr = bExpr.ior((elementExprs.get(i)).eq(valExpr)); 
                }
            }
            if (bExpr != null)
            {
                bExpr.encloseInParentheses();
            }
            return bExpr;
        }

        if (stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.FILTER)
        {
            boolean needsSubquery = getNeedsSubquery();

            // TODO Check if *this* "containsValue" is negated, not any of them (and remove above check)
            if (needsSubquery)
            {
                NucleusLogger.QUERY.debug("map.containsValue on " + mapExpr + "(" + valExpr + ") using SUBQUERY");
                return containsAsSubquery(mapExpr, valExpr);
            }
            NucleusLogger.QUERY.debug("map.containsValue on " + mapExpr + "(" + valExpr + ") using INNERJOIN");
            return containsAsInnerJoin(mapExpr, valExpr);
        }
        return containsAsSubquery(mapExpr, valExpr);
    }

    /**
     * Convenience method to decide if we handle the contains() by using a subquery, or otherwise
     * via an inner join. If there is an OR or a NOT in the query then uses a subquery.
     * @return Whether to use a subquery
     */
    protected boolean getNeedsSubquery()
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
     * Method to return an expression for Map.containsValue using INNER JOIN to the element.
     * This is only for use when there are no "!containsValue" and no "OR" operations.
     * Creates SQL by adding INNER JOIN to the join table (where it exists), and also to the value table
     * adding an AND condition on the value (with value of the valueExpr).
     * Returns a BooleanExpression "TRUE" (since the INNER JOIN will guarantee if the value is
     * contained of not).
     * @param mapExpr Map expression
     * @param valExpr Expression for the value
     * @return Contains expression
     */
    protected SQLExpression containsAsInnerJoin(MapExpression mapExpr, SQLExpression valExpr)
    {
        boolean valIsUnbound = (valExpr instanceof UnboundExpression);
        String varName = null;
        String valAlias = null;
        if (valIsUnbound)
        {
            varName = ((UnboundExpression)valExpr).getVariableName();
            NucleusLogger.QUERY.debug("map.containsValue(" + valExpr + ") binding unbound variable " + varName + " using INNER JOIN");
            // TODO What if the variable is declared as a subtype, handle this see CollectionContainsMethod
        }
        else if (!stmt.getQueryGenerator().hasExplicitJoins())
        {
            JoinType joinType = stmt.getJoinTypeForTable(valExpr.getSQLTable());
            if (joinType == JoinType.CROSS_JOIN)
            {
                // Value is currently joined via CROSS JOIN, so remove it (and use INNER JOIN below)
                valAlias = stmt.removeCrossJoin(valExpr.getSQLTable());
                valIsUnbound = true;
                NucleusLogger.QUERY.debug("map.containsValue(" + valExpr + ") was previously bound as CROSS JOIN but changing to INNER JOIN");
            }

            // TODO If owner is joined via CROSS JOIN and value is already present then remove CROSS JOIN 
            // and join via INNER JOIN
        }

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        MetaDataManager mmgr = storeMgr.getMetaDataManager();
        AbstractMemberMetaData mmd = mapExpr.getJavaTypeMapping().getMemberMetaData();
        AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, mmgr);
        if (mmd.getMap().getMapType() == MapType.MAP_TYPE_JOIN)
        {
            // Map formed in join table - add join to join table, then to value table (if present)
            MapTable mapTbl = (MapTable)storeMgr.getTable(mmd);
            SQLTable joinSqlTbl = stmt.innerJoin(mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping(), mapTbl, null, mapTbl.getOwnerMapping(), null, null);
            if (valCmd != null)
            {
                if (valIsUnbound)
                {
                    DatastoreClass valTbl = storeMgr.getDatastoreClass(valCmd.getFullClassName(), clr);
                    SQLTable valSqlTbl = stmt.innerJoin(joinSqlTbl, mapTbl.getValueMapping(), valTbl, valAlias, valTbl.getIdMapping(), null, null);

                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(stmt, valSqlTbl, valSqlTbl.getTable().getIdMapping());
                    stmt.getQueryGenerator().bindVariable(varName, valCmd, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    SQLExpression valIdExpr = exprFactory.newExpression(stmt, joinSqlTbl, mapTbl.getValueMapping());
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
            else
            {
                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(stmt, joinSqlTbl, mapTbl.getValueMapping());
                    stmt.getQueryGenerator().bindVariable(varName, null, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    SQLExpression valIdExpr = exprFactory.newExpression(stmt, joinSqlTbl, mapTbl.getValueMapping());
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
        {
            // Map formed in value table - add join to value table
            DatastoreClass valTbl = storeMgr.getDatastoreClass(valCmd.getFullClassName(), clr);
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = valTbl.getMemberMapping(valCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = valTbl.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }
            SQLTable valSqlTbl = stmt.innerJoin(mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping(), valTbl, valAlias, ownerMapping, null, null);

            if (valIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                valExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getIdMapping());
                stmt.getQueryGenerator().bindVariable(varName, valCmd, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to value
                SQLExpression valIdExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getIdMapping());
                stmt.whereAnd(valIdExpr.eq(valExpr), true);
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
        {
            // Map formed in key table - add join to key table then to value table
            AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, mmgr);
            DatastoreClass keyTbl = storeMgr.getDatastoreClass(keyCmd.getFullClassName(), clr);
            AbstractMemberMetaData keyValMmd = keyCmd.getMetaDataForMember(mmd.getValueMetaData().getMappedBy());
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = keyTbl.getMemberMapping(keyCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = keyTbl.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }
            SQLTable keySqlTbl = stmt.innerJoin(mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping(), keyTbl, null, ownerMapping, null, null);

            if (valCmd != null)
            {
                DatastoreClass valTbl = storeMgr.getDatastoreClass(valCmd.getFullClassName(), clr);
                SQLTable valSqlTbl = stmt.innerJoin(keySqlTbl, keyTbl.getMemberMapping(keyValMmd), valTbl, valAlias, valTbl.getIdMapping(), null, null);

                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getIdMapping());
                    stmt.getQueryGenerator().bindVariable(varName, valCmd, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    SQLExpression valIdExpr = exprFactory.newExpression(stmt, valSqlTbl, valTbl.getIdMapping());
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
            else
            {
                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(stmt, keySqlTbl, keyTbl.getMemberMapping(keyValMmd));
                    stmt.getQueryGenerator().bindVariable(varName, null, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    SQLExpression valIdExpr = exprFactory.newExpression(stmt, keySqlTbl, keyTbl.getMemberMapping(keyValMmd));
                    stmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
        }

        JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
        return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, true));
    }

    /**
     * Method to return an expression for Map.containsValue using a subquery "EXISTS".
     * This is for use when there are "!contains" or "OR" operations in the filter.
     * Creates the following SQL,
     * <ul>
     * <li><b>Map using join table</b>
     * <pre>
     * SELECT 1 FROM JOIN_TBL A0_SUB 
     * WHERE A0_SUB.JOIN_OWN_ID = A0.ID AND A0_SUB.JOIN_VAL_ID = {valExpr}
     * </pre>
     * </li>
     * <li><b>Map with key stored in value</b>
     * <pre>
     * SELECT 1 FROM VAL_TABLE A0_SUB INNER JOIN KEY_TBL B0 ON ... 
     * WHERE B0.JOIN_OWN_ID = A0.ID AND A0_SUB.ID = {valExpr}
     * </pre>
     * </li>
     * <li><b>Map of value stored in key</b>
     * <pre>
     * SELECT 1 FROM VAL_TABLE A0_SUB
     * WHERE A0_SUB.OWN_ID = A0.ID AND A0_SUB.ID = {valExpr}
     * </pre>
     * </li>
     * </ul>
     * and returns a BooleanSubqueryExpression ("EXISTS (subquery)")
     * @param mapExpr Map expression
     * @param valExpr Expression for the value
     * @return Contains expression
     */
    protected SQLExpression containsAsSubquery(MapExpression mapExpr, SQLExpression valExpr)
    {
        boolean valIsUnbound = (valExpr instanceof UnboundExpression);
        String varName = null;
        if (valIsUnbound)
        {
            varName = ((UnboundExpression)valExpr).getVariableName();
            NucleusLogger.QUERY.debug("map.containsValue binding unbound variable " + varName + " using SUBQUERY");
            // TODO What if the variable is declared as a subtype, handle this see CollectionContainsMethod
        }

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        MetaDataManager mmgr = storeMgr.getMetaDataManager();
        AbstractMemberMetaData mmd = mapExpr.getJavaTypeMapping().getMemberMetaData();
        AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, mmgr);
        MapTable joinTbl = (MapTable)storeMgr.getTable(mmd);
        SQLStatement subStmt = null;
        if (mmd.getMap().getMapType() == MapType.MAP_TYPE_JOIN)
        {
            // JoinTable Map
            if (valCmd == null)
            {
                // Map<?, Non-PC>
                subStmt = new SelectStatement(stmt, storeMgr, joinTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Restrict to map owner
                JavaTypeMapping ownerMapping = ((JoinTable)joinTbl).getOwnerMapping();
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping());
                subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), joinTbl.getValueMapping());
                    stmt.getQueryGenerator().bindVariable(varName, null, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), joinTbl.getValueMapping());
                    subStmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
            else
            {
                // Map<?, PC>
                DatastoreClass valTbl = storeMgr.getDatastoreClass(mmd.getMap().getValueType(), clr);
                subStmt = new SelectStatement(stmt, storeMgr, valTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Join to join table
                SQLTable joinSqlTbl = subStmt.innerJoin(subStmt.getPrimaryTable(), valTbl.getIdMapping(), joinTbl, null, joinTbl.getValueMapping(), null, null);

                // Restrict to map owner
                JavaTypeMapping ownerMapping = joinTbl.getOwnerMapping();
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, joinSqlTbl, ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping());
                subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), valTbl.getIdMapping());
                    stmt.getQueryGenerator().bindVariable(varName, valCmd, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), valTbl.getIdMapping());
                    subStmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
        {
            // Key stored in value table
            DatastoreClass valTbl = storeMgr.getDatastoreClass(mmd.getMap().getValueType(), clr);
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = valTbl.getMemberMapping(valCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = valTbl.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }

            subStmt = new SelectStatement(stmt, storeMgr, valTbl, null, null);
            subStmt.setClassLoaderResolver(clr);
            JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
            subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

            // Restrict to map owner (on value table)
            SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping());
            subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

            if (valIsUnbound)
            {
                // Bind the variable in the QueryGenerator
                valExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), valTbl.getIdMapping());
                stmt.getQueryGenerator().bindVariable(varName, valCmd, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to value
                JavaTypeMapping valMapping = valTbl.getIdMapping();
                SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), valMapping);
                subStmt.whereAnd(valIdExpr.eq(valExpr), true);
            }
        }
        else if (mmd.getMap().getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
        {
            AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, mmgr);
            DatastoreClass keyTbl = storeMgr.getDatastoreClass(mmd.getMap().getKeyType(), clr);
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = keyTbl.getMemberMapping(keyCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = keyTbl.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }

            AbstractMemberMetaData keyValMmd = keyCmd.getMetaDataForMember(mmd.getValueMetaData().getMappedBy());
            if (valCmd == null)
            {
                subStmt = new SelectStatement(stmt, storeMgr, keyTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Restrict to map owner (on key table)
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping());
                subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), keyTbl.getMemberMapping(keyValMmd));
                    stmt.getQueryGenerator().bindVariable(varName, null, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    JavaTypeMapping valMapping = keyTbl.getMemberMapping(keyValMmd);
                    SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), valMapping);
                    subStmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
            else
            {
                DatastoreClass valTbl = storeMgr.getDatastoreClass(mmd.getMap().getValueType(), clr);
                subStmt = new SelectStatement(stmt, storeMgr, valTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Join to key table
                SQLTable keySqlTbl = subStmt.innerJoin(subStmt.getPrimaryTable(), valTbl.getIdMapping(), keyTbl, null, keyTbl.getMemberMapping(keyValMmd), null, null);

                // Restrict to map owner (on key table)
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, keySqlTbl, ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping());
                subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

                if (valIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    valExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), valTbl.getIdMapping());
                    stmt.getQueryGenerator().bindVariable(varName, valCmd, valExpr.getSQLTable(), valExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to value
                    SQLExpression valIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), valTbl.getIdMapping());
                    subStmt.whereAnd(valIdExpr.eq(valExpr), true);
                }
            }
        }

        return new BooleanSubqueryExpression(stmt, "EXISTS", subStmt);
    }

    protected boolean valueTypeCompatible(Class valType, Class mapValType)
    {
        if (!valType.isPrimitive() && mapValType.isPrimitive() && !mapValType.isAssignableFrom(valType) && !valType.isAssignableFrom(mapValType))
        {
            return false;
        }
        else if (valType.isPrimitive())
        {
            if (valType == boolean.class && mapValType == Boolean.class)
            {
                return true;
            }
            else if (valType == byte.class && mapValType == Byte.class)
            {
                return true;
            }
            else if (valType == char.class && mapValType == Character.class)
            {
                return true;
            }
            else if (valType == double.class && mapValType == Double.class)
            {
                return true;
            }
            else if (valType == float.class && mapValType == Float.class)
            {
                return true;
            }
            else if (valType == int.class && mapValType == Integer.class)
            {
                return true;
            }
            else if (valType == long.class && mapValType == Long.class)
            {
                return true;
            }
            else if (valType == short.class && mapValType == Short.class)
            {
                return true;
            }
            return false;
        }

        return true;
    }
}