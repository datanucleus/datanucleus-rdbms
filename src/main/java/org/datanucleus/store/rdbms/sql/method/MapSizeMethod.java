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
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.MapLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {mapExpr}.size().
 * Returns a NumericExpression. Equates to a sub-query on the table for the map
 * returning the COUNT(*) of the number of entries. Something like
 * <PRE>
 * SELECT COUNT(*) FROM MAPTABLE A0_SUB WHERE A0_SUB.OWNER_ID_OID = A0.OWNER_ID
 * </PRE>
 * where A0 is the candidate table in the outer query, and MAPTABLE is the join table (if using 
 * join map) or the key/value table (if using FK map).
 */
public class MapSizeMethod implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (args != null && args.size() > 0)
        {
            throw new NucleusException(Localiser.msg("060015", "size", "MapExpression"));
        }

        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        if (expr instanceof MapLiteral)
        {
            // Just return the map length since we have the value
            Map map = (Map)((MapLiteral)expr).getValue();
            return exprFactory.newLiteral(stmt, exprFactory.getMappingForType(int.class, false), Integer.valueOf(map.size()));
        }

        AbstractMemberMetaData ownerMmd = expr.getJavaTypeMapping().getMemberMetaData();
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        ClassLoaderResolver clr = stmt.getQueryGenerator().getClassLoaderResolver();

        // TODO Allow for interface keys/values, etc
        JavaTypeMapping ownerMapping = null;
        Table mapTbl = null;
        if (ownerMmd.getMap().getMapType() == MapType.MAP_TYPE_JOIN)
        {
            // JoinTable
            mapTbl = storeMgr.getTable(ownerMmd);
            ownerMapping = ((JoinTable)mapTbl).getOwnerMapping();
        }
        else if (ownerMmd.getMap().getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
        {
            // ForeignKey from value table to key
            AbstractClassMetaData valueCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(ownerMmd.getMap().getValueType(), clr);
            mapTbl = storeMgr.getDatastoreClass(ownerMmd.getMap().getValueType(), clr);
            if (ownerMmd.getMappedBy() != null)
            {
                ownerMapping = mapTbl.getMemberMapping(valueCmd.getMetaDataForMember(ownerMmd.getMappedBy()));
            }
            else
            {
                ownerMapping = ((DatastoreClass)mapTbl).getExternalMapping(ownerMmd, MappingType.EXTERNAL_FK);
            }
        }
        else if (ownerMmd.getMap().getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
        {
            // ForeignKey from key table to value
            AbstractClassMetaData keyCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(ownerMmd.getMap().getKeyType(), clr);
            mapTbl = storeMgr.getDatastoreClass(ownerMmd.getMap().getKeyType(), clr);
            if (ownerMmd.getMappedBy() != null)
            {
                ownerMapping = mapTbl.getMemberMapping(keyCmd.getMetaDataForMember(ownerMmd.getMappedBy()));
            }
            else
            {
                ownerMapping = ((DatastoreClass)mapTbl).getExternalMapping(ownerMmd, MappingType.EXTERNAL_FK);
            }
        }
        else
        {
            throw new NucleusException("Invalid map for " + expr + " in size() call");
        }

        SelectStatement subStmt = new SelectStatement(stmt, storeMgr, mapTbl, null, null);
        subStmt.setClassLoaderResolver(clr);
        JavaTypeMapping mapping = storeMgr.getMappingManager().getMappingWithDatastoreMapping(String.class, false, false, clr);
        SQLExpression countExpr = exprFactory.newLiteral(subStmt, mapping, "COUNT(*)");
        ((StringLiteral)countExpr).generateStatementWithoutQuotes();
        subStmt.select(countExpr, null);

        SQLExpression elementOwnerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
        SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, expr.getSQLTable(), expr.getSQLTable().getTable().getIdMapping());
        subStmt.whereAnd(elementOwnerExpr.eq(ownerIdExpr), true);

        JavaTypeMapping subqMapping = exprFactory.getMappingForType(Integer.class, false);
        SQLExpression subqExpr = new NumericSubqueryExpression(stmt, subStmt);
        subqExpr.setJavaTypeMapping(subqMapping);
        return subqExpr;
    }
}