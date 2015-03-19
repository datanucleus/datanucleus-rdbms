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
package org.datanucleus.store.rdbms.sql.method;

import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MapMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.expression.MapExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SubqueryExpression;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating KEY({mapExpr}).
 * Returns an ObjectExpression representing the key mapping.
 */
public class MapKeyMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args != null && args.size() > 0)
        {
            throw new NucleusException(Localiser.msg("060016", "mapKey", "MapExpression", 0));
        }

        MapExpression mapExpr = (MapExpression)expr;

        // TODO There are situations where we may want to use subquery, but if the query has VALUE(map).field then this makes it harder to handle
        /*if (stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.RESULT ||
            stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.HAVING)
        {
            return getAsSubquery(mapExpr);
        }*/

        return getAsInnerJoin(mapExpr);
    }

    /**
     * Implementation of KEY(mapExpr) using an inner join to the table representing the map, and returning an expression for the key.
     * @param mapExpr The map expression
     * @return The value expression
     */
    protected SQLExpression getAsInnerJoin(MapExpression mapExpr)
    {
        JavaTypeMapping m = mapExpr.getJavaTypeMapping();
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        MetaDataManager mmgr = storeMgr.getMetaDataManager();
        AbstractMemberMetaData mmd = m.getMemberMetaData();
        if (mmd != null)
        {
            MapMetaData mapmd = mmd.getMap();
            SQLTable mapSqlTbl = mapExpr.getSQLTable();

            // Set alias of "map" table from MapExpression in case it was defined in FROM clause
            String mapJoinAlias = mapExpr.getAliasForMapTable();
            if (mapJoinAlias == null)
            {
                mapJoinAlias = (mapExpr.getSQLTable().getAlias().toString() + "_" + mmd.getName()).toUpperCase();
            }

            if (mapmd.getMapType() == MapType.MAP_TYPE_JOIN)
            {
                // Add join to join table
                MapTable joinTbl = (MapTable)storeMgr.getTable(mmd);
                SQLTable joinSqlTbl = stmt.innerJoin(mapSqlTbl, mapSqlTbl.getTable().getIdMapping(), joinTbl, mapJoinAlias, joinTbl.getOwnerMapping(), null, null);

                // Return key expression
                if (mapmd.getKeyClassMetaData(clr, mmgr) != null)
                {
                    // Persistable key so join to its table
                    DatastoreClass keyTable = storeMgr.getDatastoreClass(mapmd.getKeyType(), clr);
                    SQLTable keySqlTbl = stmt.innerJoin(joinSqlTbl, joinTbl.getKeyMapping(), keyTable, null, keyTable.getIdMapping(), null, null);

                    return exprFactory.newExpression(stmt, keySqlTbl, keyTable.getIdMapping());
                }

                return exprFactory.newExpression(stmt, joinSqlTbl, joinTbl.getKeyMapping());
            }
            else if (mapmd.getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
            {
                // Key stored in value table, so join to value table
                DatastoreClass valTable = storeMgr.getDatastoreClass(mapmd.getValueType(), clr);
                AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, mmgr);
                JavaTypeMapping mapTblOwnerMapping;
                if (mmd.getMappedBy() != null)
                {
                    mapTblOwnerMapping = valTable.getMemberMapping(valCmd.getMetaDataForMember(mmd.getMappedBy()));
                }
                else
                {
                    mapTblOwnerMapping = valTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                }
                SQLTable valSqlTbl = stmt.innerJoin(mapSqlTbl, mapSqlTbl.getTable().getIdMapping(), valTable, mapJoinAlias, mapTblOwnerMapping, null, null);

                // Return key expression
                AbstractMemberMetaData valKeyMmd = mapmd.getValueClassMetaData(clr, mmgr).getMetaDataForMember(mmd.getKeyMetaData().getMappedBy());
                JavaTypeMapping keyMapping = valTable.getMemberMapping(valKeyMmd);
                return exprFactory.newExpression(stmt, valSqlTbl, keyMapping);
            }
            else if (mapmd.getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
            {
                // Value stored in key table, so join to key table
                DatastoreClass keyTable = storeMgr.getDatastoreClass(mapmd.getKeyType(), clr);
                AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, mmgr);
                JavaTypeMapping mapTblOwnerMapping;
                if (mmd.getMappedBy() != null)
                {
                    mapTblOwnerMapping = keyTable.getMemberMapping(keyCmd.getMetaDataForMember(mmd.getMappedBy()));
                }
                else
                {
                    mapTblOwnerMapping = keyTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                }
                SQLTable keySqlTbl = stmt.innerJoin(mapSqlTbl, mapSqlTbl.getTable().getIdMapping(), keyTable, mapJoinAlias, mapTblOwnerMapping, null, null);

                // Return key expression
                return exprFactory.newExpression(stmt, keySqlTbl, keyTable.getIdMapping());
            }
        }
        throw new NucleusException("KEY(map) for the filter is not supported for " + mapExpr + ". Why not contribute support for it?");
    }

    /**
     * Implementation of KEY(mapExpr) using a subquery on the table representing the map.
     * @param mapExpr The map expression
     * @return The value expression
     */
    protected SQLExpression getAsSubquery(MapExpression mapExpr)
    {
        AbstractMemberMetaData mmd = mapExpr.getJavaTypeMapping().getMemberMetaData();
        MapMetaData mapmd = mmd.getMap();
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        MetaDataManager mmgr = storeMgr.getMetaDataManager();

        JavaTypeMapping ownerMapping = null;
        JavaTypeMapping keyMapping = null;
        Table mapTbl = null;
        if (mapmd.getMapType() == MapType.MAP_TYPE_JOIN)
        {
            // JoinTable
            mapTbl = storeMgr.getTable(mmd);
            ownerMapping = ((MapTable)mapTbl).getOwnerMapping();
            keyMapping = ((MapTable)mapTbl).getKeyMapping();
        }
        else if (mapmd.getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
        {
            // ForeignKey from value table to key
            AbstractClassMetaData valCmd = mapmd.getValueClassMetaData(clr, mmgr);
            mapTbl = storeMgr.getDatastoreClass(mmd.getMap().getValueType(), clr);
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = mapTbl.getMemberMapping(valCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = ((DatastoreClass)mapTbl).getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }
            String keyFieldName = mmd.getKeyMetaData().getMappedBy();
            AbstractMemberMetaData valKeyMmd = valCmd.getMetaDataForMember(keyFieldName);
            keyMapping = mapTbl.getMemberMapping(valKeyMmd);
        }
        else if (mapmd.getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
        {
            // ForeignKey from key table to value
            AbstractClassMetaData keyCmd = mapmd.getKeyClassMetaData(clr, mmgr);
            mapTbl = storeMgr.getDatastoreClass(mmd.getMap().getKeyType(), clr);
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = mapTbl.getMemberMapping(keyCmd.getMetaDataForMember(mmd.getMappedBy()));
            }
            else
            {
                ownerMapping = ((DatastoreClass)mapTbl).getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }
            keyMapping = mapTbl.getIdMapping();
        }
        else
        {
            throw new NucleusException("Invalid map for " + mapExpr + " in get() call");
        }

        SQLStatement subStmt = new SQLStatement(stmt, storeMgr, mapTbl, null, null);
        subStmt.setClassLoaderResolver(clr);
        SQLExpression keyExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), keyMapping);
        subStmt.select(keyExpr, null);

        // Link to primary statement
        SQLExpression elementOwnerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
        SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, mapExpr.getSQLTable(), mapExpr.getSQLTable().getTable().getIdMapping());
        subStmt.whereAnd(elementOwnerExpr.eq(ownerIdExpr), true);

        SubqueryExpression subExpr = new SubqueryExpression(stmt, subStmt);
        subExpr.setJavaTypeMapping(keyMapping);
        return subExpr;
    }
}