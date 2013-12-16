/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.query;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.scostore.BaseContainerStore;
import org.datanucleus.store.rdbms.scostore.FKArrayStore;
import org.datanucleus.store.rdbms.scostore.FKListStore;
import org.datanucleus.store.rdbms.scostore.FKMapStore;
import org.datanucleus.store.rdbms.scostore.FKSetStore;
import org.datanucleus.store.rdbms.scostore.IteratorStatement;
import org.datanucleus.store.rdbms.scostore.JoinArrayStore;
import org.datanucleus.store.rdbms.scostore.JoinListStore;
import org.datanucleus.store.rdbms.scostore.JoinMapStore;
import org.datanucleus.store.rdbms.scostore.JoinSetStore;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLStatementParameter;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.scostore.Store;

/**
 * Helper class to generate the necessary statement for multi-valued field bulk-fetch.
 */
public class BulkFetchHelper
{
    Query query;

    public BulkFetchHelper(Query q)
    {
        this.query = q;
    }

    /**
     * Convenience method to generate a bulk-fetch statement for the specified multi-valued field of the owning query.
     * @param ec ExecutionContext
     * @param candidateCmd Metadata for the candidate
     * @param parameters Parameters for the query
     * @param mmd Metadata for the multi-valued field
     * @param datastoreCompilation The datastore compilation of the query
     * @return The bulk-fetch statement for retrieving this multi-valued field.
     */
    public IteratorStatement getSQLStatementForContainerFieldBatch(ExecutionContext ec, AbstractClassMetaData candidateCmd, Map parameters, AbstractMemberMetaData mmd,
            RDBMSQueryCompilation datastoreCompilation)
    {
        IteratorStatement iterStmt = null;
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RDBMSStoreManager storeMgr = (RDBMSStoreManager) query.getStoreManager();
        Store backingStore = storeMgr.getBackingStoreForField(clr, mmd, null);
        if (backingStore instanceof JoinSetStore)
        {
            iterStmt = ((JoinSetStore)backingStore).getIteratorStatement(clr, ec.getFetchPlan(), false);
        }
        else if (backingStore instanceof FKSetStore)
        {
            iterStmt = ((FKSetStore)backingStore).getIteratorStatement(clr, ec.getFetchPlan(), false);
        }
        else if (backingStore instanceof JoinListStore)
        {
            iterStmt = ((JoinListStore)backingStore).getIteratorStatement(clr, ec.getFetchPlan(), false, -1, -1);
        }
        else if (backingStore instanceof FKListStore)
        {
            iterStmt = ((FKListStore)backingStore).getIteratorStatement(clr, ec.getFetchPlan(), false, -1, -1);
        }
        else if (backingStore instanceof JoinArrayStore)
        {
            iterStmt = ((JoinArrayStore)backingStore).getIteratorStatement(clr, ec.getFetchPlan(), false);
        }
        else if (backingStore instanceof FKArrayStore)
        {
            iterStmt = ((FKArrayStore)backingStore).getIteratorStatement(clr, ec.getFetchPlan(), false);
        }
        else if (backingStore instanceof JoinMapStore)
        {
            // TODO Implement this
            return null;
        }
        else if (backingStore instanceof FKMapStore)
        {
            // TODO Implement this
            return null;
        }

        if (backingStore instanceof JoinSetStore || backingStore instanceof JoinListStore || backingStore instanceof JoinArrayStore)
        {
            // Generate an iterator query of the form
            // SELECT ELEM_TBL.COL1, ELEM_TBL.COL2, ... FROM JOIN_TBL INNER_JOIN ELEM_TBL WHERE JOIN_TBL.ELEMENT_ID = ELEM_TBL.ID 
            // AND EXISTS (SELECT OWNER_TBL.ID FROM OWNER_TBL WHERE (queryWhereClause) AND JOIN_TBL.OWNER_ID = OWNER_TBL.ID)
            SQLStatement sqlStmt = iterStmt.getSQLStatement();
            JoinTable joinTbl = (JoinTable)sqlStmt.getPrimaryTable().getTable();
            JavaTypeMapping joinOwnerMapping = joinTbl.getOwnerMapping();

            // Generate the EXISTS subquery (based on the JDOQL query)
            SQLStatement existsStmt = RDBMSQueryUtils.getStatementForCandidates(storeMgr, sqlStmt, candidateCmd,
                datastoreCompilation.getResultDefinitionForClass(), ec, query.getCandidateClass(), query.isSubclasses(), query.getResult(), null, null);
            Set<String> options = new HashSet<String>();
            options.add(QueryToSQLMapper.OPTION_SELECT_CANDIDATE_ID_ONLY);
            QueryToSQLMapper sqlMapper = new QueryToSQLMapper(existsStmt, query.getCompilation(), parameters,
                null, null, candidateCmd, query.getFetchPlan(), ec, query.getParsedImports(), options, query.getExtensions());
            sqlMapper.compile();

            // Add EXISTS clause on iterator statement so we can restrict to just the owners in this query
            BooleanExpression existsExpr = new BooleanSubqueryExpression(sqlStmt, "EXISTS", existsStmt);
            sqlStmt.whereAnd(existsExpr, true);

            // Join to outer statement so we restrict to collection elements for the query candidates
            SQLExpression joinTblOwnerExpr = sqlStmt.getRDBMSManager().getSQLExpressionFactory().newExpression(sqlStmt, sqlStmt.getPrimaryTable(), joinOwnerMapping);
            SQLExpression existsOwnerExpr = sqlStmt.getRDBMSManager().getSQLExpressionFactory().newExpression(existsStmt, existsStmt.getPrimaryTable(), 
                existsStmt.getPrimaryTable().getTable().getIdMapping());
            existsStmt.whereAnd(joinTblOwnerExpr.eq(existsOwnerExpr), true);

            // Select the owner candidate so we can separate the collection elements out to their owner
            int[] ownerColIndexes = sqlStmt.select(joinTblOwnerExpr, null);
            StatementMappingIndex ownerMapIdx = new StatementMappingIndex(existsStmt.getPrimaryTable().getTable().getIdMapping());
            ownerMapIdx.setColumnPositions(ownerColIndexes);
            iterStmt.setOwnerMapIndex(ownerMapIdx);
        }
        else if (backingStore instanceof FKSetStore || backingStore instanceof FKListStore || backingStore instanceof FKArrayStore)
        {
            // Generate an iterator query of the form
            // SELECT ELEM_TBL.COL1, ELEM_TBL.COL2, ... FROM ELEM_TBL
            // WHERE EXISTS (SELECT OWNER_TBL.ID FROM OWNER_TBL WHERE (queryWhereClause) AND ELEM_TBL.OWNER_ID = OWNER_TBL.ID)
            SQLStatement sqlStmt = iterStmt.getSQLStatement();

            // Generate the EXISTS subquery (based on the JDOQL query)
            SQLStatement existsStmt = RDBMSQueryUtils.getStatementForCandidates(storeMgr, sqlStmt, candidateCmd,
                datastoreCompilation.getResultDefinitionForClass(), ec, query.getCandidateClass(), query.isSubclasses(), query.getResult(), null, null);
            Set<String> options = new HashSet<String>();
            options.add(QueryToSQLMapper.OPTION_SELECT_CANDIDATE_ID_ONLY);
            QueryToSQLMapper sqlMapper = new QueryToSQLMapper(existsStmt, query.getCompilation(), parameters,
                null, null, candidateCmd, query.getFetchPlan(), ec, query.getParsedImports(), options, query.getExtensions());
            sqlMapper.compile();

            // Add EXISTS clause on iterator statement so we can restrict to just the owners in this query
            BooleanExpression existsExpr = new BooleanSubqueryExpression(sqlStmt, "EXISTS", existsStmt);
            sqlStmt.whereAnd(existsExpr, true);

            // Join to outer statement so we restrict to collection elements for the query candidates
            SQLExpression elemTblOwnerExpr = sqlStmt.getRDBMSManager().getSQLExpressionFactory().newExpression(sqlStmt, sqlStmt.getPrimaryTable(), 
                ((BaseContainerStore) backingStore).getOwnerMapping());
            SQLExpression existsOwnerExpr = sqlStmt.getRDBMSManager().getSQLExpressionFactory().newExpression(existsStmt, existsStmt.getPrimaryTable(), 
                existsStmt.getPrimaryTable().getTable().getIdMapping());
            existsStmt.whereAnd(elemTblOwnerExpr.eq(existsOwnerExpr), true);

            // Select the owner candidate so we can separate the collection elements out to their owner
            int[] ownerColIndexes = sqlStmt.select(elemTblOwnerExpr, null);
            StatementMappingIndex ownerMapIdx = new StatementMappingIndex(existsStmt.getPrimaryTable().getTable().getIdMapping());
            ownerMapIdx.setColumnPositions(ownerColIndexes);
            iterStmt.setOwnerMapIndex(ownerMapIdx);
        }
        else if (backingStore instanceof JoinMapStore)
        {
            // TODO Implement this
        }
        else if (backingStore instanceof FKMapStore)
        {
            // TODO Implement this
        }
        return iterStmt;
    }

    /**
     * Convenience method to apply the passed parameters to the provided bulk-fetch statement.
     * Takes care of applying parameters across any UNIONs of elements.
     * @param ps PreparedStatement
     * @param ec ExecutionContext
     * @param datastoreCompilation The datastore compilation for the query itself
     * @param sqlStmt The bulk-fetch iterator statement
     * @param parameters The map of parameters passed in to the query
     */
    public void applyParametersToStatement(PreparedStatement ps, ExecutionContext ec, RDBMSQueryCompilation datastoreCompilation, SQLStatement sqlStmt, Map parameters)
    {
        Map<Integer, String> stmtParamNameByPosition = null;
        List<SQLStatementParameter> stmtParams = null;
        if (datastoreCompilation.getStatementParameters() != null)
        {
            stmtParams = new ArrayList<SQLStatementParameter>();
            stmtParams.addAll(datastoreCompilation.getStatementParameters());
            stmtParamNameByPosition = new HashMap<Integer, String>();
            stmtParamNameByPosition.putAll(datastoreCompilation.getParameterNameByPosition());
            int numUnions = sqlStmt.getNumberOfUnions();
            int numParams = stmtParamNameByPosition.size();
            for (int i=0;i<numUnions;i++)
            {
                stmtParams.addAll(datastoreCompilation.getStatementParameters());
                Iterator<Map.Entry<Integer, String>> paramEntryIter = datastoreCompilation.getParameterNameByPosition().entrySet().iterator();
                while (paramEntryIter.hasNext())
                {
                    Map.Entry<Integer, String> paramEntry = paramEntryIter.next();
                    stmtParamNameByPosition.put(numParams*(i+1) + paramEntry.getKey(), paramEntry.getValue());
                }
            }

            SQLStatementHelper.applyParametersToStatement(ps, ec, stmtParams, stmtParamNameByPosition, parameters);
        }
    }
}