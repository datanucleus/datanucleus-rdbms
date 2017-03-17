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

import java.util.HashSet;
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
import org.datanucleus.store.rdbms.scostore.FKSetStore;
import org.datanucleus.store.rdbms.scostore.IteratorStatement;
import org.datanucleus.store.rdbms.scostore.JoinArrayStore;
import org.datanucleus.store.rdbms.scostore.JoinListStore;
import org.datanucleus.store.rdbms.scostore.JoinSetStore;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.types.scostore.Store;

/**
 * Bulk-Fetch handler to generate the necessary statement for multi-valued field bulk-fetch using EXISTS subquery semantics.
 * <p>
 * In simple terms if we have a query with resultant SQL like
 * <pre>SELECT COL1, COL2, COL3, ... FROM CANDIDATE_TBL T1 WHERE T1.COL2 = value</pre>
 * 
 * then to retrieve a multi-valued collection field of the candidate class it generates an SQL like
 * <pre>SELECT ELEM.COL1, ELEM.COL2, ... FROM ELEMENT_TBL ELEM WHERE EXISTS (
 * SELECT T1.ID FROM CANDIDATE_TBL T1 WHERE T1.COL2 = value AND ELEM.OWNER_ID = T1.ID)</pre>
 * 
 * Obviously there are differences when using a join-table, or when the elements are embedded into the join-table, but the
 * basic idea is we generate an iterator statement for the elements (just like the backing store normally would) except
 * instead of restricting the statement to just a particular owner, it adds an EXISTS clause with the query as the exists subquery.
 */
public class BulkFetchExistsHandler implements BulkFetchHandler
{
    /**
     * Convenience method to generate a bulk-fetch statement for the specified multi-valued field of the owning query.
     * @param candidateCmd Metadata for the candidate
     * @param parameters Parameters for the query
     * @param mmd Metadata for the multi-valued field
     * @param datastoreCompilation The datastore compilation of the query
     * @param mapperOptions Any options for the query to SQL mapper
     * @return The bulk-fetch statement for retrieving this multi-valued field.
     */
    public IteratorStatement getStatementToBulkFetchField(AbstractClassMetaData candidateCmd, AbstractMemberMetaData mmd, 
            Query query, Map parameters, RDBMSQueryCompilation datastoreCompilation, Set<String> mapperOptions)
    {
        IteratorStatement iterStmt = null;
        ExecutionContext ec = query.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RDBMSStoreManager storeMgr = (RDBMSStoreManager) query.getStoreManager();
        Store backingStore = storeMgr.getBackingStoreForField(clr, mmd, null);

        if (backingStore instanceof JoinSetStore || backingStore instanceof JoinListStore || backingStore instanceof JoinArrayStore)
        {
            // Set/List/array using join-table : Generate an iterator query of the form
            if (backingStore instanceof JoinSetStore)
            {
                iterStmt = ((JoinSetStore)backingStore).getIteratorStatement(ec, ec.getFetchPlan(), false);
            }
            else if (backingStore instanceof JoinListStore)
            {
                iterStmt = ((JoinListStore)backingStore).getIteratorStatement(ec, ec.getFetchPlan(), false, -1, -1);
            }
            else if (backingStore instanceof JoinArrayStore)
            {
                iterStmt = ((JoinArrayStore)backingStore).getIteratorStatement(ec, ec.getFetchPlan(), false);
            }

            // SELECT ELEM_TBL.COL1, ELEM_TBL.COL2, ... FROM JOIN_TBL INNER_JOIN ELEM_TBL WHERE JOIN_TBL.ELEMENT_ID = ELEM_TBL.ID 
            // AND EXISTS (SELECT OWNER_TBL.ID FROM OWNER_TBL WHERE (queryWhereClause) AND JOIN_TBL.OWNER_ID = OWNER_TBL.ID)
            SelectStatement sqlStmt = iterStmt.getSelectStatement();
            JoinTable joinTbl = (JoinTable)sqlStmt.getPrimaryTable().getTable();
            JavaTypeMapping joinOwnerMapping = joinTbl.getOwnerMapping();

            // Generate the EXISTS subquery (based on the JDOQL/JPQL query)
            SelectStatement existsStmt = RDBMSQueryUtils.getStatementForCandidates(storeMgr, sqlStmt, candidateCmd,
                datastoreCompilation.getResultDefinitionForClass(), ec, query.getCandidateClass(), query.isSubclasses(), query.getResult(), null, null, null);
            Set<String> options = new HashSet<>();
            if (mapperOptions != null)
            {
                options.addAll(mapperOptions);
            }
            options.add(QueryToSQLMapper.OPTION_SELECT_CANDIDATE_ID_ONLY);
            QueryToSQLMapper sqlMapper = new QueryToSQLMapper(existsStmt, query.getCompilation(), parameters,
                null, null, candidateCmd, query.isSubclasses(), query.getFetchPlan(), ec, query.getParsedImports(), options, query.getExtensions());
            sqlMapper.compile();

            // Add EXISTS clause on iterator statement so we can restrict to just the owners in this query
            existsStmt.setOrdering(null, null); // ORDER BY in EXISTS is forbidden by some RDBMS
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
            if (backingStore instanceof FKSetStore)
            {
                iterStmt = ((FKSetStore)backingStore).getIteratorStatement(ec, ec.getFetchPlan(), false);
            }
            else if (backingStore instanceof FKListStore)
            {
                iterStmt = ((FKListStore)backingStore).getIteratorStatement(ec, ec.getFetchPlan(), false, -1, -1);
            }
            else if (backingStore instanceof FKArrayStore)
            {
                iterStmt = ((FKArrayStore)backingStore).getIteratorStatement(ec, ec.getFetchPlan(), false);
            }

            // Set/List/array using foreign-key : Generate an iterator query of the form
            // SELECT ELEM_TBL.COL1, ELEM_TBL.COL2, ... FROM ELEM_TBL
            // WHERE EXISTS (SELECT OWNER_TBL.ID FROM OWNER_TBL WHERE (queryWhereClause) AND ELEM_TBL.OWNER_ID = OWNER_TBL.ID)
            SelectStatement sqlStmt = iterStmt.getSelectStatement();

            // Generate the EXISTS subquery (based on the JDOQL/JPQL query)
            SelectStatement existsStmt = RDBMSQueryUtils.getStatementForCandidates(storeMgr, sqlStmt, candidateCmd,
                datastoreCompilation.getResultDefinitionForClass(), ec, query.getCandidateClass(), query.isSubclasses(), query.getResult(), null, null, null);
            Set<String> options = new HashSet<>();
            if (mapperOptions != null)
            {
                options.addAll(mapperOptions);
            }
            options.add(QueryToSQLMapper.OPTION_SELECT_CANDIDATE_ID_ONLY);
            QueryToSQLMapper sqlMapper = new QueryToSQLMapper(existsStmt, query.getCompilation(), parameters,
                null, null, candidateCmd, query.isSubclasses(), query.getFetchPlan(), ec, query.getParsedImports(), options, query.getExtensions());
            sqlMapper.compile();

            // Add EXISTS clause on iterator statement so we can restrict to just the owners in this query
            existsStmt.setOrdering(null, null); // ORDER BY in EXISTS is forbidden by some RDBMS
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

        return iterStmt;
    }
}