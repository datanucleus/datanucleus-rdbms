/**********************************************************************
Copyright (c) 2017 Andy Jefferson and others. All rights reserved.
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

import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.scostore.FKArrayStore;
import org.datanucleus.store.rdbms.scostore.FKListStore;
import org.datanucleus.store.rdbms.scostore.FKSetStore;
import org.datanucleus.store.rdbms.scostore.IteratorStatement;
import org.datanucleus.store.rdbms.scostore.JoinArrayStore;
import org.datanucleus.store.rdbms.scostore.JoinListStore;
import org.datanucleus.store.rdbms.scostore.JoinSetStore;
import org.datanucleus.store.types.scostore.Store;

/**
 * Helper class to generate the necessary statement for multi-valued field bulk-fetch, using JOIN semantics.
 * <p>
 * In simple terms if we have a query with resultant SQL like
 * <pre>SELECT COL1, COL2, COL3, ... FROM CANDIDATE_TBL T1 WHERE T1.COL2 = value</pre>
 * 
 * then to retrieve a multi-valued collection field of the candidate class it generates an SQL like
 * <pre>SELECT ELEM.COL1, ELEM.COL2, ... FROM CANDIDATE_TBL T1, ELEMENT_TBL ELEM WHERE ELEM.CAND_ID = T1.ID AND T1.COL2 = value</pre>
 * 
 * Obviously there are differences when using a join-table, or when the elements are embedded into the join-table, but the basic idea is 
 * we generate an iterator statement for the elements (just like the backing store normally would) except instead of restricting the statement 
 * to just a particular owner, it adds a JOIN, restricting to the candidates implied by the query
 * </p>
 * <b>This is work-in-progress</b>
 */
public class BulkFetchJoinHandler implements BulkFetchHandler
{
    /**
     * Convenience method to generate a bulk-fetch statement for the specified multi-valued field of the owning query.
     * @param candidateCmd Metadata for the candidate
     * @param mmd Metadata for the member we are bulk-fetching the value(s) for
     * @param query The query
     * @param parameters Parameters for the query
     * @param datastoreCompilation The datastore compilation of the query
     * @param mapperOptions Any mapper options for query generation
     * @return The statement to use for bulk fetching, together with mappings for extracting the results of the elements
     */
    public IteratorStatement getStatementToBulkFetchField(AbstractClassMetaData candidateCmd, AbstractMemberMetaData mmd,
            Query query, Map parameters, RDBMSQueryCompilation datastoreCompilation, Set<String> mapperOptions)
    {
        ExecutionContext ec = query.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RDBMSStoreManager storeMgr = (RDBMSStoreManager) query.getStoreManager();
        Store backingStore = storeMgr.getBackingStoreForField(clr, mmd, null);

        if (backingStore instanceof JoinSetStore || backingStore instanceof JoinListStore || backingStore instanceof JoinArrayStore)
        {
            // Set/List/array using join-table : Generate an iterator query of the form
            // SELECT ELEM_TBL.COL1, ELEM_TBL.COL2, ... FROM CANDIDATE_TBL T1 INNER JOIN JOIN_TBL T2 ON T2.OWNER_ID = T1.ID INNER_JOIN ELEM_TBL T3 ON T3.ID = T2.ELEM_ID
            // WHERE (queryWhereClause)

            // TODO Start from the original query, and remove any grouping, having, ordering etc, and join to join table + element table.
        }
        else if (backingStore instanceof FKSetStore || backingStore instanceof FKListStore || backingStore instanceof FKArrayStore)
        {
            // Set/List/array using foreign-key : Generate an iterator query of the form
            // SELECT ELEM_TBL.COL1, ELEM_TBL.COL2, ... FROM ELEM_TBL
            // WHERE EXISTS (SELECT OWNER_TBL.ID FROM OWNER_TBL WHERE (queryWhereClause) AND ELEM_TBL.OWNER_ID = OWNER_TBL.ID)

            // TODO Start from the original query, and remove any grouping, having, ordering etc, and join to element table.
        }
        throw new NucleusException("BulkFetch via JOIN is not yet implemented");
//        return iterStmt;
    }
}