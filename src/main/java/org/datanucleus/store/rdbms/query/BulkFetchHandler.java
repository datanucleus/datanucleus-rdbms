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

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.rdbms.scostore.IteratorStatement;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLStatementParameter;
import org.datanucleus.store.rdbms.sql.SelectStatement;

/**
 * Interface for a handler for "bulk fetch" of a multi-valued field from a query.
 */
public interface BulkFetchHandler
{
    /**
     * Method to return the bulk-fetch statement (and its associated mappings for extracting the results).
     * @param candidateCmd Metadata for the candidate
     * @param mmd Metadata for the member we are bulk-fetching the value(s) for
     * @param query The query
     * @param parameters Parameters for the query
     * @param datastoreCompilation The datastore compilation of the query
     * @param mapperOptions Any mapper options for query generation
     * @return The statement to use for bulk fetching, together with mappings for extracting the results of the elements
     */
    IteratorStatement getStatementToBulkFetchField(AbstractClassMetaData candidateCmd, AbstractMemberMetaData mmd, 
            Query query, Map parameters, RDBMSQueryCompilation datastoreCompilation, Set<String> mapperOptions);

    /**
     * Convenience method to apply the passed parameters to the provided bulk-fetch statement.
     * Takes care of applying parameters across any UNIONs of elements.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param datastoreCompilation The datastore compilation for the query itself
     * @param sqlStmt The bulk-fetch iterator statement
     * @param parameters The map of parameters passed in to the query
     */
    public static void applyParametersToStatement(ExecutionContext ec, PreparedStatement ps, RDBMSQueryCompilation datastoreCompilation, SQLStatement sqlStmt, Map parameters)
    {
        Map<Integer, String> stmtParamNameByPosition = null;
        List<SQLStatementParameter> stmtParams = null;
        if (datastoreCompilation.getStatementParameters() != null)
        {
            stmtParams = new ArrayList<>();
            stmtParams.addAll(datastoreCompilation.getStatementParameters());

            SelectStatement selectStmt = (SelectStatement)sqlStmt;
            int numUnions = selectStmt.getNumberOfUnions();
            for (int i=0;i<numUnions;i++)
            {
                stmtParams.addAll(datastoreCompilation.getStatementParameters());
            }

            if (datastoreCompilation.getParameterNameByPosition() != null && datastoreCompilation.getParameterNameByPosition().size() > 0)
            {
                // ParameterNameByPosition is only populated with implicit parameters
                stmtParamNameByPosition = new HashMap<>();
                stmtParamNameByPosition.putAll(datastoreCompilation.getParameterNameByPosition());

                int numParams = stmtParamNameByPosition.size();
                for (int i=0;i<numUnions;i++)
                {
                    Iterator<Map.Entry<Integer, String>> paramEntryIter = datastoreCompilation.getParameterNameByPosition().entrySet().iterator();
                    while (paramEntryIter.hasNext())
                    {
                        Map.Entry<Integer, String> paramEntry = paramEntryIter.next();
                        stmtParamNameByPosition.put(numParams*(i+1) + paramEntry.getKey(), paramEntry.getValue());
                    }
                }
            }

            SQLStatementHelper.applyParametersToStatement(ps, ec, stmtParams, stmtParamNameByPosition, parameters);
        }
    }
}
