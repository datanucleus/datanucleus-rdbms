/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.scostore.IteratorStatement;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementParameter;

/**
 * Datastore-specific (RDBMS) compilation information for a java query.
 * Can represent a single SQL statement, or can represent multiple SQL statements all with the same results and parameters.
 */
public class RDBMSQueryCompilation
{
    List<StatementCompilation> statementCompilations = new ArrayList(1);

    /** Input parameter definitions, in the order used in the SQL. */
    List<SQLStatementParameter> inputParameters;

    /** Map of input parameter name keyed by its position. Only for SELECT queries. */
    Map<Integer, String> inputParameterNameByPosition;

    /** Result mappings when the result is for a candidate (can be null). Only for SELECT queries. */
    StatementClassMapping resultsDefinitionForClass = null;

    /** Result mappings when the result is not for a candidate (can be null). Only for SELECT queries. */
    StatementResultMapping resultsDefinition = null;

    /** Map of statements to get SCO containers that are in the fetch plan (bulk fetch). Only for SELECT queries. */
    Map<String, IteratorStatement> scoIteratorStatementByMemberName;

    boolean precompilable = true;

    public class StatementCompilation
    {
        SQLStatement stmt;
        String sql;
        boolean useInCount = true;
        public StatementCompilation(SQLStatement stmt, String sql, boolean useInCount)
        {
            this.stmt = stmt;
            this.sql = sql;
            this.useInCount = useInCount;
        }
        public SQLStatement getStatement()
        {
            return stmt;
        }
        public String getSQL()
        {
            return sql;
        }
        public boolean useInCount()
        {
            return useInCount;
        }
        public void setSQL(String sql)
        {
            this.sql = sql;
        }
    }

    public RDBMSQueryCompilation()
    {
    }

    public int getNumberOfStatements()
    {
        return this.statementCompilations.size();
    }
    public void clearStatements()
    {
        this.statementCompilations.clear();
    }

    public void addStatement(SQLStatement stmt, String sql, boolean useInCount)
    {
        statementCompilations.add(new StatementCompilation(stmt, sql, useInCount));
    }

    public List<StatementCompilation> getStatementCompilations()
    {
        return statementCompilations;
    }

    /**
     * Convenience mutator for the SQL to invoke, when we only have 1 statement associated with this compilation.
     * Use addStatement/getStatementCompilations to set the SQLs when we have multiple statements.
     * @param sql The SQL to be invoked, overwriting any previous SQL set here
     * @deprecated Use addStatement instead so then we have the SQLStatement
     */
    public void setSQL(String sql)
    {
        clearStatements();
        addStatement(null, sql, true);
    }

    /**
     * Convenience accessor for the SQL to invoke, when we only have 1 statement associated with this compilation.
     * Use getStatementCompilations to get the SQLs when we have multiple statements.
     * @return The SQL to be invoked
     */
    public String getSQL()
    {
        if (statementCompilations.isEmpty())
        {
            return null;
        }
        return statementCompilations.get(0).getSQL();
    }

    public void setPrecompilable(boolean precompilable)
    {
        this.precompilable = precompilable;
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void setResultDefinitionForClass(StatementClassMapping def)
    {
        this.resultsDefinitionForClass = def;
    }

    public StatementClassMapping getResultDefinitionForClass()
    {
        return resultsDefinitionForClass;
    }

    public void setResultDefinition(StatementResultMapping def)
    {
        this.resultsDefinition = def;
    }

    public StatementResultMapping getResultDefinition()
    {
        return resultsDefinition;
    }

    public void setStatementParameters(List<SQLStatementParameter> params)
    {
        this.inputParameters = params;
    }

    public List<SQLStatementParameter> getStatementParameters()
    {
        return inputParameters;
    }

    public void setParameterNameByPosition(Map<Integer, String> paramNameByPos)
    {
        this.inputParameterNameByPosition = paramNameByPos;
    }

    public Map<Integer, String> getParameterNameByPosition()
    {
        return inputParameterNameByPosition;
    }

    public void setSCOIteratorStatement(String memberName, IteratorStatement iterStmt)
    {
        if (scoIteratorStatementByMemberName == null)
        {
            scoIteratorStatementByMemberName = new HashMap<>();
        }
        scoIteratorStatementByMemberName.put(memberName, iterStmt);
    }

    public Map<String, IteratorStatement> getSCOIteratorStatements()
    {
        return scoIteratorStatementByMemberName;
    }
}