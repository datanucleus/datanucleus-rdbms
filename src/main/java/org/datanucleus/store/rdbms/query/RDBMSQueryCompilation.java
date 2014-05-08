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
import org.datanucleus.store.rdbms.sql.SQLStatementParameter;

/**
 * Datastore-specific (RDBMS) compilation information for a java query.
 * Can represent a single SQL statement, or can represent multiple SQL statements all with the same
 * results and parameters.
 */
public class RDBMSQueryCompilation
{
    /** Generated SQL statement. */
    String sql = null;

    /** SQL Statements when it equates to multiple statements. */
    List<String> sqls = null;

    /** Flags for each SQL statement whether they count towards the overall "count" of the operation. */
    List<Boolean> sqlUseInCountFlags = null;

    /** Result mappings when the result is for a candidate (can be null). */
    StatementClassMapping resultsDefinitionForClass = null;

    /** Result mappings when the result is not for a candidate (can be null). */
    StatementResultMapping resultsDefinition = null;

    /** Input parameter definitions, in the order used in the SQL. */
    List<SQLStatementParameter> inputParameters;

    Map<Integer, String> inputParameterNameByPosition;

    /** Map of statements to get SCO containers that are in the fetch plan (bulk fetch). */
    Map<String, IteratorStatement> scoIteratorStatementByMemberName;

    boolean precompilable = true;

    public RDBMSQueryCompilation()
    {
    }

    public void setSQL(String sql)
    {
        this.sql = sql;
        this.sqls = null;
    }

    public void setSQL(List<String> sqls, List<Boolean> sqlUseInCountFlags)
    {
        this.sql = null;
        if (this.sqls == null)
        {
            this.sqls = new ArrayList<String>(sqls.size());
        }
        this.sqls.addAll(sqls);
        if (this.sqlUseInCountFlags == null)
        {
            this.sqlUseInCountFlags = new ArrayList<Boolean>(sqlUseInCountFlags.size());
        }
        this.sqlUseInCountFlags.addAll(sqlUseInCountFlags);
    }

    public String getSQL()
    {
        return sql;
    }

    public List<String> getSQLs()
    {
        return sqls;
    }

    public List<Boolean> getSQLUseInCountFlags()
    {
        return sqlUseInCountFlags;
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
            scoIteratorStatementByMemberName = new HashMap<String, IteratorStatement>();
        }
        scoIteratorStatementByMemberName.put(memberName, iterStmt);
    }

    public Map<String, IteratorStatement> getSCOIteratorStatements()
    {
        return scoIteratorStatementByMemberName;
    }
}