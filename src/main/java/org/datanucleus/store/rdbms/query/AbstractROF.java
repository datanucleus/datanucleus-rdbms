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

import java.sql.ResultSet;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;

/**
 * Abstract result object factory, taking the ExecutionContext being operated in, and the ResultSet that will be processed.
 */
public abstract class AbstractROF<T> implements ResultObjectFactory<T>
{
    protected ExecutionContext ec;

    protected ResultSet rs;

    protected boolean ignoreCache = false;

    protected boolean updateAllFields = false;

    protected FetchPlan fp;

    /**
     * Constructor.
     * @param ec ExecutionContext
     * @param rs The JDBC ResultSet
     * @param fp FetchPlan
     */
    public AbstractROF(ExecutionContext ec, ResultSet rs, FetchPlan fp)
    {
        this.ec = ec;
        this.rs = rs;
        this.fp = fp;
    }

    @Override
    public ResultSet getResultSet()
    {
        return rs;
    }

    public void setIgnoreCache(boolean ignore)
    {
        this.ignoreCache = ignore;
    }

    public void setUpdateAllFields(boolean update)
    {
        this.updateAllFields = update;
    }
}