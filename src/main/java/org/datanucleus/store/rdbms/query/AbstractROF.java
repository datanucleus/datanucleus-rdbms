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

/**
 * Abstract result object factory, taking the ExecutionContext being operated in, and the ResultSet that will be processed.
 */
public abstract class AbstractROF<T> implements ResultObjectFactory<T>
{
    protected ExecutionContext ec;

    protected ResultSet rs;

    protected boolean ignoreCache = false;

    /**
     * Constructor.
     * @param ec ExecutionContext
     * @param rs The JDBC ResultSet
     * @param ignoreCache Whether to ignore the cache(s) when instantiating any persistable objects in the results.
     */
    public AbstractROF(ExecutionContext ec, ResultSet rs, boolean ignoreCache)
    {
        this.ec = ec;
        this.rs = rs;
        this.ignoreCache = ignoreCache;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.query.ResultObjectFactory#getResultSet()
     */
    @Override
    public ResultSet getResultSet()
    {
        return rs;
    }
}
