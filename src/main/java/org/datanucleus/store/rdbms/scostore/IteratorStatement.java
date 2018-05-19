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
package org.datanucleus.store.rdbms.scostore;

import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.types.scostore.Store;

/**
 * Representation of the SQLStatement for an iterator of a container (map, collection, or array).
 * An iterator statement can be an iterator for a single owner, or a bulk iterator for multiple owners (in which case
 * the <cite>ownerMapIndex</cite> will be set so we can check the owner for the element/key/value.
 * This will be extended for a collection/array element or map key/value dependent on which type we represent.
 */
public abstract class IteratorStatement
{
    Store backingStore;

    /** The SQL Statement for the iterator. */
    SelectStatement sqlStmt = null;

    /** Mapping index for the owner in the statement (only specified on bulk fetch iterators). */
    StatementMappingIndex ownerMapIndex = null;

    public IteratorStatement(Store store, SelectStatement stmt)
    {
        this.backingStore = store;
        this.sqlStmt = stmt;
    }

    public Store getBackingStore()
    {
        return backingStore;
    }
    public SelectStatement getSelectStatement()
    {
        return sqlStmt;
    }

    public StatementMappingIndex getOwnerMapIndex()
    {
        return ownerMapIndex;
    }
    public void setOwnerMapIndex(StatementMappingIndex idx)
    {
        this.ownerMapIndex = idx;
    }
}