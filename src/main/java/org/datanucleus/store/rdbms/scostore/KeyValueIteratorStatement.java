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

import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.types.scostore.Store;

/**
 * Representation of the SQLStatement for an iterator of a map of keys/values.
 * An iterator statement can be an iterator for a single owner, or a bulk iterator for multiple owners (in which case
 * the <cite>ownerMapIndex</cite> will be set so we can check the owner for the key/value.
 */
public class KeyValueIteratorStatement extends IteratorStatement
{
    /** The class mapping for the map key of the iterator. */
    StatementClassMapping keyClassMapping = null;

    /** The class mapping for the map value of the iterator. */
    StatementClassMapping valueClassMapping = null;

    public KeyValueIteratorStatement(Store store, SelectStatement stmt, StatementClassMapping keyClsMapping, StatementClassMapping valueClsMapping)
    {
        super(store, stmt);
        this.keyClassMapping = keyClsMapping;
        this.valueClassMapping = valueClsMapping;
    }

    public StatementClassMapping getKeyClassMapping()
    {
        return keyClassMapping;
    }
    public StatementClassMapping getValueClassMapping()
    {
        return valueClassMapping;
    }
}