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
 * Representation of the SQLStatement for an iterator of a collection/array of elements.
 * An iterator statement can be an iterator for a single owner, or a bulk iterator for multiple owners (in which case
 * the <cite>ownerMapIndex</cite> will be set so we can check the owner for the element.
 */
public class ElementIteratorStatement extends IteratorStatement
{
    /** The class mapping for the collection/array element of the iterator. */
    StatementClassMapping elementClassMapping = null;

    public ElementIteratorStatement(Store store, SelectStatement stmt, StatementClassMapping elemClsMapping)
    {
        super(store, stmt);
        this.elementClassMapping = elemClsMapping;
    }

    public StatementClassMapping getElementClassMapping()
    {
        return elementClassMapping;
    }
}