/**********************************************************************
Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MemberComponent;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.Table;

/**
 * RDBMS-specific implementation of {@link Iterator} for Collections/Sets.
 * @param <E> Type of element in the collection backing store
 */
class CollectionStoreIterator<E> implements Iterator<E>
{
    private final AbstractCollectionStore<E> collStore;
    private final DNStateManager sm;
    private final ExecutionContext ec;
    private final Iterator<E> delegate;
    private E lastElement = null;

    CollectionStoreIterator(DNStateManager sm, ResultSet rs, ResultObjectFactory rof, AbstractCollectionStore<E> store)
    throws MappedDatastoreException
    {
        this.sm = sm;
        this.ec = sm.getExecutionContext();
        this.collStore = store;
        ArrayList results = new ArrayList();
        if (rs != null)
        {
            while (next(rs))
            {
                Object nextElement;
                if (collStore.elementsAreEmbedded || collStore.elementsAreSerialised)
                {
                    int param[] = new int[collStore.elementMapping.getNumberOfColumnMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }

                    if (collStore.elementMapping instanceof SerialisedPCMapping ||
                        collStore.elementMapping instanceof SerialisedReferenceMapping ||
                        collStore.elementMapping instanceof EmbeddedElementPCMapping)
                    {
                        // Element = Serialised
                        int ownerFieldNumber = -1;
                        if (collStore.containerTable != null)
                        {
                            ownerFieldNumber = getOwnerMemberMetaData(collStore.containerTable).getAbsoluteFieldNumber();
                        }
                        nextElement = collStore.elementMapping.getObject(ec, rs, param, sm, ownerFieldNumber, MemberComponent.COLLECTION_ELEMENT);
                    }
                    else
                    {
                        // Element = Non-PC
                        nextElement = collStore.elementMapping.getObject(ec, rs, param);
                    }
                }
                else if (collStore.elementMapping instanceof ReferenceMapping)
                {
                    // Element = Reference (Interface/Object)
                    int param[] = new int[collStore.elementMapping.getNumberOfColumnMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }
                    nextElement = collStore.elementMapping.getObject(ec, rs, param);
                }
                else
                {
                    // Element = PC
                    nextElement = rof.getObject();
                }

                results.add(nextElement);
            }
        }
        delegate = results.iterator();
    }

    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    public E next()
    {
        lastElement = delegate.next();

        return lastElement;
    }

    public synchronized void remove()
    {
        if (lastElement == null)
        {
            throw new IllegalStateException("No entry to remove");
        }

        collStore.remove(sm, lastElement, -1, true);
        delegate.remove();

        lastElement = null;
    }

    protected boolean next(Object rs) throws MappedDatastoreException
    {
        try
        {
            return ((ResultSet) rs).next();
        }
        catch (SQLException e)
        {
            throw new MappedDatastoreException("SQLException", e);
        }
    }

    protected AbstractMemberMetaData getOwnerMemberMetaData(Table containerTable)
    {
        return ((JoinTable) containerTable).getOwnerMemberMetaData();
    }
}