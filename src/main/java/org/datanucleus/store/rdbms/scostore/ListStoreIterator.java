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
package org.datanucleus.store.rdbms.scostore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ListIterator;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.Table;

/**
 * ListStore iterator for RDBMS datastores.
 * @param <E> Element type of the list store
 */
public class ListStoreIterator<E> implements ListIterator<E>
{
    private final ObjectProvider op;

    private final ListIterator<E> delegate;

    private E lastElement = null;

    private int currentIndex = -1;

    private final AbstractListStore<E> abstractListStore;

    ListStoreIterator(ObjectProvider op, ResultSet resultSet, ResultObjectFactory rof, AbstractListStore<E> als)
    throws MappedDatastoreException
    {
        this.op = op;
        this.abstractListStore = als;

        ExecutionContext ec = op.getExecutionContext();
        ArrayList results = new ArrayList();
        if (resultSet != null)
        {
            Table containerTable = als.getContainerTable();
            boolean elementsAreSerialised = als.isElementsAreSerialised();
            boolean elementsAreEmbedded = als.isElementsAreEmbedded();
            JavaTypeMapping elementMapping = als.getElementMapping();
            while (next(resultSet))
            {
                Object nextElement;
                if (elementsAreEmbedded || elementsAreSerialised)
                {
                    int param[] = new int[elementMapping.getNumberOfDatastoreMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }

                    if (elementMapping instanceof SerialisedPCMapping || 
                        elementMapping instanceof SerialisedReferenceMapping ||
                        elementMapping instanceof EmbeddedElementPCMapping)
                    {
                        // Element = Serialised
                        int ownerFieldNumber = -1;
                        if (containerTable != null)
                        {
                            ownerFieldNumber =
                                getOwnerMemberMetaData(abstractListStore.containerTable).getAbsoluteFieldNumber();
                        }
                        nextElement = elementMapping.getObject(ec, resultSet, param, op, ownerFieldNumber);
                    }
                    else
                    {
                        // Element = Non-PC
                        nextElement = elementMapping.getObject(ec, resultSet, param);
                    }
                }
                else if (elementMapping instanceof ReferenceMapping)
                {
                    // Element = Reference (Interface/Object)
                    int param[] = new int[elementMapping.getNumberOfDatastoreMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }
                    nextElement = elementMapping.getObject(ec, resultSet, param);
                }
                else
                {
                    nextElement = rof.getObject();
                }

                results.add(nextElement);
            }
        }
        delegate = results.listIterator();
    }

    public void add(E elem)
    {
        currentIndex = delegate.nextIndex();
        abstractListStore.add(op, elem, currentIndex, -1);
        delegate.add(elem);
        lastElement = null;
    }

    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    public boolean hasPrevious()
    {
        return delegate.hasPrevious();
    }

    public E next()
    {
        currentIndex = delegate.nextIndex();
        lastElement = delegate.next();

        return lastElement;
    }

    public int nextIndex()
    {
        return delegate.nextIndex();
    }

    public E previous()
    {
        currentIndex = delegate.previousIndex();
        lastElement = delegate.previous();

        return lastElement;
    }

    public int previousIndex()
    {
        return delegate.previousIndex();
    }

    public synchronized void remove()
    {
        if (lastElement == null)
        {
            throw new IllegalStateException("No entry to remove");
        }

        abstractListStore.remove(op, currentIndex, -1);
        delegate.remove();

        lastElement = null;
        currentIndex = -1;
    }

    public synchronized void set(E elem)
    {
        if (lastElement == null)
        {
            throw new IllegalStateException("No entry to replace");
        }

        abstractListStore.set(op, currentIndex, elem, true);
        delegate.set(elem);
        lastElement = elem;
    }

    protected AbstractMemberMetaData getOwnerMemberMetaData(Table containerTable)
    {
        return ((JoinTable) containerTable).getOwnerMemberMetaData();
    }

    protected boolean next(Object resultSet) throws MappedDatastoreException
    {
        try
        {
            return ((ResultSet)resultSet).next();
        }
        catch (SQLException e)
        {
            throw new MappedDatastoreException(e.getMessage(), e);
        }
    }
}