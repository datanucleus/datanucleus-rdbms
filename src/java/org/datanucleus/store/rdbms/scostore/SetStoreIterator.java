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
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.Table;

/**
 * RDBMS-specific implementation of {@link Iterator} for Sets.
 */
class SetStoreIterator implements Iterator
{
    private final AbstractSetStore abstractSetStore;
    private final ObjectProvider op;
    private final ExecutionContext ec;
    private final Iterator delegate;
    private Object lastElement = null;

    SetStoreIterator(ObjectProvider op, ResultSet rs, ResultObjectFactory rof, AbstractSetStore setStore)
        throws MappedDatastoreException
    {
        this.op = op;
        this.ec = op.getExecutionContext();
        this.abstractSetStore = setStore;
        ArrayList results = new ArrayList();
        if (rs != null)
        {
            while (next(rs))
            {
                Object nextElement;
                if (abstractSetStore.elementsAreEmbedded || abstractSetStore.elementsAreSerialised)
                {
                    int param[] = new int[abstractSetStore.elementMapping.getNumberOfDatastoreMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }

                    if (abstractSetStore.elementMapping instanceof SerialisedPCMapping ||
                        abstractSetStore.elementMapping instanceof SerialisedReferenceMapping ||
                        abstractSetStore.elementMapping instanceof EmbeddedElementPCMapping)
                    {
                        // Element = Serialised
                        int ownerFieldNumber = -1;
                        if (abstractSetStore.containerTable != null)
                        {
                            ownerFieldNumber = getOwnerMemberMetaData(abstractSetStore.containerTable).getAbsoluteFieldNumber();
                        }
                        nextElement = abstractSetStore.elementMapping.getObject(ec, rs, param, op, ownerFieldNumber);
                    }
                    else
                    {
                        // Element = Non-PC
                        nextElement = abstractSetStore.elementMapping.getObject(ec, rs, param);
                    }
                }
                else if (abstractSetStore.elementMapping instanceof ReferenceMapping)
                {
                    // Element = Reference (Interface/Object)
                    int param[] = new int[abstractSetStore.elementMapping.getNumberOfDatastoreMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }
                    nextElement = abstractSetStore.elementMapping.getObject(ec, rs, param);
                }
                else
                {
                    // Element = PC
                    nextElement = rof.getObject(ec, rs);
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

    public Object next()
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

        abstractSetStore.remove(op, lastElement, -1, true);
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