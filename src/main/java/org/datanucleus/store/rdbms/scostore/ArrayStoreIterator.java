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
import java.util.Iterator;

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
 * ArrayStore iterator for RDBMS datastores.
 */
public class ArrayStoreIterator implements Iterator
{
    private final ExecutionContext ec;

    /** Underlying iterator that we wrap. */
    private final Iterator delegate;

    private Object lastElement = null;

    ArrayStoreIterator(ObjectProvider op, ResultSet rs, ResultObjectFactory rof, ElementContainerStore backingStore) 
    throws MappedDatastoreException
    {
        this.ec = op.getExecutionContext();

        ArrayList results = new ArrayList();
        if (rs != null)
        {
            JavaTypeMapping elementMapping = backingStore.getElementMapping();
            while (next(rs))
            {
                Object nextElement;
                if (backingStore.isElementsAreEmbedded() || backingStore.isElementsAreSerialised())
                {
                    int param[] = new int[elementMapping.getNumberOfColumnMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }

                    if (elementMapping instanceof SerialisedPCMapping || elementMapping instanceof SerialisedReferenceMapping || elementMapping instanceof EmbeddedElementPCMapping)
                    {
                        // Element = Serialised
                        int ownerFieldNumber = -1;
                        if (backingStore.getContainerTable() != null)
                        {
                            ownerFieldNumber = getOwnerFieldMetaData(backingStore.getContainerTable()).getAbsoluteFieldNumber();
                        }
                        nextElement = elementMapping.getObject(ec, rs, param, op, ownerFieldNumber);
                    }
                    else
                    {
                        // Element = Non-PC
                        nextElement = elementMapping.getObject(ec, rs, param);
                    }
                }
                else if (elementMapping instanceof ReferenceMapping)
                {
                    // Element = Reference (Interface/Object)
                    int param[] = new int[elementMapping.getNumberOfColumnMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }
                    nextElement = elementMapping.getObject(ec, rs, param);
                }
                else if (rof != null)
                {
                    // Element = PC
                    nextElement = rof.getObject();
                }
                else
                {
                    nextElement = null;
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
        // Do nothing
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

    protected AbstractMemberMetaData getOwnerFieldMetaData(Table containerTable)
    {
        return ((JoinTable) containerTable).getOwnerMemberMetaData();
    }
}