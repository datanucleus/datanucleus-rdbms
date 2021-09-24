/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.java;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;

import org.datanucleus.ExecutionContext;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingPostSet;

/**
 * Oracle variant of the CollectionMapping for cases where we are serialising the field into a single (BLOB/CLOB) column.
 */
public class OracleCollectionMapping extends CollectionMapping
{
    public void performSetPostProcessing(DNStateManager sm)
    {
        if (containerIsStoredInSingleColumn())
        {
            if (columnMappings[0] instanceof ColumnMappingPostSet)
            {
                // Create the value to put in the BLOB
                Collection value = (Collection) sm.provideField(mmd.getAbsoluteFieldNumber());
                byte[] bytes = new byte[0];
                if (value != null)
                {
                    try
                    {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(baos);
                        oos.writeObject(value);
                        bytes = baos.toByteArray();
                    }
                    catch (IOException e1)
                    {
                        // Do Nothing
                    }
                }

                // Update the BLOB
                ((ColumnMappingPostSet)columnMappings[0]).setPostProcessing(sm, bytes);
            }
        }
    }

    public void postInsert(DNStateManager ownerSM)
    {
        if (containerIsStoredInSingleColumn())
        {
            ExecutionContext ec = ownerSM.getExecutionContext();
            Collection value = (Collection) ownerSM.provideField(mmd.getAbsoluteFieldNumber());

            if (value != null)
            {
                if (mmd.getCollection().elementIsPersistent())
                {
                    // Make sure all persistable elements have StateManagers
                    Object[] collElements = value.toArray();
                    for (Object elem : collElements)
                    {
                        if (elem != null)
                        {
                            DNStateManager elemSM = ec.findStateManager(elem);
                            if (elemSM == null || ec.getApiAdapter().getExecutionContext(elem) == null)
                            {
                                elemSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, elem, false, ownerSM, mmd.getAbsoluteFieldNumber());
                            }
                        }
                    }
                }
            }
        }
        else
        {
            super.postInsert(ownerSM);
        }
    }

    public void postUpdate(DNStateManager ownerSM)
    {
        if (containerIsStoredInSingleColumn())
        {
            ExecutionContext ec = ownerSM.getExecutionContext();
            Collection value = (Collection) ownerSM.provideField(mmd.getAbsoluteFieldNumber());

            if (value != null)
            {
                if (mmd.getCollection().elementIsPersistent())
                {
                    // Make sure all persistable elements have StateManagers
                    Object[] collElements = value.toArray();
                    for (Object elem : collElements)
                    {
                        if (elem != null)
                        {
                            DNStateManager elemSM = ec.findStateManager(elem);
                            if (elemSM == null || ec.getApiAdapter().getExecutionContext(elem) == null)
                            {
                                elemSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, elem, false, ownerSM, mmd.getAbsoluteFieldNumber());
                            }
                        }
                    }
                }
            }
        }
        else
        {
            super.postUpdate(ownerSM);
        }
    }
}