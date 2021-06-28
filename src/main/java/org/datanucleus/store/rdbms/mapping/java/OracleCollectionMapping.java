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
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.mapping.column.OracleBlobColumnMapping;
import org.datanucleus.util.NucleusLogger;

/**
 * Oracle variant of the CollectionMapping for cases where we are serialising the field into a single column. 
 * This is necessary so we can perform any necessary postInsert, postUpdate nonsense for inserting BLOBs.
 */
public class OracleCollectionMapping extends CollectionMapping
{
    public void performSetPostProcessing(ObjectProvider op)
    {
        NucleusLogger.GENERAL.info(">> OracleCollectionMapping.performSetPostProc - DO NOTHING");
    }

    /**
     * Retrieve the empty BLOB created by the insert statement and write out the
     * current BLOB field value to the Oracle BLOB object.
     * @param ownerOP ObjectProvider of the owner
     */
    public void postInsert(ObjectProvider ownerOP)
    {
        if (containerIsStoredInSingleColumn())
        {
            ExecutionContext ec = ownerOP.getExecutionContext();
            Collection value = (Collection) ownerOP.provideField(mmd.getAbsoluteFieldNumber());

            if (value != null)
            {
                if (mmd.getCollection().elementIsPersistent())
                {
                    // Make sure all persistable elements have ObjectProviders
                    Object[] collElements = value.toArray();
                    for (Object elem : collElements)
                    {
                        if (elem != null)
                        {
                            ObjectProvider elemOP = ec.findObjectProvider(elem);
                            if (elemOP == null || ec.getApiAdapter().getExecutionContext(elem) == null)
                            {
                                elemOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, elem, false, ownerOP, mmd.getAbsoluteFieldNumber());
                            }
                        }
                    }
                }
            }

            // Generate the contents for the BLOB
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
            OracleBlobColumnMapping.updateBlobColumn(ownerOP, getTable(), getColumnMapping(0), bytes);
        }
        else
        {
            super.postInsert(ownerOP);
        }
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#postUpdate(org.datanucleus.state.ObjectProvider)
     */
    public void postUpdate(ObjectProvider ownerOP)
    {
        if (containerIsStoredInSingleColumn())
        {
            ExecutionContext ec = ownerOP.getExecutionContext();
            Collection value = (Collection) ownerOP.provideField(mmd.getAbsoluteFieldNumber());

            if (value != null)
            {
                if (mmd.getCollection().elementIsPersistent())
                {
                    // Make sure all persistable elements have ObjectProviders
                    Object[] collElements = value.toArray();
                    for (Object elem : collElements)
                    {
                        if (elem != null)
                        {
                            ObjectProvider elemOP = ec.findObjectProvider(elem);
                            if (elemOP == null || ec.getApiAdapter().getExecutionContext(elem) == null)
                            {
                                elemOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, elem, false, ownerOP, mmd.getAbsoluteFieldNumber());
                            }
                        }
                    }
                }
            }

            // TODO This should really just be the generate the contents of the BLOB followed by call to OracleBlobColumnMapping.updateBlobColumn
            postInsert(ownerOP);
        }
        else
        {
            super.postUpdate(ownerOP);
        }
    }
}