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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingPostSet;

/**
 * Oracle variant of the MapMapping for cases where we are serialising the field into a single (BLOB/CLOB) column.
 */
public class OracleMapMapping extends MapMapping
{
    public void performSetPostProcessing(DNStateManager sm)
    {
        if (containerIsStoredInSingleColumn())
        {
            if (columnMappings[0] instanceof ColumnMappingPostSet)
            {
                // Create the value to put in the BLOB
                java.util.Map value = (java.util.Map)sm.provideField(mmd.getAbsoluteFieldNumber());
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
            java.util.Map value = (java.util.Map) ownerSM.provideField(mmd.getAbsoluteFieldNumber());

            // Do nothing when serialised since we are handled in the main request
            if (value != null)
            {
                if (mmd.getMap().keyIsPersistent() || mmd.getMap().valueIsPersistent())
                {
                    // Make sure all persistable keys/values have StateManagers
                    Set entries = value.entrySet();
                    Iterator iter = entries.iterator();
                    while (iter.hasNext())
                    {
                        Map.Entry entry = (Map.Entry)iter.next();
                        if (mmd.getMap().keyIsPersistent() && entry.getKey() != null)
                        {
                            Object key = entry.getKey();
                            if (ec.findStateManager(key) == null || ec.getApiAdapter().getExecutionContext(key) == null)
                            {
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, key, false,
                                    ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_MAP_KEY_PC);
                            }
                        }
                        if (mmd.getMap().valueIsPersistent() && entry.getValue() != null)
                        {
                            Object val = entry.getValue();
                            if (ec.findStateManager(val) == null || ec.getApiAdapter().getExecutionContext(val) == null)
                            {
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, val, false,
                                    ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_MAP_VALUE_PC);
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
            java.util.Map value = (java.util.Map) ownerSM.provideField(mmd.getAbsoluteFieldNumber());

            if (value != null)
            {
                if (mmd.getMap().keyIsPersistent() || mmd.getMap().valueIsPersistent())
                {
                    // Make sure all persistable keys/values have StateManagers
                    Set entries = value.entrySet();
                    Iterator iter = entries.iterator();
                    while (iter.hasNext())
                    {
                        Map.Entry entry = (Map.Entry)iter.next();
                        if (mmd.getMap().keyIsPersistent() && entry.getKey() != null)
                        {
                            Object key = entry.getKey();
                            if (ec.findStateManager(key) == null || ec.getApiAdapter().getExecutionContext(key) == null)
                            {
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, key, false,
                                    ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_MAP_KEY_PC);
                            }
                        }
                        if (mmd.getMap().valueIsPersistent() && entry.getValue() != null)
                        {
                            Object val = entry.getValue();
                            if (ec.findStateManager(val) == null || ec.getApiAdapter().getExecutionContext(val) == null)
                            {
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, val, false,
                                    ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_MAP_VALUE_PC);
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