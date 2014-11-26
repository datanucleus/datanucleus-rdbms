/**********************************************************************
Copyright (c) 2003 Mike Martin and others. All rights reserved.
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
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - implementation of newScalarExpression, newLiteral
2005 Andy Jefferson - basic serialisation support
2005 Andy Jefferson - updated serialisation using SCOUtils methods
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.scostore.MapStore;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.wrappers.backed.BackedSCO;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * SCO Mapping for Map types.
 */
public class MapMapping extends AbstractContainerMapping implements MappingCallbacks
{
    /**
     * Accessor for the Java type represented here.
     * @return The java type
     */
    public Class getJavaType()
    {
        return Map.class;
    }

    // ---------------- Implementation of MappingCallbacks --------------------

    public void insertPostProcessing(ObjectProvider ownerOP)
    {
    }

    /**
     * Method to be called after the insert of the owner class element.
     * @param ownerOP ObjectProvider of the owner
     */
    public void postInsert(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        java.util.Map value = (java.util.Map) ownerOP.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when serialised since we are handled in the main request
            if (value != null)
            {
                // Make sure the keys/values are ok for proceeding
                SCOUtils.validateObjectsForWriting(ec, value.keySet());
                SCOUtils.validateObjectsForWriting(ec, value.values());
            }
            return;
        }

        if (value == null)
        {
            // replace null map with an empty SCO wrapper
            replaceFieldWithWrapper(ownerOP, null);
            return;
        }

        if (mmd.isCascadePersist())
        {
            // Reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007007", mmd.getFullFieldName()));
            }

            if (value.size() > 0)
            {
                // Add the entries direct to the datastore
                RDBMSStoreManager storeMgr = table.getStoreManager();
                ((MapStore) storeMgr.getBackingStoreForField(ownerOP.getExecutionContext().getClassLoaderResolver(), mmd, value.getClass())).putAll(ownerOP, value);

                // Create a SCO wrapper with the entries loaded
                replaceFieldWithWrapper(ownerOP, value);

                // Make sure all are flushed
                ec.flushInternal(true);
            }
            else
            {
                if (mmd.getRelationType(ownerOP.getExecutionContext().getClassLoaderResolver()) == RelationType.MANY_TO_MANY_BI)
                {
                    // Create a SCO wrapper, pass in null so it loads any from the datastore (on other side?)
                    replaceFieldWithWrapper(ownerOP, null);
                }
                else
                {
                    // Create a SCO wrapper, pass in empty map to avoid loading from DB (extra SQL)
                    replaceFieldWithWrapper(ownerOP, value);
                }
            }
        }
        else
        {
            // Field doesnt support cascade-persist so no reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
            }

            // Check for any persistable keys/values that arent persistent
            ApiAdapter api = ec.getApiAdapter();
            Set entries = value.entrySet();
            Iterator iter = entries.iterator();
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry)iter.next();
                if (api.isPersistable(entry.getKey()))
                {
                    if (!api.isPersistent(entry.getKey()) && !api.isDetached(entry.getKey()))
                    {
                        // Key is not persistent so throw exception
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), entry.getKey());
                    }
                }
                if (api.isPersistable(entry.getValue()))
                {
                    if (!api.isPersistent(entry.getValue()) && !api.isDetached(entry.getValue()))
                    {
                        // Value is not persistent so throw exception
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), entry.getValue());
                    }
                }
            }
            replaceFieldWithWrapper(ownerOP, value);
        }
    }

    /**
     * Method to be called after any update of the owner class element.
     * @param ownerOP ObjectProvider of the owner
     */
    public void postUpdate(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        RDBMSStoreManager storeMgr = table.getStoreManager();
        java.util.Map value = (java.util.Map) ownerOP.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when serialised since we are handled in the main request
            if (value != null)
            {
                // Make sure the keys/values are ok for proceeding
                SCOUtils.validateObjectsForWriting(ec, value.keySet());
                SCOUtils.validateObjectsForWriting(ec, value.values());
            }
            return;
        }

        if (value == null)
        {
            // replace null map with empty SCO wrapper
            replaceFieldWithWrapper(ownerOP, null);
            return;
        }

        if (value instanceof BackedSCO)
        {
            // Already have a SCO value, so flush outstanding updates
            ownerOP.getExecutionContext().flushOperationsForBackingStore(((BackedSCO)value).getBackingStore(), ownerOP);
            return;
        }

        if (mmd.isCascadeUpdate())
        {
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007009", mmd.getFullFieldName()));
            }

            // Update the datastore with this value of map (clear old entries and add new ones)
            // This method could be called in two situations
            // 1). Update a map field of an object, so UpdateRequest is called, which calls here
            // 2). Persist a new object, and it needed to wait til the element was inserted so
            //     goes into dirty state and then flush() triggers UpdateRequest, which comes here
            MapStore store = ((MapStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, value.getClass()));

            // TODO Consider making this more efficient picking the ones to remove/add
            // e.g use an update() method on the backing store like for CollectionStore
            store.clear(ownerOP);
            store.putAll(ownerOP, value);

            // Replace the field with a wrapper containing these entries
            replaceFieldWithWrapper(ownerOP, value);
        }
        else
        {
            // TODO Should this throw exception if the element doesn't exist?
            // User doesnt want to update by reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007008", mmd.getFullFieldName()));
            }
            return;
        }
    }

    /**
     * Method to be called before any delete of the owner class element.
     * @param ownerOP ObjectProvider of the owner
     */
    public void preDelete(ObjectProvider ownerOP)
    {
        // Do nothing - dependent deletion is performed by deleteDependent()
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when serialised since we are handled in the main request
            return;
        }

        // makes sure field is loaded
        ownerOP.isLoaded(getAbsoluteFieldNumber());
        java.util.Map value = (java.util.Map) ownerOP.provideField(getAbsoluteFieldNumber());
        if (value == null)
        {
            // Do nothing
        }
        else
        {
            if (!(value instanceof SCO))
            {
                // Make sure we have a SCO wrapper so we can clear from the datastore
                value = (java.util.Map)SCOUtils.wrapSCOField(ownerOP, mmd.getAbsoluteFieldNumber(), value, false, false, true);
            }
            value.clear();

            // Flush any outstanding updates for this backing store
            ownerOP.getExecutionContext().flushOperationsForBackingStore(((BackedSCO)value).getBackingStore(), ownerOP);
        }
    }
}