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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.scostore.MapStore;
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

    /**
     * Method to be called after the insert of the owner class element.
     * @param ownerSM StateManager of the owner
     */
    public void postInsert(DNStateManager ownerSM)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();
        java.util.Map value = (java.util.Map) ownerSM.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
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
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, key, false, ownerSM, mmd.getAbsoluteFieldNumber());
                            }
                        }
                        if (mmd.getMap().valueIsPersistent() && entry.getValue() != null)
                        {
                            Object val = entry.getValue();
                            if (ec.findStateManager(val) == null || ec.getApiAdapter().getExecutionContext(val) == null)
                            {
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, val, false, ownerSM, mmd.getAbsoluteFieldNumber());
                            }
                        }
                    }
                }
            }
            return;
        }

        if (value == null)
        {
            // replace null map with an empty SCO wrapper
            replaceFieldWithWrapper(ownerSM, null);
            return;
        }

        if (!mmd.isCascadePersist())
        {
            // Check that all keys/values are persistent before continuing and throw exception if necessary
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
            }

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
        }
        else
        {
            // Reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007007", IdentityUtils.getPersistableIdentityForId(ownerSM.getInternalObjectId()), mmd.getFullFieldName()));
            }
        }

        if (value.size() > 0)
        {
            // Add the entries direct to the datastore
            ((MapStore) table.getStoreManager().getBackingStoreForField(ec.getClassLoaderResolver(), mmd, value.getClass())).putAll(ownerSM, value, Collections.emptyMap());

            // Create a SCO wrapper with the entries loaded
            replaceFieldWithWrapper(ownerSM, value);
        }
        else
        {
            if (mmd.getRelationType(ec.getClassLoaderResolver()) == RelationType.MANY_TO_MANY_BI)
            {
                // Create a SCO wrapper, pass in null so it loads any from the datastore (on other side?)
                replaceFieldWithWrapper(ownerSM, null);
            }
            else
            {
                // Create a SCO wrapper, pass in empty map to avoid loading from DB (extra SQL)
                replaceFieldWithWrapper(ownerSM, value);
            }
        }
    }

    /**
     * Method to be called after any update of the owner class element.
     * @param ownerSM StateManager of the owner
     */
    public void postUpdate(DNStateManager ownerSM)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();
        RDBMSStoreManager storeMgr = table.getStoreManager();
        java.util.Map value = (java.util.Map) ownerSM.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
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
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, key, false, ownerSM, mmd.getAbsoluteFieldNumber());
                            }
                        }
                        if (mmd.getMap().valueIsPersistent() && entry.getValue() != null)
                        {
                            Object val = entry.getValue();
                            if (ec.findStateManager(val) == null || ec.getApiAdapter().getExecutionContext(val) == null)
                            {
                                ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, val, false, ownerSM, mmd.getAbsoluteFieldNumber());
                            }
                        }
                    }
                }
            }
            return;
        }

        if (value == null)
        {
            // remove any entries in the map and replace it with an empty SCO wrapper
            ((MapStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, null)).clear(ownerSM);
            replaceFieldWithWrapper(ownerSM, null);
            return;
        }

        if (value instanceof BackedSCO)
        {
            // Already have a SCO value, so flush outstanding updates
            ec.flushOperationsForBackingStore(((BackedSCO)value).getBackingStore(), ownerSM);
            return;
        }

        if (mmd.isCascadePersist())
        {
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007009", IdentityUtils.getPersistableIdentityForId(ownerSM.getInternalObjectId()), mmd.getFullFieldName()));
            }

            // Update the datastore with this value of map (clear old entries and add new ones)
            // This method could be called in two situations
            // 1). Update a map field of an object, so UpdateRequest is called, which calls here
            // 2). Persist a new object, and it needed to wait til the element was inserted so
            //     goes into dirty state and then flush() triggers UpdateRequest, which comes here
            MapStore store = ((MapStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, value.getClass()));

            store.update(ownerSM, value);
            // TODO Optimise this. Should mean call removeAll(Set<K>), followed by putAll(map, currentMap)
//            store.clear(ownerSM);
//            store.putAll(ownerSM, value, Collections.emptyMap());

            // Replace the field with a wrapper containing these entries
            replaceFieldWithWrapper(ownerSM, value);
        }
        else
        {
            // TODO Should this throw exception if the element doesn't exist?
            // User doesnt want to update by reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007008", IdentityUtils.getPersistableIdentityForId(ownerSM.getInternalObjectId()), mmd.getFullFieldName()));
            }
            return;
        }
    }

    /**
     * Method to be called before any delete of the owner class element.
     * @param ownerSM StateManager of the owner
     */
    public void preDelete(DNStateManager ownerSM)
    {
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when serialised since we are handled in the main request
            return;
        }

        // makes sure field is loaded
        ownerSM.isLoaded(getAbsoluteFieldNumber());
        java.util.Map value = (java.util.Map) ownerSM.provideField(getAbsoluteFieldNumber());
        if (value == null)
        {
            // Do nothing
            return;
        }

        if (!(value instanceof SCO))
        {
            // Make sure we have a SCO wrapper so we can clear from the datastore
            value = (java.util.Map)SCOUtils.wrapSCOField(ownerSM, mmd.getAbsoluteFieldNumber(), value, true);
        }
        value.clear();

        // Flush any outstanding updates for this backing store
        ownerSM.getExecutionContext().flushOperationsForBackingStore(((BackedSCO)value).getBackingStore(), ownerSM);
    }
}