/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved.
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
2005 Andy Jefferson - basic serialisation support
2005 Andy Jefferson - updated serialisation using SCOUtils methods
2005 Andy Jefferson - changed to allow the store to be set or list
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.util.Collection;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.scostore.CollectionStore;
import org.datanucleus.store.types.wrappers.backed.BackedSCO;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping for Collection types.
 */
public class CollectionMapping extends AbstractContainerMapping implements MappingCallbacks
{
    /**
     * Accessor for the Java type represented here.
     * @return The java type
     */
    @Override
    public Class getJavaType()
    {
        return Collection.class;
    }

    // --------------- Implementation of MappingCallbacks -------------------

    @Override
    public void postInsert(DNStateManager ownerSM)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();
        Collection value = (Collection) ownerSM.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            // Will have been inserted in INSERT statement for table
            if (value != null)
            {
                if (mmd.getCollection().elementIsPersistent())
                {
                    // Make sure all persistable elements have StateManagers
                    for (Object elem : value)
                    {
                        if (elem != null)
                        {
                            DNStateManager elemSM = ec.findStateManager(elem);
                            if (elemSM == null || ec.getApiAdapter().getExecutionContext(elem) == null)
                            {
                                elemSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, elem, false,
                                    ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_COLLECTION_ELEMENT_PC);
                            }
                        }
                    }
                }
            }
            return;
        }

        // No elements either means null field or no elements, or specified at other side of a bidir M-N.
        if (value == null)
        {
            // Create SCO wrapper - note that this can trigger a load of the other side depending on whether the field is marked for lazy loading
            replaceFieldWithWrapper(ownerSM, null);
            return;
        }
        else if (value.isEmpty())
        {
            if (mmd.getRelationType(ec.getClassLoaderResolver()) == RelationType.MANY_TO_MANY_BI)
            {
                // Create a SCO wrapper, pass in null so it loads any from the datastore (on other side?)
                replaceFieldWithWrapper(ownerSM, null);
            }
            else
            {
                // Create a SCO wrapper passing in empty collection to avoid loading from DB (extra SQL)
                replaceFieldWithWrapper(ownerSM, value);
            }
            return;
        }

        if (mmd.isCascadePersist())
        {
            // Reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007007", IdentityUtils.getPersistableIdentityForId(ownerSM.getInternalObjectId()), mmd.getFullFieldName()));
            }
        }
        else
        {
            // Check that all elements are persistent before continuing and throw exception if necessary
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
            }

            for (Object elem : value)
            {
                if (!ec.getApiAdapter().isDetached(elem) && !ec.getApiAdapter().isPersistent(elem))
                {
                    // Element is not persistent so throw exception
                    throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), elem);
                }
            }
        }

        // Check if some elements need attaching
        boolean needsAttaching = false;
        for (Object elem : value)
        {
            if (ec.getApiAdapter().isDetached(elem))
            {
                needsAttaching = true;
                break;
            }
        }
        if (needsAttaching)
        {
            // Create a wrapper and attach the elements (and add the others)
            SCO collWrapper = replaceFieldWithWrapper(ownerSM, null);
            if (value.size() > 0)
            {
                collWrapper.attachCopy(value);

                // The attach will have put entries in the operationQueue if using optimistic, so flush them
                ec.flushOperationsForBackingStore(((BackedSCO)collWrapper).getBackingStore(), ownerSM);
            }
        }
        else
        {
            // Add the elements direct to the datastore
            ((CollectionStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, value.getClass())).addAll(ownerSM, value, 0);

            // Create a SCO wrapper with the elements loaded
            replaceFieldWithWrapper(ownerSM, value);
        }
    }

    /**
     * Method to be called after any update of the owner class element.
     * This method could be called in two situations
     * <ul>
     * <li>Update a collection field of an object by replacing the collection with a new collection, so UpdateRequest is called, which calls here</li>
     * <li>Persist a new object, and it needed to wait til the element was inserted so goes into dirty state and then flush() triggers UpdateRequest, which comes here</li>
     * </ul>
     * @param ownerSM StateManager of the owner
     */
    @Override
    public void postUpdate(DNStateManager ownerSM)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();
        Collection value = (Collection) ownerSM.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            if (value != null)
            {
                if (mmd.getCollection().elementIsPersistent())
                {
                    // Make sure all persistable elements have StateManagers
                    Object[] collElements = value.toArray();
                    for (Object collElement : collElements)
                    {
                        if (collElement != null)
                        {
                            DNStateManager elemSM = ec.findStateManager(collElement);
                            if (elemSM == null || ec.getApiAdapter().getExecutionContext(collElement) == null)
                            {
                                elemSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, collElement, false,
                                    ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_COLLECTION_ELEMENT_PC);
                            }
                        }
                    }
                }
            }
            return;
        }

        if (value == null)
        {
            // remove any elements in the collection and replace it with an empty SCO wrapper
            ((CollectionStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, null)).clear(ownerSM);
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

            CollectionStore backingStore = ((CollectionStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, value.getClass()));
            backingStore.update(ownerSM, value);

            // Replace the field with a wrapper containing these elements
            replaceFieldWithWrapper(ownerSM, value);
        }
        else
        {
            // TODO Should this throw exception if the element doesn't exist?
            // User doesn't want to update by reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007008", IdentityUtils.getPersistableIdentityForId(ownerSM.getInternalObjectId()), mmd.getFullFieldName()));
            }
            return;
        }
    }

    @Override
    public void preDelete(DNStateManager ownerSM)
    {
        if (containerIsStoredInSingleColumn())
        {
            // Field is stored with the main object so nothing to clean up
            return;
        }

        // makes sure field is loaded
        ownerSM.isLoaded(getAbsoluteFieldNumber());
        Collection value = (Collection) ownerSM.provideField(getAbsoluteFieldNumber());
        if (value == null)
        {
            return;
        }

        ExecutionContext ec = ownerSM.getExecutionContext();
        boolean dependent = mmd.getCollection().isDependentElement();
        if (mmd.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        boolean hasJoin = (mmd.getJoinMetaData() != null);
        boolean hasFK = false;
        if (!hasJoin)
        {
            if (mmd.getElementMetaData() != null && mmd.getElementMetaData().getForeignKeyMetaData() != null)
            {
                // FK collection, using <element> FK spec
                hasFK = true;
            }
            else if (mmd.getForeignKeyMetaData() != null)
            {
                // FK collection, using <field> FK spec
                hasFK = true;
            }
            AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(ec.getClassLoaderResolver());
            if (relatedMmds != null && relatedMmds[0].getForeignKeyMetaData() != null)
            {
                // FK collection (bidir), using <field> FK spec at other end
                hasFK = true;
            }
        }
        if (ec.getStringProperty(PropertyNames.PROPERTY_DELETION_POLICY).equals("JDO2"))
        {
            // JDO doesn't currently take note of foreign-key
            hasFK = false;
        }

        if (ec.getManageRelations())
        {
            ec.getRelationshipManager(ownerSM).relationChange(getAbsoluteFieldNumber(), value, null);
        }

        // TODO Why dont we just do clear here always ? The backing store should take care of if nulling or deleting etc
        if (dependent || hasJoin || !hasFK)
        {
            // Elements are either dependent (in which case we need to delete them) 
            // or there is a join (in which case we need to remove the join entries), 
            // or there are no FKs specified (in which case we need to clean up)
            if (!(value instanceof SCO))
            {
                value = (Collection)SCOUtils.wrapSCOField(ownerSM, getAbsoluteFieldNumber(), value, true);
            }
            value.clear();

            // Flush any outstanding updates
            ec.flushOperationsForBackingStore(((BackedSCO)value).getBackingStore(), ownerSM);
        }
    }
}