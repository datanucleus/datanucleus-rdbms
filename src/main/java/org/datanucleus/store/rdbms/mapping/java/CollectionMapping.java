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
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.scostore.CollectionStore;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.SCOContainer;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.wrappers.backed.BackedSCO;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping for Collection/Set/List types.
 */
public class CollectionMapping extends AbstractContainerMapping implements MappingCallbacks
{
    /**
     * Accessor for the Java type represented here.
     * @return The java type
     */
    public Class getJavaType()
    {
        return Collection.class;
    }

    // --------------- Implementation of MappingCallbacks -------------------

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
        Collection value = (Collection) ownerOP.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            // Make sure the elements are ok for proceeding
            SCOUtils.validateObjectsForWriting(ec, value);
            return;
        }

        if (value == null)
        {
            // replace null collections with an empty SCO wrapper
            replaceFieldWithWrapper(ownerOP, null, false, false);
            return;
        }

        Object[] collElements = value.toArray();
        if (!mmd.isCascadePersist())
        {
            // Field doesnt support cascade-persist so no reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
            }

            // Check for any persistable elements that arent persistent
            for (int i=0;i<collElements.length;i++)
            {
                if (!ec.getApiAdapter().isDetached(collElements[i]) &&
                    !ec.getApiAdapter().isPersistent(collElements[i]))
                {
                    // Element is not persistent so throw exception
                    throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), collElements[i]);
                }
            }
            replaceFieldWithWrapper(ownerOP, value, false, false);
        }
        else
        {
            // Reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007007", mmd.getFullFieldName()));
            }

            // Check if some elements need attaching
            // TODO Investigate if we can just use the attachCopy route below and skip off this check
            boolean needsAttaching = false;
            for (int i=0;i<collElements.length;i++)
            {
                if (ownerOP.getExecutionContext().getApiAdapter().isDetached(collElements[i]))
                {
                    needsAttaching = true;
                    break;
                }
            }

            if (needsAttaching)
            {
                // Create a wrapper and attach the elements (and add the others)
                SCO collWrapper = replaceFieldWithWrapper(ownerOP, null, false, false);
                collWrapper.attachCopy(value);
            }
            else
            {
                if (value.size() > 0)
                {
                    // Add the elements direct to the datastore
                    ((CollectionStore) storeMgr.getBackingStoreForField(ownerOP.getExecutionContext().getClassLoaderResolver(),mmd, value.getClass())).addAll(ownerOP, value, 0);

                    // Create a SCO wrapper with the elements loaded
                    replaceFieldWithWrapper(ownerOP, value, false, false);
                }
                else
                {
                    if (mmd.getRelationType(ownerOP.getExecutionContext().getClassLoaderResolver()) == RelationType.MANY_TO_MANY_BI)
                    {
                        // Create a SCO wrapper, pass in null so it loads any from the datastore (on other side?)
                        replaceFieldWithWrapper(ownerOP, null, false, false);
                    }
                    else
                    {
                        // Create a SCO wrapper, pass in empty collection to avoid loading from DB (extra SQL)
                        replaceFieldWithWrapper(ownerOP, value, false, false);
                    }
                }
            }
        }
    }

    /**
     * Method to be called after any update of the owner class element.
     * This method could be called in two situations
     * <ul>
     * <li>Update a collection field of an object by replacing the collection with a new collection, 
     * so UpdateRequest is called, which calls here</li>
     * <li>Persist a new object, and it needed to wait til the element was inserted so
     * goes into dirty state and then flush() triggers UpdateRequest, which comes here</li>
     * </ul>
     * @param ownerOP ObjectProvider of the owner
     */
    public void postUpdate(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        Collection value = (Collection) ownerOP.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            // Make sure the elements are ok for proceeding
            SCOUtils.validateObjectsForWriting(ec, value);
            return;
        }

        if (value == null)
        {
            // remove any elements in the collection and replace it with an empty SCO wrapper
            ((CollectionStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, null)).clear(ownerOP);
            replaceFieldWithWrapper(ownerOP, null, false, false);
            return;
        }

        if (value instanceof SCOContainer)
        {
            // Already have a SCO value
            SCOContainer sco = (SCOContainer) value;
            if (ownerOP.getObject() == sco.getOwner() && mmd.getName().equals(sco.getFieldName()))
            {
                // Flush any outstanding updates
                ownerOP.getExecutionContext().flushOperationsForBackingStore(((BackedSCO)sco).getBackingStore(), ownerOP);

                return;
            }

            if (sco.getOwner() != null)
            {
                throw new NucleusException(Localiser.msg("CollectionMapping.WrongOwnerError")).setFatal();
            }
        }

        if (!mmd.isCascadeUpdate())
        {
            // TODO Should this throw exception if the element doesn't exist?
            // User doesn't want to update by reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007008", mmd.getFullFieldName()));
            }
            return;
        }
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("007009", mmd.getFullFieldName()));
        }

        CollectionStore backingStore = ((CollectionStore) storeMgr.getBackingStoreForField(
            ec.getClassLoaderResolver(), mmd, value.getClass()));
        backingStore.update(ownerOP, value);

        // Replace the field with a wrapper containing these elements
        replaceFieldWithWrapper(ownerOP, value, false, false);
    }

    /**
     * Method to be called before any delete of the owner class element.
     * @param ownerOP ObjectProvider of the owner
     */
    public void preDelete(ObjectProvider ownerOP)
    {
        if (containerIsStoredInSingleColumn())
        {
            // Field is stored with the main object so nothing to clean up
            return;
        }

        // makes sure field is loaded
        ownerOP.isLoaded(getAbsoluteFieldNumber());
        Collection value = (Collection) ownerOP.provideField(getAbsoluteFieldNumber());
        if (value == null)
        {
            return;
        }

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
            AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(ownerOP.getExecutionContext().getClassLoaderResolver());
            if (relatedMmds != null && relatedMmds[0].getForeignKeyMetaData() != null)
            {
                // FK collection (bidir), using <field> FK spec at other end
                hasFK = true;
            }
        }
        if (ownerOP.getExecutionContext().getStringProperty(PropertyNames.PROPERTY_DELETION_POLICY).equals("JDO2"))
        {
            // JDO2 doesn't currently (2.0 spec) take note of foreign-key
            hasFK = false;
        }

        if (ownerOP.getExecutionContext().getManageRelations())
        {
            ownerOP.getExecutionContext().getRelationshipManager(ownerOP).relationChange(getAbsoluteFieldNumber(), value, null);
        }

        // TODO Why dont we just do clear here always ? THe backing store should take care of if nulling or deleting etc
        if (dependent || hasJoin || !hasFK)
        {
            // Elements are either dependent (in which case we need to delete them) or theres a join (in which case
            // we need to remove the join entries), or there are no FKs specified (in which case we need to clean up)
            if (!(value instanceof SCO))
            {
                value = (Collection)ownerOP.wrapSCOField(getAbsoluteFieldNumber(), value, false, false, true);
            }
            value.clear();

            // Flush any outstanding updates
            ownerOP.getExecutionContext().flushOperationsForBackingStore(((BackedSCO)value).getBackingStore(), ownerOP);
        }
    }
}