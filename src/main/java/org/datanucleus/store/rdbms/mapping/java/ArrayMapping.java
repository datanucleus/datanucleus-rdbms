/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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

import java.lang.reflect.Array;
import java.util.List;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.scostore.ArrayStore;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping for an array.
 */
public class ArrayMapping extends AbstractContainerMapping implements MappingCallbacks
{
    /**
     * Accessor for the Java type represented here.
     * @return The java type
     */
    public Class getJavaType()
    {
        if (mmd != null)
        {
            return mmd.getType();
        }
        return null;
    }

    /**
     * Convenience method to return if the array is stored in the owning table as a column.
     * Overrides the superclass since arrays can be stored in a single column also when the no join is
     * specified and the array is of a primitive/wrapper type.
     * @return Whether it is stored in a single column in the main table.
     */
    protected boolean containerIsStoredInSingleColumn()
    {
        if (super.containerIsStoredInSingleColumn())
        {
            return true;
        }
        if (mmd != null && mmd.hasArray() && mmd.getJoinMetaData() == null && MetaDataUtils.getInstance().arrayStorableAsByteArrayInSingleColumn(mmd))
        {
            return true;
        }
        return false;
    }

    // ---------------------- Implementation of MappingCallbacks ----------------------------------

    public void insertPostProcessing(ObjectProvider op)
    {
    }

    /**
     * Method to be called after the insert of the owner class element.
     * @param ownerOP ObjectProvider of the owner
     **/
    public void postInsert(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        Object value = ownerOP.provideField(getAbsoluteFieldNumber());
        if (value == null)
        {
            return;
        }

        if (containerIsStoredInSingleColumn())
        {
            if (mmd.getArray().elementIsPersistent())
            {
                // Make sure all persistable elements have ObjectProviders
                Object[] arrElements = (Object[])value;
                for (Object elem : arrElements)
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
            return;
        }

        if (!mmd.isCascadePersist())
        {
            // Field doesnt support cascade-persist so no reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
            }

            // Check for any persistable keys/values that arent persistent
            if (!mmd.getType().getComponentType().isPrimitive())
            {
                Object[] array = (Object[])value;
                for (int i=0;i<array.length;i++)
                {
                    if (!ec.getApiAdapter().isDetached(array[i]) && !ec.getApiAdapter().isPersistent(array[i]))
                    {
                        // Element is not persistent so throw exception
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), array[i]);
                    }
                }
            }
        }
        else
        {
            // Reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("007007", mmd.getFullFieldName()));
            }

            // Insert the array
            ((ArrayStore) storeMgr.getBackingStoreForField(ownerOP.getExecutionContext().getClassLoaderResolver(),mmd, null)).set(ownerOP, value);
        }
    }

    /**
     * Method to be called after any fetch of the owner class element.
     * @param op ObjectProvider of the owner
     */
    public void postFetch(ObjectProvider op)
    {
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when stored in a single column since we are handled in the main request
            return;
        }

        List elements = ((ArrayStore) storeMgr.getBackingStoreForField(op.getExecutionContext().getClassLoaderResolver(),mmd,null)).getArray(op);
        if (elements != null)
        {
            boolean primitiveArray = mmd.getType().getComponentType().isPrimitive();
            Object array = Array.newInstance(mmd.getType().getComponentType(), elements.size());
            for (int i=0;i<elements.size();i++)
            {
                Object element = elements.get(i);
                if (primitiveArray)
                {
                    // Handle the conversion back to the primitive
                    if (element instanceof Boolean)
                    {
                        Array.setBoolean(array, i, ((Boolean)element).booleanValue());
                    }
                    else if (element instanceof Byte)
                    {
                        Array.setByte(array, i, ((Byte)element).byteValue());
                    }
                    else if (element instanceof Character)
                    {
                        Array.setChar(array, i, ((Character)element).charValue());
                    }
                    else if (element instanceof Double)
                    {
                        Array.setDouble(array, i, ((Double)element).doubleValue());
                    }
                    else if (element instanceof Float)
                    {
                        Array.setFloat(array, i, ((Float)element).floatValue());
                    }
                    else if (element instanceof Integer)
                    {
                        Array.setInt(array, i, ((Integer)element).intValue());
                    }
                    else if (element instanceof Long)
                    {
                        Array.setLong(array, i, ((Long)element).longValue());
                    }
                    else if (element instanceof Short)
                    {
                        Array.setShort(array, i, ((Short)element).shortValue());
                    }
                }
                else
                {
                    Array.set(array, i, element);
                }
            }
            if (elements.size() == 0)
            {
                op.replaceFieldMakeDirty(getAbsoluteFieldNumber(), null);
            }
            else
            {
                op.replaceFieldMakeDirty(getAbsoluteFieldNumber(), array);
            }
        }
        else
        {
            op.replaceFieldMakeDirty(getAbsoluteFieldNumber(), null);
        }
    }

    /**
     * Method to be called after any update of the owner class element.
     * This method could be called in two situations
     * <ul>
     * <li>Update an array field of an object by replacing the array with a new array, 
     * so UpdateRequest is called, which calls here</li>
     * <li>Persist a new object, and it needed to wait til the element was inserted so
     * goes into dirty state and then flush() triggers UpdateRequest, which comes here</li>
     * </ul>
     * @param ownerOP ObjectProvider of the owner
     */
    public void postUpdate(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        Object value = ownerOP.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            if (value != null)
            {
                if (mmd.getArray().elementIsPersistent())
                {
                    // Make sure all persistable elements have ObjectProviders
                    Object[] arrElements = (Object[])value;
                    for (Object elem : arrElements)
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
            return;
        }

        if (value == null)
        {
            // array is now null so remove any elements in the array
            ((ArrayStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(),mmd,null)).clear(ownerOP);
            return;
        }

        if (!mmd.isCascadeUpdate())
        {
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

        // Update the datastore
        // TODO Do this more efficiently, removing elements no longer present, and adding new ones
        ArrayStore backingStore = (ArrayStore) storeMgr.getBackingStoreForField(ec.getClassLoaderResolver(), mmd, null);
        backingStore.clear(ownerOP);
        backingStore.set(ownerOP, value);
    }

    /**
     * Method to be called before any delete of the owner class element, if the field in the owner is dependent
     * @param op ObjectProvider of the owner
     */
    public void preDelete(ObjectProvider op)
    {
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when stored in a single column since we are handled in the main request
            return;
        }

        // makes sure field is loaded
        op.isLoaded(getAbsoluteFieldNumber());
        Object value = op.provideField(getAbsoluteFieldNumber());
        if (value == null)
        {
            return;
        }

        // Clear the array via its backing store
        ArrayStore backingStore = (ArrayStore) storeMgr.getBackingStoreForField(op.getExecutionContext().getClassLoaderResolver(), mmd, null);
        backingStore.clear(op);
    }
}