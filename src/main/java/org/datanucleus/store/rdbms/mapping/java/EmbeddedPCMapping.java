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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.TypeManager;

/**
 * Mapping for a PC object embedded within another PC object (1-1 relation).
 * Provides mapping for a single Java type (the PC type) to multiple datastore columns.
 * Allows for nested embedded fields.
 * Implements MappingCallbacks since if we are embedding a MappingCallbacks field (e.g a BLOB on Oracle)
 * then we need in turn to call the underlying MappingCallbacks methods.
 **/
public class EmbeddedPCMapping extends EmbeddedMapping implements MappingCallbacks
{
    /**
     * Initialise this JavaTypeMapping with the given DatastoreAdapter for the given metadata.
     * @param mmd FieldMetaData for the field to be mapped (if any)
     * @param table The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
        initialize(mmd, table, clr, mmd.getEmbeddedMetaData(), mmd.getTypeName(), PersistableObjectType.EMBEDDED_PC);
    }

    /**
     * MappingCallback called when the owning object is being fetched.
     * @param sm StateManager of the owning object
     */
    public void postFetch(DNStateManager sm)
    {
        // Find the OP for the embedded PC object
        DNStateManager thisOP = getStateManagerForEmbeddedObject(sm);
        if (thisOP == null)
        {
            return;
        }

        for (int i=0;i<getNumberOfJavaTypeMappings();i++)
        {
            JavaTypeMapping m = getJavaTypeMapping(i);
            if (m instanceof MappingCallbacks)
            {
                ((MappingCallbacks)m).postFetch(thisOP);
            }
        }
    }

    /**
     * MappingCallback called when the owning object has just being inserted.
     * @param sm StateManager of the owning object
     */
    public void postInsert(DNStateManager sm)
    {
        // Find the OP for the embedded PC object
        DNStateManager thisOP = getStateManagerForEmbeddedObject(sm);
        if (thisOP == null)
        {
            return;
        }
 
        // Call postInsert on any MappingCallbacks components
        for (int i=0;i<getNumberOfJavaTypeMappings();i++)
        {
            JavaTypeMapping m = getJavaTypeMapping(i);
            if (m instanceof MappingCallbacks)
            {
                ((MappingCallbacks)m).postInsert(thisOP);
            }
        }
    }

    /**
     * MappingCallback called when the owning object has just being udpated.
     * @param sm StateManager of the owning object
     */
    public void postUpdate(DNStateManager sm)
    {
        // Find the OP for the embedded PC object
        DNStateManager thisOP = getStateManagerForEmbeddedObject(sm);
        if (thisOP == null)
        {
            return;
        }

        // Call postUpdate on any MappingCallbacks components
        for (int i=0;i<getNumberOfJavaTypeMappings();i++)
        {
            JavaTypeMapping m = getJavaTypeMapping(i);
            if (m instanceof MappingCallbacks)
            {
                ((MappingCallbacks)m).postUpdate(thisOP);
            }
        }
    }

    /**
     * MappingCallback called when the owning object is about to be deleted.
     * @param sm StateManager of the owning object
     */
    public void preDelete(DNStateManager sm)
    {
        // Find the OP for the embedded PC object
        DNStateManager thisOP = getStateManagerForEmbeddedObject(sm);
        if (thisOP == null)
        {
            return;
        }

        // Call preDelete on any MappingCallbacks components
        for (int i=0;i<getNumberOfJavaTypeMappings();i++)
        {
            JavaTypeMapping m = getJavaTypeMapping(i);
            if (m instanceof MappingCallbacks)
            {
                ((MappingCallbacks)m).preDelete(thisOP);
            }
        }
    }

    /**
     * Accessor for StateManager of the embedded PC object when provided with the owner object.
     * @param ownerSM StateManager of the owner
     * @return StateManager of the embedded object
     */
    private DNStateManager getStateManagerForEmbeddedObject(DNStateManager ownerSM)
    {
        AbstractMemberMetaData theMmd = getRealMemberMetaData();

        Object value = ownerSM.provideField(theMmd.getAbsoluteFieldNumber()); // Owner (non-embedded) PC
        TypeManager typeManager = ownerSM.getExecutionContext().getTypeManager();
        value = mmd.isSingleCollection() ? typeManager.getContainerAdapter(value).iterator().next() : value;
        if (value == null)
        {
            return null;
        }

        ExecutionContext ec = ownerSM.getExecutionContext();
        DNStateManager thisOP = ec.findStateManager(value);
        if (thisOP == null)
        {
            // Assign a StateManager to manage our embedded object
            thisOP = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, value, false, ownerSM, theMmd.getAbsoluteFieldNumber(), objectType);
        }

        return thisOP;
    }
}