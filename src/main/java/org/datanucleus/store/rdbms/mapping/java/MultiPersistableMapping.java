/**********************************************************************
Copyright (c) 2012 Andy Jefferson and others. All rights reserved.
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NotYetFlushedException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Extension of MultiMapping where the actual mapping represents multiple possible persistable types
 * such as with an interface/reference field. For example, with an interface mapping we have say 3 known
 * implementations of the interface, so this mapping represents 3 FKs. A maximum of one will be set
 * and the others null.
 */
public abstract class MultiPersistableMapping extends MultiMapping
{
    /**
     * Convenience accessor for the number of the java type mapping where the passed value would be
     * stored. If no suitable mapping is found will return -1. If is a persistent interface then will
     * return -2 meaning persist against *any* mapping
     * @param ec ExecutionContext
     * @param value The value
     * @return The index of javaTypeMappings to use (if any), or -1 (none), or -2 (any)
     */
    protected int getMappingNumberForValue(ExecutionContext ec, Object value)
    {
        if (value == null)
        {
            return -1;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();

        // Find the appropriate mapping
        for (int i=0; i<javaTypeMappings.length; i++)
        {
            Class cls = clr.classForName(javaTypeMappings[i].getType());
            if (cls.isAssignableFrom(value.getClass()))
            {
                return i;
            }
        }

        // PERSISTENT INTERFACE : allow for the value (impl) not be directly assignable from the superclass impl
        // e.g If we have interface "Base" with impl "BaseImpl", and sub-interface "Sub1" with impl "Sub1Impl"
        // So if the mapping is of type BaseImpl and the value is Sub1Impl then they don't come up as "assignable"
        // but they are
        Class mappingJavaType = null;
        MetaDataManager mmgr = storeMgr.getNucleusContext().getMetaDataManager();
        boolean isPersistentInterface = mmgr.isPersistentInterface(getType());
        if (isPersistentInterface)
        {
            // Field is declared as a "persistent-interface" type so all impls of that type should match
            mappingJavaType = clr.classForName(getType());
        }
        else if (mmd != null && mmd.getFieldTypes() != null && mmd.getFieldTypes().length == 1)
        {
            isPersistentInterface = mmgr.isPersistentInterface(mmd.getFieldTypes()[0]);
            if (isPersistentInterface)
            {
                // Field is declared as interface and accepts "persistent-interface" value, so all impls should match
                mappingJavaType = clr.classForName(mmd.getFieldTypes()[0]);
            }
        }
        if (mappingJavaType != null && mappingJavaType.isAssignableFrom(value.getClass()))
        {
            return -2; // Persistent interface persistable in all mappings. Should only be 1 anyway
        }

        return -1;
    }

    /**
     * Method to set the parameters in the PreparedStatement with the fields of this object.
     * @param ec execution context
     * @param ps The PreparedStatement
     * @param pos The parameter positions
     * @param value The object to populate the statement with
     * @throws NotYetFlushedException Thrown if the object is not yet flushed to the datastore
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] pos, Object value)
    {
        setObject(ec, ps, pos, value, null, -1);
    }

    /**
     * Sets the specified positions in the PreparedStatement associated with this field, and value.
     * If the number of positions in "pos" is not the same as the number of datastore mappings then it is assumed
     * that we should only set the positions for the real implementation FK; this happens where we have a statement
     * like "... WHERE IMPL1_ID_OID = ? AND IMPL2_ID_OID IS NULL" so we need to filter on the other implementations
     * being null and only want to input parameter(s) for the real implementation of "value".
     * @param ec execution context
     * @param ps a datastore object that executes statements in the database
     * @param value the value stored in this field
     * @param ownerSM the owner StateManager
     * @param ownerFieldNumber the owner absolute field number
     * @param pos The position(s) of the PreparedStatement to populate
     */
    @Override
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] pos, Object value, DNStateManager ownerSM, int ownerFieldNumber)
    {
        boolean setValueFKOnly = false;
        if (pos != null && pos.length < getNumberOfColumnMappings())
        {
            setValueFKOnly = true;
        }

        // Make sure that this field has a sub-mapping appropriate for the specified value
        int javaTypeMappingNumber = getMappingNumberForValue(ec, value);
        if (value != null && javaTypeMappingNumber == -1)
        {
            // as required by the JDO spec, a ClassCastException is thrown since not valid implementation
            // TODO Change this to a multiple field mapping localised message
            throw new ClassCastException(Localiser.msg("041044", mmd != null ? mmd.getFullFieldName() : "", getType(), value.getClass().getName()));
        }

        if (value != null)
        {
            ApiAdapter api = ec.getApiAdapter();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();

            // Make sure the value is persisted if it is persistable in its own right
            if (!ec.isInserting(value))
            {
                // Object either already exists, or is not yet being inserted.
                Object id = api.getIdForObject(value);

                // Check if the persistable exists in this datastore
                boolean requiresPersisting = false;
                if (ec.getApiAdapter().isDetached(value) && ownerSM != null)
                {
                    // Detached object that needs attaching (or persisting if detached from a different datastore)
                    requiresPersisting = true;
                }
                else if (id == null)
                {
                    // Transient object, so we need to persist it
                    requiresPersisting = true;
                }
                else
                {
                    ExecutionContext valueEC = api.getExecutionContext(value);
                    if (valueEC != null && ec != valueEC)
                    {
                        throw new NucleusUserException(Localiser.msg("041015"), id);
                    }
                }

                if (requiresPersisting)
                {
                    // The object is either not yet persistent or is detached and so needs attaching
                    Object pcNew = ec.persistObjectInternal(value, null, PersistableObjectType.PC);
                    ec.flushInternal(false);
                    id = api.getIdForObject(pcNew);
                    if (ec.getApiAdapter().isDetached(value) && ownerSM != null)
                    {
                        // Update any detached reference to refer to the attached variant
                        ownerSM.replaceFieldMakeDirty(ownerFieldNumber, pcNew);
                        if (mmd != null)
                        {
                            RelationType relationType = mmd.getRelationType(clr);
                            if (relationType == RelationType.ONE_TO_ONE_BI)
                            {
                                DNStateManager relatedSM = ec.findStateManager(pcNew);
                                AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                                // TODO Allow for multiple related fields
                                relatedSM.replaceFieldMakeDirty(relatedMmds[0].getAbsoluteFieldNumber(), ownerSM.getObject());
                            }
                            else if (relationType == RelationType.MANY_TO_ONE_BI)
                            {
                                // TODO Update the container element with the attached variant
                                if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                                {
                                    NucleusLogger.PERSISTENCE.debug("PCMapping.setObject : object " + ownerSM.getInternalObjectId() + " has field " + ownerFieldNumber +
                                            " that is 1-N bidirectional - should really update the reference in the relation. Not yet supported");
                                }
                            }
                        }
                    }
                }

                if (getNumberOfColumnMappings() <= 0)
                {
                    // If the field doesn't map to any columns, omit the set process
                    return;
                }
            }
        }

        if (pos == null)
        {
            return;
        }

        DNStateManager sm = (value != null ? ec.findStateManager(value) : null);
        try
        {
            if (sm != null)
            {
                sm.setStoringPC();
            }

            int n = 0;
            NotYetFlushedException notYetFlushed = null;
            for (int i=0; i<javaTypeMappings.length; i++)
            {
                // Set the PreparedStatement positions for this implementation mapping
                int[] posMapping;
                if (setValueFKOnly)
                {
                    // Only using first "pos" value(s)
                    n = 0;
                }
                else if (n >= pos.length)
                {
                    n = 0; // store all implementations to the same columns, so we reset the index
                }
                if (javaTypeMappings[i].getReferenceMapping() != null)
                {
                    posMapping = new int[javaTypeMappings[i].getReferenceMapping().getNumberOfColumnMappings()];
                }
                else
                {
                    posMapping = new int[javaTypeMappings[i].getNumberOfColumnMappings()];
                }
                for (int j=0; j<posMapping.length; j++)
                {
                    posMapping[j] = pos[n++];
                }

                try
                {
                    if (javaTypeMappingNumber == -2 || (value != null && javaTypeMappingNumber == i))
                    {
                        // This mapping is where the value is to be stored, or using persistent interfaces
                        javaTypeMappings[i].setObject(ec, ps, posMapping, value);
                    }
                    else if (!setValueFKOnly)
                    {
                        // Set null for this mapping, since the value is null or is for something else
                        javaTypeMappings[i].setObject(ec, ps, posMapping, null);
                    }
                }
                catch (NotYetFlushedException e)
                {
                    notYetFlushed = e;
                }
            }
            if (notYetFlushed != null)
            {
                throw notYetFlushed;
            }
        }
        finally
        {
            if (sm != null)
            {
                sm.unsetStoringPC();
            }
        }
    }

    /**
     * Method to retrieve an object of this type from the ResultSet.
     * @param ec execution context
     * @param rs The ResultSet
     * @param pos The parameter positions
     * @return The object
     */
    public Object getObject(ExecutionContext ec, final ResultSet rs, int[] pos)
    {
        // Go through the possible types for this field and find a non-null value (if there is one)
        int n = 0;
        for (int i=0; i<javaTypeMappings.length; i++)
        {
            int[] posMapping;
            if (n >= pos.length)
            {
                //this means we store all implementations to the same columns, so we reset the index
                n = 0;
            }

            if (javaTypeMappings[i].getReferenceMapping() != null)
            {
                posMapping = new int[javaTypeMappings[i].getReferenceMapping().getNumberOfColumnMappings()];
            }
            else
            {
                posMapping = new int[javaTypeMappings[i].getNumberOfColumnMappings()];
            }
            for (int j=0; j<posMapping.length; j++)
            {
                posMapping[j] = pos[n++];
            }

            Object value = null;
            try
            {
                // Retrieve the value (PC object) for this mappings' object
                value = javaTypeMappings[i].getObject(ec, rs, posMapping);
                if (value != null)
                {
                    if (IdentityUtils.isDatastoreIdentity(value))
                    {
                        // What situation is this catering for exactly ?
                        Column col = null;
                        if (javaTypeMappings[i].getReferenceMapping() != null)
                        {
                            col = javaTypeMappings[i].getReferenceMapping().getColumnMapping(0).getColumn();
                        }
                        else
                        {
                            col = javaTypeMappings[i].getColumnMapping(0).getColumn();
                        }
                        String className = col.getStoredJavaType();
                        value = ec.getNucleusContext().getIdentityManager().getDatastoreId(className, IdentityUtils.getTargetKeyForDatastoreIdentity(value));
                        return ec.findObject(value, false, true, null);
                    }
                    else if (ec.getClassLoaderResolver().classForName(getType()).isAssignableFrom(value.getClass()))                
                    {
                        return value;
                    }
                }
            }
            catch (NullValueException e)
            {
                // expected if implementation object is null and has primitive fields in the primary key
            }
            catch (NucleusObjectNotFoundException onfe)
            {
                // expected, will try next implementation
            }
        }
        return null;
    }
}