/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.fieldmanager;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.rdbms.mapping.java.InterfaceMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.ElementContainerTable;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Field manager that is used to check the values in fields in order to detect "new" classes
 * that impact on the datastore schema, hence allowing dynamic schema updates.
 */
public class DynamicSchemaFieldManager extends AbstractFieldManager
{
    /** Manager for the RDBMS datastore. */
    RDBMSStoreManager rdbmsMgr;

    /** ObjectProvider of the object being processed. */
    ObjectProvider op;

    /** Flag for whether we have updated the schema. */
    boolean schemaUpdatesPerformed = false;

    /**
     * Constructor.
     * @param rdbmsMgr RDBMSManager
     * @param op StateManager for the object being processed
     */
    public DynamicSchemaFieldManager(RDBMSStoreManager rdbmsMgr, ObjectProvider op)
    {
        this.rdbmsMgr = rdbmsMgr;
        this.op = op;
    }

    /**
     * Accessor for whether this field manager has made updates to the schema.
     * @return Whether updates have been made.
     */
    public boolean hasPerformedSchemaUpdates()
    {
        return schemaUpdatesPerformed;
    }

    /**
     * Method to store an object field into the attached instance.
     * @param fieldNumber Number of the field to store
     * @param value the value in the detached instance
     */
    public void storeObjectField(int fieldNumber, Object value)
    {
        if (value == null)
        {
            return; // No value so nothing to do
        }

        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();

        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd != null)
        {
            DatastoreClass table = rdbmsMgr.getDatastoreClass(op.getObject().getClass().getName(), clr);
            JavaTypeMapping fieldMapping = table.getMemberMapping(mmd);
            if (fieldMapping != null)
            {
                if (fieldMapping instanceof InterfaceMapping)
                {
                    // 1-1 Interface field
                    InterfaceMapping intfMapping = (InterfaceMapping)fieldMapping;
                    if (mmd.getFieldTypes() != null || mmd.hasExtension(MetaData.EXTENSION_MEMBER_IMPLEMENTATION_CLASSES))
                    {
                        // Field is defined to not accept this type so just return
                        return;
                    }

                    processInterfaceMappingForValue(intfMapping, value, mmd, ec);
                }
                else if (mmd.hasCollection() || mmd.hasArray())
                {
                    boolean hasJoin = false;
                    if (mmd.getJoinMetaData() != null)
                    {
                        hasJoin = true;
                    }
                    else
                    {
                        AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                        if (relMmds != null && relMmds[0].getJoinMetaData() != null)
                        {
                            hasJoin = true;
                        }
                    }
                    if (!hasJoin)
                    {
                        // Not join table so no supported schema updates
                        return;
                    }

                    Table joinTbl = fieldMapping.getStoreManager().getTable(mmd);
                    ElementContainerTable collTbl = (ElementContainerTable)joinTbl;
                    JavaTypeMapping elemMapping = collTbl.getElementMapping();
                    if (elemMapping instanceof InterfaceMapping)
                    {
                        InterfaceMapping intfMapping = (InterfaceMapping)elemMapping;
                        if (mmd.hasCollection())
                        {
                            Collection coll = (Collection)value;
                            if (coll.isEmpty())
                            {
                                return;
                            }

                            // Update value mapping using first element. Maybe we should do the same for all elements?
                            Object elementValue = coll.iterator().next();
                            processInterfaceMappingForValue(intfMapping, elementValue, mmd, ec);
                        }
                        else if (mmd.hasArray())
                        {
                            if (Array.getLength(value) == 0)
                            {
                                return;
                            }

                            // Update value mapping using first element. Maybe we should do the same for all elements?
                            Object elementValue = Array.get(value,  0);
                            processInterfaceMappingForValue(intfMapping, elementValue, mmd, ec);
                        }
                    }
                }
                else if (mmd.hasMap())
                {
                    boolean hasJoin = false;
                    if (mmd.getJoinMetaData() != null)
                    {
                        hasJoin = true;
                    }
                    else
                    {
                        AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                        if (relMmds != null && relMmds[0].getJoinMetaData() != null)
                        {
                            hasJoin = true;
                        }
                    }
                    if (!hasJoin)
                    {
                        // Not join table so no supported schema updates
                        return;
                    }

                    Map map = (Map)value;
                    if (map.isEmpty())
                    {
                        return;
                    }

                    Table joinTbl = fieldMapping.getStoreManager().getTable(mmd);
                    MapTable mapTbl = (MapTable)joinTbl;
                    JavaTypeMapping keyMapping = mapTbl.getKeyMapping();
                    if (keyMapping instanceof InterfaceMapping)
                    {
                        // Update key mapping using first key. Maybe we should do the same for all keys?
                        InterfaceMapping intfMapping = (InterfaceMapping)keyMapping;
                        Object keyValue = map.keySet().iterator().next();
                        processInterfaceMappingForValue(intfMapping, keyValue, mmd, ec);
                    }
                    JavaTypeMapping valMapping = mapTbl.getValueMapping();
                    if (valMapping instanceof InterfaceMapping)
                    {
                        // Update value mapping using first value. Maybe we should do the same for all values?
                        InterfaceMapping intfMapping = (InterfaceMapping)valMapping;
                        Object valValue = map.values().iterator().next();
                        processInterfaceMappingForValue(intfMapping, valValue, mmd, ec);
                    }
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeBooleanField(int, boolean)
     */
    public void storeBooleanField(int fieldNumber, boolean value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeByteField(int, byte)
     */
    public void storeByteField(int fieldNumber, byte value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeCharField(int, char)
     */
    public void storeCharField(int fieldNumber, char value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeDoubleField(int, double)
     */
    public void storeDoubleField(int fieldNumber, double value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeFloatField(int, float)
     */
    public void storeFloatField(int fieldNumber, float value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeIntField(int, int)
     */
    public void storeIntField(int fieldNumber, int value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeLongField(int, long)
     */
    public void storeLongField(int fieldNumber, long value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeShortField(int, short)
     */
    public void storeShortField(int fieldNumber, short value)
    {
    }

    /*
     * (non-Javadoc)
     * @see FieldConsumer#storeStringField(int, java.lang.String)
     */
    public void storeStringField(int fieldNumber, String value)
    {
    }

    protected void processInterfaceMappingForValue(InterfaceMapping intfMapping, Object value, AbstractMemberMetaData mmd, ExecutionContext ec)
    {
        if (intfMapping.getMappingStrategy() == ReferenceMapping.PER_IMPLEMENTATION_MAPPING)
        {
            int intfImplMappingNumber = intfMapping.getMappingNumberForValue(ec, value);
            if (intfImplMappingNumber == -1)
            {
                if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug("Dynamic schema updates : field=" + mmd.getFullFieldName() + 
                        " has an interface mapping yet " + StringUtils.toJVMIDString(value) + " is not a known implementation - trying to update the schema ...");
                }

                // Make sure the metadata for this value class is loaded (may be first encounter)
                MetaDataManager mmgr = ec.getNucleusContext().getMetaDataManager();
                ClassLoaderResolver clr = ec.getClassLoaderResolver();
                mmgr.getMetaDataForClass(value.getClass(), clr);

                String[] impls = ec.getMetaDataManager().getClassesImplementingInterface(intfMapping.getType(), clr);
                if (ClassUtils.stringArrayContainsValue(impls, value.getClass().getName()))
                {
                    // Value is a valid implementation yet there is no mapping so re-initialize the mapping
                    try
                    {
                        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug("Dynamic schema updates : field=" + mmd.getFullFieldName() + " has a new implementation available so reinitialising its mapping");
                        }
                        intfMapping.initialize(mmd, intfMapping.getTable(), clr);
                        intfMapping.getStoreManager().validateTable((TableImpl)intfMapping.getTable(), clr);
                    }
                    catch (Exception e)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug("Exception thrown trying to create missing columns for implementation", e);
                        throw new NucleusException("Exception thrown performing dynamic update of schema", e);
                    }
                    schemaUpdatesPerformed = true;
                }
            }
        }
    }
}