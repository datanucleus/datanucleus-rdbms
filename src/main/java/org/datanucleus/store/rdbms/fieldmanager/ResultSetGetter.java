/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved. 
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
2010 Andy Jefferson - added mode where we dont have StateManager, for just the PK fields
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.fieldmanager;

import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.containers.ElementContainerHandler;

/**
 * ResultSet getter implementation of a field manager, extracting field values from a ResultSet.
 */
public class ResultSetGetter extends AbstractFieldManager
{
    protected final ExecutionContext ec;
    protected final ResultSet rs;
    protected final StatementClassMapping resultMappings;

    protected DNStateManager sm;
    protected AbstractClassMetaData cmd;

    /**
     * Constructor for a ResultSet with particular result mappings and root class metadata.
     * @param ec Execution Context
     * @param rs the ResultSet
     * @param resultMappings Mappings for the results for this class
     * @param cmd Metadata for the class
     */
    public ResultSetGetter(ExecutionContext ec, ResultSet rs, StatementClassMapping resultMappings, AbstractClassMetaData cmd)
    {
        this.ec = ec;
        this.rs = rs;
        this.resultMappings = resultMappings;

        this.sm = null;
        this.cmd = cmd;
    }

    /**
     * Method to set StateManager that processing applies to.
     * This is typically called just before processing the current persistable object on the current row.
     * @param sm StateManager that we are applying to.
     */
    public void setStateManager(DNStateManager sm)
    {
        this.sm = sm;
        this.cmd = sm.getClassMetaData();
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getBoolean(ec, rs, mapIdx.getColumnPositions());
    }

    public char fetchCharField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getChar(ec, rs, mapIdx.getColumnPositions());
    }

    public byte fetchByteField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getByte(ec, rs, mapIdx.getColumnPositions());
    }

    public short fetchShortField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getShort(ec, rs, mapIdx.getColumnPositions());
    }

    public int fetchIntField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getInt(ec, rs, mapIdx.getColumnPositions());
    }

    public long fetchLongField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getLong(ec, rs, mapIdx.getColumnPositions());
    }

    public float fetchFloatField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getFloat(ec, rs, mapIdx.getColumnPositions());
    }

    public double fetchDoubleField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getDouble(ec, rs, mapIdx.getColumnPositions());
    }

    public String fetchStringField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getString(ec, rs, mapIdx.getColumnPositions());
    }

    public Object fetchObjectField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        JavaTypeMapping mapping = mapIdx.getMapping();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(ec.getClassLoaderResolver());

        Object value;
        if (mapping instanceof EmbeddedPCMapping || mapping instanceof SerialisedPCMapping || mapping instanceof SerialisedReferenceMapping)
        {
            value = mapping.getObject(ec, rs, mapIdx.getColumnPositions(), sm, fieldNumber);
        }
        else
        {
            if (mmd.isSingleCollection())
            {
                StatementClassMapping relationMappings = resultMappings.getMappingDefinitionForMemberPosition(fieldNumber);
                if (relationMappings != null)
                {
                    Class type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
                    value = processSubObjectFields(mapping, type, relationMappings);

                    ElementContainerHandler containerHandler = ec.getTypeManager().getContainerHandler(mmd.getType());
                    value = containerHandler.newContainer(mmd, value);
                }
                else
                {
                    value = mapping.getObject(ec, rs, mapIdx.getColumnPositions());
                }
            }
            else if (RelationType.isRelationSingleValued(relationType))
            {
                // Process fields of sub-object if available in this result set
                StatementClassMapping relationMappings = resultMappings.getMappingDefinitionForMemberPosition(fieldNumber);
                if (relationMappings != null)
                {
                    value = processSubObjectFields(mapping, mmd.getType(), relationMappings);
                }
                else
                {
                    value = mapping.getObject(ec, rs, mapIdx.getColumnPositions());
                }
            }
            else
            {
                value = mapping.getObject(ec, rs, mapIdx.getColumnPositions());
            }
        }

        if (sm != null)
        {
            if (relationType == RelationType.ONE_TO_ONE_BI && value != null)
            {
                // Store the value at the other side of the 1-1 BI for use later if required
                DNStateManager otherSM = ec.findStateManager(value);
                if (otherSM != null)
                {
                    AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(ec.getClassLoaderResolver());
                    if (!otherSM.isFieldLoaded(relMmds[0].getAbsoluteFieldNumber()) &&
                        relMmds[0].getType().isAssignableFrom(sm.getObject().getClass()))
                    {
                        otherSM.storeFieldValue(relMmds[0].getAbsoluteFieldNumber(), sm.getExternalObjectId());
                    }
                }
            }
            if (cmd.getSCOMutableMemberFlags()[fieldNumber])
            {
                // Wrap any SCO mutable fields
                return SCOUtils.wrapSCOField(sm, fieldNumber, value, false);
            }
            else if (RelationType.isRelationSingleValued(relationType) && (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getOwnerMember() != null))
            {
                // Embedded PC, so make sure the field is wrapped where appropriate TODO This should be part of ManagedRelationships
                sm.updateOwnerFieldInEmbeddedField(fieldNumber, value);
                return value;
            }
        }
        return value;
    }

    private Object processSubObjectFields(JavaTypeMapping mapping, Class fieldType, StatementClassMapping relationMappings)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData relatedCmd = ec.getMetaDataManager().getMetaDataForClass(fieldType, clr);
        if (mapping instanceof ReferenceMapping)
        {
            // Special case where we have a ReferenceMapping and single implementation with FK
            ReferenceMapping refMapping = (ReferenceMapping)mapping;
            if (refMapping.getMappingStrategy() == ReferenceMapping.PER_IMPLEMENTATION_MAPPING)
            {
                JavaTypeMapping[] subMappings = refMapping.getJavaTypeMapping();
                if (subMappings != null && subMappings.length == 1)
                {
                    relatedCmd = ec.getMetaDataManager().getMetaDataForClass(subMappings[0].getType(), clr);
                    fieldType = clr.classForName(subMappings[0].getType());
                }
            }
        }

        ResultObjectFactory relationROF = new PersistentClassROF(ec, rs, ec.getFetchPlan(), relationMappings, relatedCmd, fieldType);
        return relationROF.getObject();
    }
}