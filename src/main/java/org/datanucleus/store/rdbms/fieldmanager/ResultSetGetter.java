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
2010 Andy Jefferson - added mode where we dont have the ObjectProvider, for just the PK fields
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.fieldmanager;

import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.types.SCOUtils;

/**
 * ResultSet getter implementation of a field manager.
 */
public class ResultSetGetter extends AbstractFieldManager
{
    private final RDBMSStoreManager storeMgr;
    private final ObjectProvider op;
    private final AbstractClassMetaData cmd;
    private final ExecutionContext ec;
    private final ResultSet resultSet;
    private final StatementClassMapping resultMappings;

    /**
     * Constructor where we know the object to put the field values in.
     * @param storeMgr RDBMS StoreManager
     * @param op ObjectProvider where we are putting the results
     * @param rs the ResultSet
     * @param resultMappings Mappings for the results for this class
     */
    public ResultSetGetter(RDBMSStoreManager storeMgr, ObjectProvider op, ResultSet rs, 
            StatementClassMapping resultMappings)
    {
        this.storeMgr = storeMgr;
        this.op = op;
        this.cmd = op.getClassMetaData();
        this.ec = op.getExecutionContext();
        this.resultSet = rs;
        this.resultMappings = resultMappings;
    }

    /**
     * Constructor without the ObjectProvider, where we know the result set but don't have the object yet.
     * @param storeMgr RDBMS StoreManager
     * @param ec Execution Context
     * @param rs the ResultSet
     * @param resultMappings Mappings for the results for this class
     * @param cmd Metadata for the class
     */
    public ResultSetGetter(RDBMSStoreManager storeMgr, ExecutionContext ec, ResultSet rs,
            StatementClassMapping resultMappings, AbstractClassMetaData cmd)
    {
        this.storeMgr = storeMgr;
        this.op = null;
        this.cmd = cmd;
        this.ec = ec;
        this.resultSet = rs;
        this.resultMappings = resultMappings;
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getBoolean(ec, resultSet, mapIdx.getColumnPositions());
    }

    public char fetchCharField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getChar(ec, resultSet, mapIdx.getColumnPositions());
    }

    public byte fetchByteField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getByte(ec, resultSet, mapIdx.getColumnPositions());
    }

    public short fetchShortField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getShort(ec, resultSet, mapIdx.getColumnPositions());
    }

    public int fetchIntField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getInt(ec, resultSet, mapIdx.getColumnPositions());
    }

    public long fetchLongField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getLong(ec, resultSet, mapIdx.getColumnPositions());
    }

    public float fetchFloatField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getFloat(ec, resultSet, mapIdx.getColumnPositions());
    }

    public double fetchDoubleField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getDouble(ec, resultSet, mapIdx.getColumnPositions());
    }

    public String fetchStringField(int fieldNumber)
    {
        StatementMappingIndex mapIdx = resultMappings.getMappingForMemberPosition(fieldNumber);
        return mapIdx.getMapping().getString(ec, resultSet, mapIdx.getColumnPositions());
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
            value = mapping.getObject(ec, resultSet, mapIdx.getColumnPositions(), op, fieldNumber);
        }
        else
        {
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Process fields of sub-object if available in this result set
                StatementClassMapping relationMappings = resultMappings.getMappingDefinitionForMemberPosition(fieldNumber);
                if (relationMappings != null)
                {
                    ClassLoaderResolver clr = ec.getClassLoaderResolver();
                    AbstractClassMetaData relatedCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    ResultObjectFactory relationROF = storeMgr.newResultObjectFactory(relatedCmd, relationMappings, false, ec.getFetchPlan(), mmd.getType());
                    value = relationROF.getObject(ec, resultSet);
                }
                else
                {
                    value = mapping.getObject(ec, resultSet, mapIdx.getColumnPositions());
                }
            }
            else
            {
                value = mapping.getObject(ec, resultSet, mapIdx.getColumnPositions());
            }
        }

        // Return the field value (as a wrapper if wrappable)
        if (op != null)
        {
            if (op.getClassMetaData().getSCOMutableMemberFlags()[fieldNumber])
            {
                return SCOUtils.wrapSCOField(op, fieldNumber, value, false, false, false);
            }
            else if (RelationType.isRelationSingleValued(relationType) && (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getOwnerMember() != null))
            {
                // Embedded PC, so make sure the field is wrapped where appropriate TODO This should be part of ManagedRelationships
                op.updateOwnerFieldInEmbeddedField(fieldNumber, value);
                return value;
            }
        }
        return value;
    }
}