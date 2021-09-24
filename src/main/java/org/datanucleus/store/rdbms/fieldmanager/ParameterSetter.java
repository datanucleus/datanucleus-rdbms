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
2003 Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.fieldmanager;

import java.sql.PreparedStatement;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NotYetFlushedException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.InterfaceMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.Localiser;

/**
 * Parameter setter implementation of a field manager.
 */
public class ParameterSetter extends AbstractFieldManager
{
    protected final ObjectProvider sm;
    protected final ExecutionContext ec;
    protected final PreparedStatement statement;
    protected final StatementClassMapping stmtMappings;

    /**
     * Constructor.
     * @param sm StateManager for the object.
     * @param stmt The Statement to set values on.
     * @param stmtMappings mappings for parameters in the statement.
     */
    public ParameterSetter(ObjectProvider sm, PreparedStatement stmt, StatementClassMapping stmtMappings)
    {
        this.sm = sm;
        this.ec = sm.getExecutionContext();
        this.statement = stmt;
        this.stmtMappings = stmtMappings;
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setBoolean(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeCharField(int fieldNumber, char value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setChar(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setByte(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeShortField(int fieldNumber, short value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setShort(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeIntField(int fieldNumber, int value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setInt(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeLongField(int fieldNumber, long value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setLong(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setFloat(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setDouble(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeStringField(int fieldNumber, String value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        if (value == null && mapIdx.getMapping().getMemberMetaData().getNullValue() == NullValue.EXCEPTION)
        {
            throw new NucleusUserException(Localiser.msg("052400",
                mapIdx.getMapping().getMemberMetaData().getFullFieldName()));
        }
        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
        {
            // Set this value for all occurrences of this parameter
            mapIdx.getMapping().setString(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
        }
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        StatementMappingIndex mapIdx = stmtMappings.getMappingForMemberPosition(fieldNumber);
        if (value == null && mapIdx.getMapping().getMemberMetaData().getNullValue() == NullValue.EXCEPTION)
        {
            throw new NucleusUserException(Localiser.msg("052400", mapIdx.getMapping().getMemberMetaData().getFullFieldName()));
        }

        try
        {
            JavaTypeMapping mapping = mapIdx.getMapping();
            boolean provideOwner = false;
            if (mapping instanceof EmbeddedPCMapping ||
                mapping instanceof SerialisedPCMapping ||
                mapping instanceof SerialisedReferenceMapping ||
                mapping instanceof PersistableMapping ||
                mapping instanceof InterfaceMapping)
            {
                // Pass in the owner ObjectProvider/field for any mappings that have relations
                provideOwner = true;
            }

            if (mapIdx.getNumberOfParameterOccurrences() > 0)
            {
                for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
                {
                    // Set this value for all occurrences of this parameter
                    if (provideOwner)
                    {
                        mapping.setObject(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value, sm, fieldNumber);
                    }
                    else
                    {
                        mapping.setObject(ec, statement, mapIdx.getParameterPositionsForOccurrence(i), value);
                    }
                }
            }
            else
            {
                // Important : call setObject even if the paramIndices is null (reachability)
                if (provideOwner)
                {
                    mapping.setObject(ec, statement, null, value, sm, fieldNumber);
                }
                else
                {
                    mapping.setObject(ec, statement, null, value);
                }
            }

            AbstractMemberMetaData mmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            RelationType relationType = mmd.getRelationType(ec.getClassLoaderResolver());
            if (sm.getClassMetaData().getSCOMutableMemberFlags()[fieldNumber])
            {
                SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
            }
            else if (RelationType.isRelationSingleValued(relationType) && (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getOwnerMember() != null))
            {
                // Embedded PC, so make sure the field is wrapped where appropriate TODO This should be part of ManagedRelationships
                sm.updateOwnerFieldInEmbeddedField(fieldNumber, value);
            }
        }
        catch (NotYetFlushedException e)
        {
            if (sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getNullValue() == NullValue.EXCEPTION)
            {
                throw e;
            }
            sm.updateFieldAfterInsert(e.getPersistable(),fieldNumber);
        }
    }
}