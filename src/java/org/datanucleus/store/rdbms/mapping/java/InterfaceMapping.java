/**********************************************************************
Copyright (c) 2004 Barry Haddow and others. All rights reserved.
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
2004 Erik Bengtson - nearly all
2007 Andy Jefferson - bypass getObject for normal persistence cases
***********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.StringUtils;

/**
 * Mapping for Interface fields.
 */
public class InterfaceMapping extends ReferenceMapping
{
    /** 
     * Extension - if a field is declared with implementation-classes. 
     * Comma-separated list of implementation classes.
     */
    private String implementationClasses;

    /**
     * Initialisation.
     * @param mmd MetaData for the field/property
     * @param table datastore container (table)
     * @param clr ClassLoader resolver
     */
    public void initialize(AbstractMemberMetaData mmd, Table table, 
            ClassLoaderResolver clr)
    {
        super.initialize(mmd, table, clr);

        if (mmd.getType().isInterface() && mmd.getFieldTypes() != null && mmd.getFieldTypes().length == 1)
        {
            // Field is an interface but field-type is specified with one value only so try to impose that
            // This is of particular use where the JDO2 TCK has fields defined as interfaces and the field-type
            // as the persistent-interface type, forcing the persistence of the property.
            Class fieldTypeCls = clr.classForName(mmd.getFieldTypes()[0]);
            if (fieldTypeCls.isInterface())
            {
                type = mmd.getFieldTypes()[0];
            }
        }
    }

    /**
     * Set the implementation classes. If the field defined what the implementation
     * classes are, this mapping will only use it
     * @param implementationClasses the implementation classes string
     */
    public void setImplementationClasses(String implementationClasses)
    {
        this.implementationClasses = implementationClasses;
    }

    /**
     * Method to retrieve an object of this type from the ResultSet.
     * @param ec ExecutionContext
     * @param rs The ResultSet
     * @param pos The parameter positions
     * @return The object
     */
    public Object getObject(ExecutionContext ec, final ResultSet rs, int[] pos)
    {
        if (ec.getMetaDataManager().isPersistentInterface(type))
        {
            if (mappingStrategy == ID_MAPPING || mappingStrategy == XCALIA_MAPPING)
            {
                // TODO Cater for other mapping-strategy than FK per implementation.
                throw new NucleusUserException("DataNucleus does not support use of mapping-strategy=" + mappingStrategy +
                    " with a \"persistable interface\"");
            }

            // ********* This code is for the "persistent-interfaces" JDO feature **********
            // It does not work for normal interfaces and should be left well alone IMHO
            // The method MultiMapping.getObject() should be used for all normal persistence cases
            // and one day someone needs to comment on why this code exists and what it is trying to do that is
            // different from MultiMapping.getObject()
            String[] implTypes = null;
            if (this.implementationClasses != null)
            {
                // Use the implementations specified by the user for this field
                implTypes = StringUtils.split(implementationClasses, ",");
            }
            else
            {
                // Use the available implementation types
                implTypes = ec.getMetaDataManager().getClassesImplementingInterface(getType(), ec.getClassLoaderResolver());
            }

            // Go through the possible types for this field and find a non-null value (if there is one)
            int n = 0;
            for (int i=0; i<implTypes.length; i++)
            {
                JavaTypeMapping mapping;
                if (implTypes.length > javaTypeMappings.length)
                {
                    // all mappings stored to the same column(s), so same FK
                    PersistableMapping m = ((PersistableMapping) javaTypeMappings[0]);
                    mapping = storeMgr.getMappingManager().getMapping(ec.getClassLoaderResolver().classForName(implTypes[i]));
                    for (int j = 0; j < m.getDatastoreMappings().length; j++)
                    {
                        mapping.addDatastoreMapping(m.getDatastoreMappings()[j]);
                    }
                    for (int j = 0; j < m.getJavaTypeMapping().length; j++)
                    {
                        ((PersistableMapping) mapping).addJavaTypeMapping(m.getJavaTypeMapping()[j]);
                    }
                    ((PersistableMapping) mapping).setReferenceMapping(m.getReferenceMapping());
                }
                else
                {
                    mapping = javaTypeMappings[i];
                }
                int[] posMapping;
                if (n >= pos.length)
                {
                    // this means we store all implementations to the same columns,
                    // so we reset the index
                    n = 0;
                }
                if (mapping.getReferenceMapping() != null)
                {
                    posMapping = new int[mapping.getReferenceMapping().getNumberOfDatastoreMappings()];
                }
                else
                {
                    posMapping = new int[mapping.getNumberOfDatastoreMappings()];
                }
                for (int j = 0; j < posMapping.length; j++)
                {
                    posMapping[j] = pos[n++];
                }            
                Object value = null;
                try
                {
                    // Retrieve the value (PC object) for this mappings' object
                    value = mapping.getObject(ec, rs, posMapping);
                }
                catch (NullValueException e)
                {
                    // expected if implementation object is null and has primitive
                    // fields in the primary key
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    // expected, will try next implementation
                }
                if (value != null)
                {
                    if (IdentityUtils.isDatastoreIdentity(value))
                    {
                        // What situation is this catering for exactly ?
                        String className;
                        if (mapping.getReferenceMapping() != null)
                        { 
                            className = mapping.getReferenceMapping().getDatastoreMapping(0).getColumn().getStoredJavaType();
                        }
                        else
                        {
                            className = mapping.getDatastoreMapping(0).getColumn().getStoredJavaType();
                        }
                        value = OIDFactory.getInstance(ec.getNucleusContext(), className, IdentityUtils.getTargetKeyForDatastoreIdentity(value));
                        return ec.findObject(value, false, true, null);
                    }
                    else if (ec.getClassLoaderResolver().classForName(getType()).isAssignableFrom(value.getClass()))               	
                    {
                        return value;
                    }
                }
            }
            return null;
        }
        else
        {
            // Normal persistence goes via MultiMapping.getObject()
            return super.getObject(ec, rs, pos);
        }
    }
}