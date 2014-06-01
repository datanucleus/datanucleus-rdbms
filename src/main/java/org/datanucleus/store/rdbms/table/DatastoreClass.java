/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.table;

import java.util.Collection;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;

/**
 * Representation of a Java class in a datastore.
 */
public interface DatastoreClass extends Table
{
    /**
     * Accessor for the primary class represented.
     * @return Name of the class
     */
    String getType();

    /**
     * Accessor for the identity-type used by this table.
     * @return identity-type tag value
     */    
    public IdentityType getIdentityType();

    /**
     * Accessor for whether the object id will be attributed by the datastore
     * directly, or whether values have to be supplied.
     * @return Whether it is attributed in the datastore
     */
    boolean isObjectIdDatastoreAttributed();

    /**
     * Accessor for whether this datastore class is the base datastore class
     * for this inheritance hierarchy.
     * @return Whether it is the base.
     */
    boolean isBaseDatastoreClass();

    /**
     * Accessor for the base datastore class.
     * Returns this object if it has no superclass table, otherwise goes up to the superclass etc.
     * @return The base datastore class
     */
    DatastoreClass getBaseDatastoreClass();

    /**
     * Method to return the base DatastoreClass that persists the
     * specified field. This navigates up through the superclass
     * tables to find a table that manages the field.
     * @param fmd MetaData for the field required
     * @return The DatastoreClass managing that field
     */
    public DatastoreClass getBaseDatastoreClassWithMember(AbstractMemberMetaData fmd);

    /**
     * Accessor whether the supplied DatastoreClass is a supertable of this table.
     * @param table The DatastoreClass to check
     * @return Whether it is a supertable (somewhere up the inheritance tree)
     */
    public boolean isSuperDatastoreClass(DatastoreClass table);

    /**
     * Accessor for the supertable for this table.
     * This is only relevant if the DatastoreClass in use supports supertables.
     * If supertables arent supported by the datastore then null is returned.
     * @return The supertable (if any)
     */
    public DatastoreClass getSuperDatastoreClass();

    /**
     * Accessor for any secondary tables for this table.
     * @return Collection of secondary tables (if any)
     */
    public Collection<SecondaryDatastoreClass> getSecondaryDatastoreClasses();

    /**
     * Accessor for whether this table manages the specified class
     * @param className Name of the class
     * @return Whether it is managed by this table
     */
    public boolean managesClass(String className);

    /**
     * Accessor for the names of all classes managed by this table.
     * @return Names of the classes managed (stored) here
     */
    public String[] getManagedClasses();

    /**
     * Convenience method to return if this table manages the columns for the supplied mapping.
     * @param mapping The mapping
     * @return Whether the mapping is managed in this table
     */
    public boolean managesMapping(JavaTypeMapping mapping);

    /**
     * Accessor for the name of the datastore class (table).
     * @return The name
     */
    String toString();

    // --------------------------- Mapping Access ------------------------------

    /**
     * Accessor for a mapping for the datastore ID for this object.
     * @return The datastoreId mapping.
     */
    JavaTypeMapping getDatastoreIdMapping();

    /**
     * Accessor for the mapping for the specified member name.
     * Doesn't cope with fields of the same name in different subclasses - you
     * should call the equivalent method passing FieldMetaData for those.
     * @param memberName Name of field/property
     * @return The Mapping for the field/property
     */
    JavaTypeMapping getMemberMapping(String memberName);

    /**
     * Accessor for the mapping for the specified field.
     * @param mmd Metadata of the field/property
     * @return The Mapping for the field.
     */
    JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd);

    /**
     * Accessor for the mapping for the specified field only in this datastore class.
     * @param mmd Metadata of the field/property
     * @return The Mapping for the field (or null if not present here)
     */
    JavaTypeMapping getMemberMappingInDatastoreClass(AbstractMemberMetaData mmd);

    /**
     * Accessor for a mapping for the datastore ID (OID) for this table.
     * @param consumer Consumer for the mappings
     */
    void provideDatastoreIdMappings(MappingConsumer consumer);

    /**
     * Provide the mappings to the consumer for all primary-key fields mapped to
     * this table (for application identity).
     * @param consumer Consumer for the mappings
     */
    void providePrimaryKeyMappings(MappingConsumer consumer);

    /**
     * Provide the mappings to the consumer for all non primary-key fields mapped to this table.
     * @param consumer Consumer for the mappings
     */
    void provideNonPrimaryKeyMappings(MappingConsumer consumer);

    /**
     * Provide the mappings to the consumer for all specified members.
     * @param consumer Consumer for the mappings
     * @param mmds MetaData of the fields/properties to provide mappings for
     * @param includeSecondaryTables Whether to supply fields in secondary tables
     */
    void provideMappingsForMembers(MappingConsumer consumer, AbstractMemberMetaData[] mmds, boolean includeSecondaryTables);

    /**
     * Provide the mappings to version mappings
     * @param consumer Consumer for the version mappings
     */
    void provideVersionMappings(MappingConsumer consumer);

    /**
     * Provide the mappings to discriminator mappings
     * @param consumer Consumer for the mappings
     */
    void provideDiscriminatorMappings(MappingConsumer consumer);

    /**
     * Provide the mapping for multitenancy discriminator (if any).
     * @param consumer Consumer for the mapping
     */
    void provideMultitenancyMapping(MappingConsumer consumer);

    /**
     * Instruction to provide all columns without mappings.
     * @param consumer The consumer for the columns
     */
    void provideUnmappedColumns(MappingConsumer consumer);

    /**
     * Instruction to provide all external mappings to the passed consumer.
     * @param consumer The consumer for the mappings
     * @param mappingType Type of external mapping to provide
     */
    void provideExternalMappings(MappingConsumer consumer, int mappingType);

    /**
     * Accessor for the external mapping for the specified field of the specified type.
     * An external mapping is a mapping for which there is no field in the actual class
     * to represent it (part of a relation).
     * The type can be FK, FK discriminator, order, etc
     * @param mmd MetaData for the (external) field/property
     * @param mappingType The type of mapping
     * @return The external mapping
     */
    JavaTypeMapping getExternalMapping(AbstractMemberMetaData mmd, int mappingType);

    /**
     * Accessor for the owner field metadata for the specified external mapping of the
     * specified type
     * @param mapping The external mapping
     * @param mappingType The type of mapping
     * @return Field MetaData in the owner class
     */
    AbstractMemberMetaData getMetaDataForExternalMapping(JavaTypeMapping mapping, int mappingType);
}