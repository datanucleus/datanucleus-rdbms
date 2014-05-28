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
package org.datanucleus.store.rdbms.table;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.java.IntegerMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.DatastoreIdMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.StringMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.valuegenerator.AbstractGenerator;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Abstract representation of a table for a class.
 * Abstracts out the common parts of a primary ClassTable
 * and a SecondaryClassTable.
 * 
 * <H3>Mappings</H3>
 * A Table is built from a series of field mappings. Each Java class has a series of
 * fields and each of these has an associated JavaTypeMapping. Each JavaTypeMapping has
 * related DatastoreMapping(s). These are used in mapping the Java class to the table,
 * and are used when populating the table, and when retrieving data from the table back
 * to the object. There are several categories of mappings in this class
 * <UL>
 * <LI><B>memberMappingsMap</B> - the set of mappings relating to the fields in the
 * class. The mappings are keyed by the FieldMetaData of the field. Any embedded field
 * will have a single mapping here of type EmbeddedPCMapping, with a set of datastore
 * mappings attached.</LI>
 * <LI><B>datastoreIDMapping</B> - the Identity mapping when using "datastore identity"</LI>
 * <LI><B>pkMappings</B> - the mappings for the primary key column(s).</LI>
 * <LI><B>discriminatorMapping</B> - mapping for any discriminator column. This is only
 * used where classes share this table and some of them use "superclass-table" strategy</LI>
 * <LI><B>versionMapping</B> - mapping for any versioning column</LI>
 * </UL>
 */
public abstract class AbstractClassTable extends TableImpl
{
    /** Mappings for members mapped to this table, keyed by the metadata for the member. */
    protected Map<AbstractMemberMetaData, JavaTypeMapping> memberMappingsMap = new HashMap();

    /** Mapping for datastore identity (optional). */
    protected JavaTypeMapping datastoreIDMapping;

    /** Mappings for application identity (optional). */
    protected JavaTypeMapping[] pkMappings;

    /** Mapping for the id of the table. */
    protected JavaTypeMapping idMapping;

    /** Mapping for any version/timestamp column. */
    protected JavaTypeMapping versionMapping;

    /** MetaData for versioning of objects stored in this table. */
    protected VersionMetaData versionMetaData;

    /** MetaData for discriminator for objects stored in this table. */
    protected DiscriminatorMetaData discriminatorMetaData;

    /** Mapping for any discriminator column. */
    protected JavaTypeMapping discriminatorMapping;

    /** Highest absolute field/property number managed by this table */
    protected int highestMemberNumber = 0;

    /** Mapping for multi-tenancy. */
    protected JavaTypeMapping tenantMapping;

    /**
     * Constructor.
     * @param tableName Name of the table
     * @param storeMgr Store Manager that is managing this instance
     */
    public AbstractClassTable(DatastoreIdentifier tableName, RDBMSStoreManager storeMgr)
    {
        super(tableName, storeMgr);
    }

    /**
     * Convenience method to return the primary table.
     * @return The primary table for this table
     */
    public Table getPrimaryTable()
    {
        return this;
    }

    /**
     * Method to initialise the table primary key field(s).
     * @param clr The ClassLoaderResolver
     **/
    protected abstract void initializePK(ClassLoaderResolver clr);

    /**
     * Convenience method for whether the (fully-specified) member is managed by this table
     * @param memberName Fully qualified name of the field/property
     * @return Whether it is managed
     */
    public boolean managesMember(String memberName)
    {
        if (memberName == null)
        {
            return false;
        }

        return (getMappingForMemberName(memberName) != null);
    }

    /**
     * Accessor for the JavaTypeMapping that is handling the member of the specified name.
     * Returns the first one that matches.
     * @param memberName Name of the field/property
     * @return The java type mapping
     */
    protected JavaTypeMapping getMappingForMemberName(String memberName)
    {
        Set<AbstractMemberMetaData> fields = memberMappingsMap.keySet();
        Iterator<AbstractMemberMetaData> fieldsIter = fields.iterator();
        while (fieldsIter.hasNext())
        {
            AbstractMemberMetaData mmd = fieldsIter.next();
            if (mmd.getFullFieldName().equals(memberName))
            {
                return memberMappingsMap.get(mmd);
            }
        }
        return null;
    }

    /**
     * Convenience method to return if this table manages the columns for the supplied mapping.
     * @param mapping The mapping
     * @return Whether the mapping is managed in this table
     */
    public boolean managesMapping(JavaTypeMapping mapping)
    {
        Collection<JavaTypeMapping> mappings = memberMappingsMap.values();
        if (mappings.contains(mapping))
        {
            return true;
        }
        else if (mapping == discriminatorMapping)
        {
            return true;
        }
        else if (mapping == versionMapping)
        {
            return true;
        }
        else if (mapping == datastoreIDMapping)
        {
            return true;
        }
        else if (mapping == idMapping)
        {
            return true;
        }
        else if (mapping == tenantMapping)
        {
            return true;
        }
        return false;
    }

    /**
     * Utility to create the application identity columns and mapping.
     * Uses the id mapping of the specified class table and copies the mappings
     * and columns, whilst retaining the passed preferences for column namings.
     * This is used to copy the PK mappings of a superclass table so we have the same PK.
     * @param columnContainer The container of column MetaData with any namings
     * @param refTable The table that we use as reference
     * @param clr The ClassLoaderResolver
     * @param cmd The ClassMetaData
     */
    final void addApplicationIdUsingClassTableId(ColumnMetaDataContainer columnContainer, DatastoreClass refTable, ClassLoaderResolver clr, AbstractClassMetaData cmd)
    {
        ColumnMetaData[] userdefinedCols = null;
        int nextUserdefinedCol = 0;
        if (columnContainer != null)
        {
            userdefinedCols = columnContainer.getColumnMetaData();
        }

        pkMappings = new JavaTypeMapping[cmd.getPKMemberPositions().length];
        for (int i=0; i<cmd.getPKMemberPositions().length; i++)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[i]);
            JavaTypeMapping mapping = refTable.getMemberMapping(mmd);
            if (mapping == null)
            {
                //probably due to invalid metadata defined by the user
                throw new NucleusUserException("Cannot find mapping for field " + mmd.getFullFieldName()+
                    " in table " + refTable.toString() + " " +
                    StringUtils.objectArrayToString(refTable.getColumns()));
            }

            JavaTypeMapping masterMapping = storeMgr.getMappingManager().getMapping(clr.classForName(mapping.getType()));
            masterMapping.setMemberMetaData(mmd); // Update field info in mapping
            masterMapping.setTable(this);
            pkMappings[i] = masterMapping;

            // Loop through each id column in the reference table and add the same here
            // applying the required names from the columnContainer
            for (int j=0; j<mapping.getNumberOfDatastoreMappings(); j++)
            {
                JavaTypeMapping m = masterMapping;
                Column refColumn = mapping.getDatastoreMapping(j).getColumn();
                if (mapping instanceof PersistableMapping)
                {
                    m = storeMgr.getMappingManager().getMapping(clr.classForName(refColumn.getJavaTypeMapping().getType()));
                    ((PersistableMapping)masterMapping).addJavaTypeMapping(m);
                }

                ColumnMetaData userdefinedColumn = null;
                if (userdefinedCols != null)
                {
                    for (int k=0;k<userdefinedCols.length;k++)
                    {
                        if (refColumn.getIdentifier().toString().equals(userdefinedCols[k].getTarget()))
                        {
                            userdefinedColumn = userdefinedCols[k];
                            break;
                        }
                    }
                    if (userdefinedColumn == null && nextUserdefinedCol < userdefinedCols.length)
                    {
                        userdefinedColumn = userdefinedCols[nextUserdefinedCol++];
                    }
                }

                // Add this application identity column
                Column idColumn = null;
                if (userdefinedColumn != null)
                {
                    // User has provided a name for this column
                    // Currently we only use the column namings from the users definition but we could easily
                    // take more of their details.
                    idColumn = addColumn(refColumn.getStoredJavaType(),
                        storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, userdefinedColumn.getName()),
                        m, refColumn.getColumnMetaData());
                }
                else
                {
                    // No name provided so take same as superclass
                    idColumn = addColumn(refColumn.getStoredJavaType(), refColumn.getIdentifier(),
                        m, refColumn.getColumnMetaData());
                }
                if (mapping.getDatastoreMapping(j).getColumn().getColumnMetaData() != null)
                {
                    refColumn.copyConfigurationTo(idColumn);
                }
                idColumn.setAsPrimaryKey();

                // Set the column type based on the field.getType()
                getStoreManager().getMappingManager().createDatastoreMapping(m, idColumn, refColumn.getJavaTypeMapping().getType());
            }

            // Update highest field number if this is higher
            int absoluteFieldNumber = mmd.getAbsoluteFieldNumber();
            if (absoluteFieldNumber > highestMemberNumber)
            {
                highestMemberNumber = absoluteFieldNumber;
            }
        }
    }
    
    /**
     * Utility to create the datastore identity column and mapping.
     * This is used in 2 modes. The first is where we have a (primary) class table
     * and we aren't creating the OID mapping as a FK to another class. The second
     * is where we have a (secondary) class table and we are creating the OID mapping
     * as a FK to the primary class. In the second case the refTable will be specified.
     * @param columnMetaData The column MetaData for the datastore id
     * @param refTable Table used as a reference (if any)
     * @param cmd The MetaData for the class
     */
    void addDatastoreId(ColumnMetaData columnMetaData, DatastoreClass refTable, AbstractClassMetaData cmd)
    {
        // Create the mapping, setting its table
        datastoreIDMapping = new DatastoreIdMapping();
        datastoreIDMapping.setTable(this);
        datastoreIDMapping.initialize(storeMgr, cmd.getFullClassName());

        // Create a ColumnMetaData in the container if none is defined
        ColumnMetaData colmd = null;
        if (columnMetaData == null)
        {
            colmd = new ColumnMetaData();
        }
        else
        {
            colmd = columnMetaData;
        }
        if (colmd.getName() == null)
        {
            // Provide default column naming if none is defined
            if (refTable != null)
            {
                colmd.setName(storeMgr.getIdentifierFactory().newColumnIdentifier(refTable.getIdentifier().getIdentifierName(), 
                    this.storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(DatastoreId.class), FieldRole.ROLE_OWNER, false).getIdentifierName());
            }
            else
            {
                colmd.setName(storeMgr.getIdentifierFactory().newColumnIdentifier(identifier.getIdentifierName(), 
                    this.storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(DatastoreId.class), FieldRole.ROLE_NONE, false).getIdentifierName());
            }
        }

        // Add the datastore identity column as the PK
        Column idColumn = addColumn(DatastoreId.class.getName(), 
            storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colmd.getName()), datastoreIDMapping, colmd);
        idColumn.setAsPrimaryKey();

        // Set the identity column type based on the IdentityStrategy
        String strategyName = cmd.getIdentityMetaData().getValueStrategy().toString();
        if (cmd.getIdentityMetaData().getValueStrategy().equals(IdentityStrategy.CUSTOM))
        {
            strategyName = cmd.getIdentityMetaData().getValueStrategy().getCustomName();
        }

        // Check the value generator type being stored
        Class valueGeneratedType = Long.class;
        try
        {
            // Create generator so we can find the generated type
            // a). Try as unique generator first
            AbstractGenerator generator = (AbstractGenerator)
                storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                    "org.datanucleus.store_valuegenerator", 
                    new String[] {"name", "unique"}, new String[] {strategyName, "true"},
                    "class-name", new Class[] {String.class, Properties.class}, new Object[] {null, null});
            if (generator == null)
            {
                // b). Try as datastore-specific generator
                generator = (AbstractGenerator)
                    storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                    "org.datanucleus.store_valuegenerator",
                    new String[] {"name", "datastore"}, new String[] {strategyName, storeMgr.getStoreManagerKey()},
                    "class-name", new Class[] {String.class, Properties.class}, new Object[] {null, null});
            }
            if (generator != null)
            {
                valueGeneratedType = (Class) generator.getClass().getMethod("getStorageClass").invoke(null);
            }
        }
        catch (Exception e)
        {
            NucleusLogger.VALUEGENERATION.warn("Error retrieving storage class for value-generator " + 
                strategyName + " " + e.getMessage());
        }
        storeMgr.getMappingManager().createDatastoreMapping(datastoreIDMapping, idColumn, valueGeneratedType.getName());

        // Handle any auto-increment requirement
        if (isObjectIdDatastoreAttributed())
        {
            if (this instanceof DatastoreClass && ((DatastoreClass)this).isBaseDatastoreClass())
            {
                // Only the base class can be autoincremented
                idColumn.setIdentity(true);
            }
        }

        // Check if auto-increment and that it is supported by this RDBMS
        if (idColumn.isIdentity() && !dba.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS))
        {
            throw new NucleusException(Localiser.msg("057020", 
                cmd.getFullClassName(), "datastore-identity")).setFatal();
        }
    }

    /**
     * Method to add a multi-tenancy discriminator column.
     * @param colmd Metadata defining the column required
     */
    protected void addMultitenancyMapping(ColumnMetaData colmd)
    {
        String colName = "TENANT_ID";
        if (colmd != null && colmd.getName() != null)
        {
            colName = colmd.getName();
        }
        String typeName = String.class.getName();
        if (colmd != null && colmd.getJdbcType() != null)
        {
            if (colmd.getJdbcType() == JdbcType.INTEGER)
            {
                typeName = Integer.class.getName();
            }
        }

        if (typeName.equals(Integer.class.getName()))
        {
            tenantMapping = new IntegerMapping();
        }
        else
        {
            tenantMapping = new StringMapping();
        }
        tenantMapping.setTable(this);
        tenantMapping.initialize(storeMgr, typeName);
        Column tenantColumn = addColumn(typeName,
            storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colName), tenantMapping, colmd);
        storeMgr.getMappingManager().createDatastoreMapping(tenantMapping, tenantColumn, typeName);
    }

    /**
     * Utility to add the mapping for a field/property to the managed list.
     * @param fieldMapping The mapping for the field/property
     */
    protected void addMemberMapping(JavaTypeMapping fieldMapping)
    {
        AbstractMemberMetaData mmd = fieldMapping.getMemberMetaData();
        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            // Provide field->column mapping debug message
            StringBuilder columnsStr = new StringBuilder();
            for (int i=0;i<fieldMapping.getNumberOfDatastoreMappings();i++)
            {
                if (i > 0)
                {
                    columnsStr.append(",");
                }
                columnsStr.append(fieldMapping.getDatastoreMapping(i).getColumn());
            }
            if (fieldMapping.getNumberOfDatastoreMappings() == 0)
            {
                columnsStr.append("[none]");
            }
            StringBuilder datastoreMappingTypes = new StringBuilder();
            for (int i=0;i<fieldMapping.getNumberOfDatastoreMappings();i++)
            {
                if (i > 0)
                {
                    datastoreMappingTypes.append(',');
                }
                datastoreMappingTypes.append(fieldMapping.getDatastoreMapping(i).getClass().getName());
            }
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057010",
                mmd.getFullFieldName(), columnsStr.toString(), fieldMapping.getClass().getName(), datastoreMappingTypes.toString()));
        }

        memberMappingsMap.put(mmd, fieldMapping);

        // Update highest field number if this is higher
        int absoluteFieldNumber = mmd.getAbsoluteFieldNumber();
        if (absoluteFieldNumber > highestMemberNumber)
        {
            highestMemberNumber = absoluteFieldNumber;
        }
    }

    /**
     * Accessor for the identity-type.
     * @return identity-type tag value
     */    
    public abstract IdentityType getIdentityType();

    /**
     * Accessor for whether the table has its identity attributed
     * by the datastore (e.g using autoincrement)
     * @return Whether it is datastore attributed
     */
    public abstract boolean isObjectIdDatastoreAttributed();

    // -------------------------- Mapping Accessors --------------------------------

    /**
     * Accessor for a mapping for the datastore ID (OID) for this table.
     * @return The (OID) mapping.
     */
    public JavaTypeMapping getDatastoreObjectIdMapping()
    {
        assertIsInitialized();

        return datastoreIDMapping;
    }

    /**
     * Accessor for the version mapping specified .
     * @param allowSuperclasses Whether we should return just the mapping from this table
     *     or whether we should return it when this table has none and the supertable has
     * @return The version mapping.
     */
    public JavaTypeMapping getVersionMapping(boolean allowSuperclasses)
    {
        // We dont have superclasstables here so just return what we have
        return versionMapping;
    }

    /**
     * Accessor for the discriminator mapping specified.
     * @return The mapping for the discriminator column
     */
    public JavaTypeMapping getDiscriminatorMapping(boolean allowSuperclasses)
    {
        return discriminatorMapping;
    }

    public JavaTypeMapping getMultitenancyMapping()
    {
        return tenantMapping;
    }

    /**
     * Accessor for a mapping for the datastore ID (OID) for this table.
     * @param consumer Consumer for the mappings
     **/
    final public void provideDatastoreIdMappings(MappingConsumer consumer)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);

        if (getIdentityType() == IdentityType.DATASTORE)
        {
            consumer.consumeMapping(getDatastoreObjectIdMapping(), MappingConsumer.MAPPING_TYPE_DATASTORE_ID);
        }
    }

    /**
     * Provide the mappings to the consumer for all primary-key fields mapped to
     * this table (for application identity).
     * @param consumer Consumer for the mappings
     */
    public abstract void providePrimaryKeyMappings(MappingConsumer consumer);

    /**
     * Provide the mappings to the consumer for all non primary-key fields
     * mapped to this table.
     * @param consumer Consumer for the mappings
     */
    final public void provideNonPrimaryKeyMappings(MappingConsumer consumer)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);

        Set fieldNumbersSet = memberMappingsMap.keySet();
        Iterator iter = fieldNumbersSet.iterator();
        while (iter.hasNext())
        {
            AbstractMemberMetaData mmd = (AbstractMemberMetaData) iter.next();
            JavaTypeMapping memberMapping = memberMappingsMap.get(mmd);
            if (memberMapping != null)
            {
                if (!mmd.isPrimaryKey())
                {
                    consumer.consumeMapping(memberMapping, mmd);
                }
            }
        }
    }

    /**
     * Provide the mappings to the consumer for all specified members.
     * @param consumer Consumer for the mappings
     * @param mmds MetaData for the members to provide mappings for
     * @param includeSecondaryTables Whether to provide members in secondary tables
     */
    public void provideMappingsForMembers(MappingConsumer consumer, AbstractMemberMetaData[] mmds, boolean includeSecondaryTables)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);
        for (int i = 0; i < mmds.length; i++)
        {
            JavaTypeMapping fieldMapping = memberMappingsMap.get(mmds[i]);
            if (fieldMapping != null)
            {
                if (!mmds[i].isPrimaryKey())
                {
                    consumer.consumeMapping(fieldMapping, mmds[i]);
                }
            }
        }
    }

    /**
     * Provide the version mappings.
     * @param consumer Consumer for the version mappings
     */
    final public void provideVersionMappings(MappingConsumer consumer)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);
        if (getVersionMapping(false) != null)
        {
            consumer.consumeMapping(getVersionMapping(false), MappingConsumer.MAPPING_TYPE_VERSION);
        }
    }

    /**
     * Provide the discriminator mappings
     * @param consumer Consumer for the mappings
     */
    final public void provideDiscriminatorMappings(MappingConsumer consumer)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);
        if (getDiscriminatorMapping(false) != null)
        {
            consumer.consumeMapping(getDiscriminatorMapping(false), MappingConsumer.MAPPING_TYPE_DISCRIMINATOR);
        }
    }

    /**
     * Provide the multitenancy mapping.
     * @param consumer Consumer for the mapping
     */
    final public void provideMultitenancyMapping(MappingConsumer consumer)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);
        if (tenantMapping != null)
        {
            consumer.consumeMapping(tenantMapping, MappingConsumer.MAPPING_TYPE_MULTITENANCY);
        }
    }
}