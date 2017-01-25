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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ForeignKeyAction;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.JoinMetaData;
import org.datanucleus.metadata.PrimaryKeyMetaData;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a secondary table for a class.
 * Has a primary table that manages the main fields for that class and some fields are defined 
 * (using &lt;field table="..."&gt;) to be stored in the secondary table.
 * Many of the methods in this class defer to the equivalent method in the primary table.
 **/
public class SecondaryTable extends AbstractClassTable implements SecondaryDatastoreClass
{
    /** Primary table */
    private ClassTable primaryTable;

    /** MetaData for the join to the primary table. */
    private JoinMetaData joinMetaData;

    /**
     * Constructor. This is package level so that it SecondaryTables are only created by other tables.
     * @param tableName Table name SQL identifier
     * @param storeMgr Store Manager to manage this table
     * @param primaryTable The primary table for the class
     * @param jmd MetaData for the join to the primary table
     * @param clr The ClassLoaderResolver
     */
    SecondaryTable(DatastoreIdentifier tableName, RDBMSStoreManager storeMgr, ClassTable primaryTable, JoinMetaData jmd, ClassLoaderResolver clr)
    {
        super(tableName, storeMgr);
        if (primaryTable == null)
        {
            throw new NucleusUserException(Localiser.msg("057045", tableName.getName()));
        }
        this.primaryTable = primaryTable;
        this.joinMetaData = jmd;
        if (this.joinMetaData == null)
        {
            //if a join was not declared at field level,
            //look for joins declared at class level
            List<JoinMetaData> joins = this.primaryTable.getClassMetaData().getJoinMetaData();
            if (joins != null)
            {
                for (JoinMetaData joinmd : joins)
                {
                    if (tableName.getName().equals(joinmd.getTable()))
                    {
                        //found a join table with same name as this table name
                        this.joinMetaData = joinmd;
                        break;
                    }
                }
            }
        }
    }

    /**
     * Pre initilize. For things that must be initialized right after constructor 
     * @param clr the ClassLoaderResolver
     */
    public void preInitialize(final ClassLoaderResolver clr)
    {
        assertIsUninitialized();
        
        // Initialise the PK field(s)
        if (!isPKInitialized())
        {
            initializePK(clr);
        }
    }

    /**
     * Method to initialise the table.
     * This will initialise the primary key columns for the table. Any other columns
     * are added via the addFieldMapping() method.
     * @param clr The ClassLoaderResolver
     */
    public void initialize(ClassLoaderResolver clr)
    {
        assertIsUninitialized();

        // TODO Support <primary-key> specification of the columns to use in the PK.
        // This doesnt seem too important IMHO since the secondary table should *always* be
        // joined using the PK of the primary table (and one row in the secondary table matches
        // one row in the primary table).

        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057023", this));
        }
        state = TABLE_STATE_INITIALIZED;
    }

    /**
     * Post initilize. For things that must be set after all classes have been initialized before 
     * @param clr the ClassLoaderResolver
     */
    public void postInitialize(final ClassLoaderResolver clr)
    {
        assertIsInitialized();
    }

    /**
     * Method to initialise the primary key of this table.
     * @see org.datanucleus.store.rdbms.table.AbstractClassTable#initializePK(ClassLoaderResolver)
     */
    protected void initializePK(ClassLoaderResolver clr)
    {
        assertIsPKUninitialized();

        // Generate the primary key using the primary table as reference
        if (primaryTable.getIdentityType() == IdentityType.APPLICATION)
        {
            addApplicationIdUsingClassTableId(joinMetaData, primaryTable, clr, primaryTable.getClassMetaData());
        }
        else if (primaryTable.getIdentityType() == IdentityType.DATASTORE)
        {
            ColumnMetaData colmd = null;
            if (joinMetaData != null && joinMetaData.getColumnMetaData() != null && joinMetaData.getColumnMetaData().length > 0)
            {
                colmd = joinMetaData.getColumnMetaData()[0];
            }
            addDatastoreId(colmd, primaryTable, primaryTable.getClassMetaData());
        }

        state = TABLE_STATE_PK_INITIALIZED;
    }

    /**
     * Accessor for the primary key for this table. Overrides the method in TableImpl
     * to add on any specification of PK name in the metadata.
     * @return The primary key.
     */
    public PrimaryKey getPrimaryKey()
    {
        PrimaryKey pk = super.getPrimaryKey();
        if (joinMetaData == null)
        {
            // TODO Localise this message
            throw new NucleusUserException("A relationship to a secondary table requires a <join> specification. " + 
                "The secondary table is " + this.getDatastoreIdentifierFullyQualified() + 
                " and the primary table is " + this.getPrimaryTable() + 
                ". The fields mapped to this secondary table are: " + memberMappingsMap.keySet().toString());
        }
        PrimaryKeyMetaData pkmd = joinMetaData.getPrimaryKeyMetaData();
        if (pkmd != null && pkmd.getName() != null)
        {
            pk.setName(pkmd.getName());
        }

        return pk;
    }

    /**
     * Accessor for the primary datastore class that this is dependent on.
     * @return The associated primary datastore class.
     */
    public DatastoreClass getPrimaryDatastoreClass()
    {
        return primaryTable;
    }

    /**
     * Accessor for the JoinMetaData which is used to join to the primary DatastoreClass.
     * @return JoinMetaData
     */
    public JoinMetaData getJoinMetaData()
    {
        return joinMetaData;
    }

    /**
     * Accessor for the identity-type.
     * Simply returns the same as the primary table
     * @return identity-type tag value
     */    
    public IdentityType getIdentityType()
    {
        return primaryTable.getIdentityType();
    }

    /**
     * Accessor for the main type represented here.
     * @return Name of the principal class represented
     */
    public String getType()
    {
        return primaryTable.getType();
    }

    /**
     * Whether this table or super table has id (primary key) attributed by the datastore
     * @return true if the id attributed by the datastore
     */
    public boolean isObjectIdDatastoreAttributed()
    {
        // We never use auto increment since we only use the same value as the primary table entry
        return false;
    }

    /**
     * Whether this table is the base in the inheritance hierarchy.
     * @return true if this table is a root table
     */
    public boolean isBaseDatastoreClass()
    {
        return primaryTable.isBaseDatastoreClass();
    }

    public DatastoreClass getBaseDatastoreClass()
    {
        return primaryTable.getBaseDatastoreClass();
    }

    /**
     * Convenience accessor for the base table for this table which has the specified field.
     * @param mmd Field MetaData for this field
     * @return The base table which has the field specified
     */
    public DatastoreClass getBaseDatastoreClassWithMember(AbstractMemberMetaData mmd)
    {
        return primaryTable.getBaseDatastoreClassWithMember(mmd);
    }

    /**
     * Accessor for the supertable for this table.
     * @return The supertable
     **/
    public DatastoreClass getSuperDatastoreClass()
    {
        return null;
    }

    /**
     * Accessor whether the supplied DatastoreClass is a supertable of this table.
     * @param table The DatastoreClass to check
     * @return Whether it is a supertable (somewhere up the inheritance tree)
     */
    public boolean isSuperDatastoreClass(DatastoreClass table)
    {
        return false;
    }

    /**
     * Accessor for any secondary tables for this table.
     * @return Secondary tables (if any)
     */
    public Collection getSecondaryDatastoreClasses()
    {
        return null;
    }

    /**
     * Accessor for whether this table manages the specified class
     * @param className Name of the class
     * @return Whether it is managed by this table
     */
    public boolean managesClass(String className)
    {
        // We dont manage classes as such (the primary table does that) so just return false
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.DatastoreClass#getManagedClasses()
     */
    public String[] getManagedClasses()
    {
        // We don't manage classes as such so just return null
        return null;
    }

    /**
     * Accessor for the expected foreign keys for this table.
     * @return The expected foreign keys.
     **/
    protected List<ForeignKey> getExpectedForeignKeys()
    {
        assertIsInitialized();

        // Auto mode allows us to decide which FKs are needed as well as using what is in the users MetaData.
        boolean autoMode = false;
        if (storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE).equals("DataNucleus"))
        {
            autoMode = true;
        }

        // Add FK back to the primary table unless requested not to
        List<ForeignKey> foreignKeys = new ArrayList<>();
        ForeignKeyMetaData fkmd = joinMetaData != null ? joinMetaData.getForeignKeyMetaData() : null;
        if (autoMode || (fkmd != null && fkmd.getDeleteAction() != ForeignKeyAction.NONE))
        {
            ForeignKey fk = new ForeignKey(getIdMapping(), dba, primaryTable, fkmd != null && fkmd.isDeferred() ? true : false);
            if (fkmd != null && fkmd.getName() != null)
            {
                fk.setName(fkmd.getName());
            }
            foreignKeys.add(0, fk);
        }

        return foreignKeys;
    }

    /**
     * Accessor for the field/property Mapping.
     * Returns the mapping if it is present in this table.
     * @param mmd Field MetaData for this field/property
     * @return the Mapping for the field/property
     */
    public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd)
    {
        assertIsInitialized();

        // Check if we manage this field
        JavaTypeMapping m = memberMappingsMap.get(mmd);
        if (m != null)
        {
            return m;
        }

        return null;
    }

    /**
     * Accessor for the mapping for the specified field only in this datastore class.
     * @param mmd Metadata of the field/property
     * @return The Mapping for the field (or null if not present here)
     */
    public JavaTypeMapping getMemberMappingInDatastoreClass(AbstractMemberMetaData mmd)
    {
        return getMemberMapping(mmd);
    }

    /**
     * Accessor for the field mapping for the specified field.
     * TODO Use of this is discouraged since the fieldName is not fully qualified
     * and if a superclass-table inheritance is used we could have 2 fields of that name here.
     * @param fieldName Name of the field
     * @return The mapping for the field
     */
    public JavaTypeMapping getMemberMapping(String fieldName)
    {
        return getMemberMapping(primaryTable.getMetaDataForMember(fieldName));
    }

    /**
     * Accessor for the ID mapping for this table.
     * @return The ID mapping
     */
    public JavaTypeMapping getIdMapping()
    {
        if (idMapping != null)
        {
            return idMapping;
        }

        PersistableMapping mapping = new PersistableMapping();
        mapping.initialize(getStoreManager(), primaryTable.getClassMetaData().getFullClassName());
        if (getIdentityType() == IdentityType.DATASTORE)
        {
            mapping.addJavaTypeMapping(datastoreIdMapping);
        }
        else if (getIdentityType() == IdentityType.APPLICATION)
        {
            for (int i = 0; i < pkMappings.length; i++)
            {
                mapping.addJavaTypeMapping(pkMappings[i]);
            }
        }
        idMapping = mapping;
        return mapping;
    }

    /**
     * Provide the mappings to the consumer for all primary-key fields mapped to
     * this table (for application identity).
     * @param consumer Consumer for the mappings
     */
    public void providePrimaryKeyMappings(MappingConsumer consumer)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);

        ClassMetaData cmd = primaryTable.getClassMetaData();
        if (pkMappings != null)
        {
            // Application identity
            int[] primaryKeyFieldNumbers = cmd.getPKMemberPositions();
            for (int i=0;i<pkMappings.length;i++)
            {
                // Make the assumption that the pkMappings are in the same order as the absolute field numbers
                AbstractMemberMetaData fmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(primaryKeyFieldNumbers[i]);
                consumer.consumeMapping(pkMappings[i], fmd);
            }
        }
        else
        {
            // Datastore identity
            int[] primaryKeyFieldNumbers = cmd.getPKMemberPositions();
            int countPkFields = cmd.getNoOfPrimaryKeyMembers();
            for (int i = 0; i < countPkFields; i++)
            {
                AbstractMemberMetaData pkfmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(primaryKeyFieldNumbers[i]);
                consumer.consumeMapping(getMemberMapping(pkfmd), pkfmd);
            }
        }
    }

    public void provideExternalMappings(MappingConsumer consumer, int mappingType)
    {
    }

    /**
     * Method to provide all unmapped datastore fields (columns) to the consumer.
     * @param consumer Consumer of information
     */
    public void provideUnmappedColumns(MappingConsumer consumer)
    {
    }

    public JavaTypeMapping getExternalMapping(AbstractMemberMetaData fmd, int mappingType)
    {
        throw new NucleusException("N/A").setFatal();
    }

    public AbstractMemberMetaData getMetaDataForExternalMapping(JavaTypeMapping mapping, int mappingType)
    {
        throw new NucleusException("N/A").setFatal();
    }
}