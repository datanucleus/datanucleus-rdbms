/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved. 
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
2003 Andy Jefferson - added localiser
2003 Andy Jefferson - replaced TableMetadata with identifier.
2003 Erik Bengtson - refactored the datastore identity together with the SQLIdentifier class
2003 Erik Bengtson - added OptimisticMapping
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - merged with JDOBaseTable
2004 Andy Jefferson - changed to store map of fieldMappings keyed by absolute field num. Added start point for inheritance strategy handling
2004 Andy Jefferson - changed consumer to set highest field number based on actual highest (to allow for inheritance strategies)
2004 Andy Jefferson - added DiscriminatorMapping
2004 Andy Jefferson - enabled use of Long/String datastore identity column
2004 Andy Jefferson - added capability to handle 1-N inverse unidirectional FKs
2004 Andy Jefferson - removed the majority of the value-strategy code - done elsewhere
    ...
 **********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.ElementMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.ForeignKeyAction;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.ValueGenerationStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.InterfaceMetaData;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.JoinMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MultitenancyMetaData;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.PrimaryKeyMetaData;
import org.datanucleus.metadata.PropertyMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.SoftDeleteMetaData;
import org.datanucleus.metadata.UniqueMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.exceptions.ClassDefinitionException;
import org.datanucleus.store.rdbms.exceptions.DuplicateColumnException;
import org.datanucleus.store.rdbms.exceptions.NoSuchPersistentFieldException;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.mapping.CorrespondentColumnsMapper;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.BooleanMapping;
import org.datanucleus.store.rdbms.mapping.java.DiscriminatorMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.OrderIndexMapping;
import org.datanucleus.store.rdbms.mapping.java.IntegerMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.LongMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedMapping;
import org.datanucleus.store.rdbms.mapping.java.SqlTimestampMapping;
import org.datanucleus.store.rdbms.mapping.java.StringMapping;
import org.datanucleus.store.rdbms.mapping.java.VersionMapping;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.MacroString;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Table representing a Java class (or classes) as a first class object (FCO).
 * Uses the inheritance strategy to control whether this represents multiple classes or just the one class.
 * <h3>Mappings</h3>
 * This class adds some additional mappings over what the superclass provides. Here we add
 * <ul>
 * <li><b>externalFkMappings</b> - any mappings for Collections that have no associated field in this class, providing the foreign key column(s)</li>
 * <li><b>externalOrderMappings</b> - mappings for any ordering column used by Lists for ordering elements of this class</li>
 * <li><b>externalFkDiscriminatorMappings</b> - mappings for any discriminator column used when sharing external foreign keys to distinguish the element owner field</li>
 * </ul>
 * <h3>Classes</h3>
 * A table can represent multiple classes. It has a nominal owner which is the class that has an inheritance strategy of "new-table". 
 * All classes that utilise this table have their MetaData stored in this object.
 * <h3>Secondary Tables</h3>
 * This class represents a "primary" table. That is, the main table where objects of a class are persisted. 
 * It can have several "secondary" tables where some of the classes fields are stored at persistence.
 */
public class ClassTable extends AbstractClassTable implements DatastoreClass 
{
    /**
     * MetaData for the principal class being stored here.
     * In inheritance situations where multiple classes share the same table, this will be the class which uses "new-table" strategy.
     */
    private final ClassMetaData cmd;

    /** MetaData for all classes being managed here. */
    private final Collection<AbstractClassMetaData> managedClassMetaData = new HashSet();

    /** Callbacks that have been applied keyed by the managed class. */
    private final Map<String, Collection<AbstractMemberMetaData>> callbacksAppliedForManagedClass = new HashMap<>();

    /** Table above this table storing superclass information (if any). */
    private ClassTable supertable;

    /** Secondary tables for this table (if any). */
    private Map<String, SecondaryTable> secondaryTables;

    /**
     * Mappings for FK Collections/Lists not managed by this class (1-N unidirectional).
     * Keyed by the metadata of the owner field/property.
     */
    private Map<AbstractMemberMetaData, JavaTypeMapping> externalFkMappings;

    /**
     * Mappings for FK Collections/Lists relation discriminators not managed by this class (1-N unidirectional).
     * Keyed by the metadata of the owner field/property.
     */
    private Map<AbstractMemberMetaData, JavaTypeMapping> externalFkDiscriminatorMappings;

    /**
     * Mappings for FK Lists order columns not managed by this class (1-N unidirectional).
     * Keyed by the metadata of the owner field/property.
     */
    private Map<AbstractMemberMetaData, JavaTypeMapping> externalOrderMappings;

    /** User defined table schema **/
    private MacroString tableDef;

    /** DDL statement for creating the table, if using user defined table schema. */
    private String createStatementDDL;

    Map<AbstractMemberMetaData, CandidateKey> candidateKeysByMapField = new HashMap();

    /** Set of unmapped "Column" objects that have no associated field (and hence ColumnMapping). */
    Set<Column> unmappedColumns = null;

    /**
     * Constructor.
     * @param tableName Table name SQL identifier
     * @param storeMgr Store Manager to manage this table
     * @param cmd MetaData for the class.
     */
    public ClassTable(DatastoreIdentifier tableName, RDBMSStoreManager storeMgr, ClassMetaData cmd)
    {
        super(tableName, storeMgr);
        this.cmd = cmd;

        // Check if this is a valid class to map to its own table
        if (cmd.getInheritanceMetaData().getStrategy() != InheritanceStrategy.NEW_TABLE &&
            cmd.getInheritanceMetaData().getStrategy() != InheritanceStrategy.COMPLETE_TABLE)
        {
            throw new NucleusUserException(Localiser.msg("057003", cmd.getFullClassName(), cmd.getInheritanceMetaData().getStrategy().toString())).setFatal();
        }

        highestMemberNumber = cmd.getNoOfManagedMembers() + cmd.getNoOfInheritedManagedMembers();
        
        // Extract the table definition from MetaData, if exists
        String tableImpStr = cmd.getValueForExtension("ddl-imports");
        String tableDefStr = null;
        if (dba.getVendorID() != null)
        {
            tableDefStr = cmd.getValueForExtension("ddl-definition" + '-' + dba.getVendorID());
        }
        if (tableDefStr == null)
        {
            tableDefStr = cmd.getValueForExtension("ddl-definition");
        }
        if (tableDefStr != null)
        {
            tableDef = new MacroString(cmd.getFullClassName(), tableImpStr, tableDefStr);
        }
    }

    /**
     * Pre-initialize.
     * We require any supertable, and the PK to be ready before we start initialisation.
     * @param clr the ClassLoaderResolver
     */
    public void preInitialize(final ClassLoaderResolver clr)
    {
        assertIsPKUninitialized();

        if (cmd.getInheritanceMetaData().getStrategy() != InheritanceStrategy.COMPLETE_TABLE)
        {
            // Inheritance strategy may imply having a supertable, so identify it
            supertable = getSupertable(cmd, clr);

            if (supertable != null && !supertable.isInitialized() && !supertable.isPKInitialized())
            {
                // Make sure that the supertable is preinitialised before we think about initialising here
                supertable.preInitialize(clr);
            }
        }

        // Initialise the PK field(s)
        if (!isPKInitialized())
        {
            initializePK(clr);
        }
    }

    /**
     * Method to initialise the table. 
     * This adds the columns based on the MetaData representation for the class being represented by this table.
     * @param clr The ClassLoaderResolver
     */
    public void initialize(ClassLoaderResolver clr)
    {
        // if already initialized, we have nothing further to do here
        if (isInitialized()) 
        {
            return;
        }

        // initialize any supertable first - this will ensure that any resources
        // we may inherit from that table are initialized at the point at which we may need them
        if (supertable != null) 
        {
            supertable.initialize(clr);
        }

        // Add the fields for this class (and any other superclasses that we need to manage the
        // fields for (inheritance-strategy="subclass-table" in the superclass)
        initializeForClass(cmd, clr);

        MappingManager mapMgr = storeMgr.getMappingManager();
        // Add Version where specified in MetaData
        // TODO If there is a superclass table that has a version we should omit from here even if in MetaData
        // See "getTableWithDiscriminator()" for the logic
        versionMetaData = cmd.getVersionMetaDataForTable();
        if (versionMetaData != null && versionMetaData.getFieldName() == null)
        {
            if (versionMetaData.getVersionStrategy() == VersionStrategy.NONE || versionMetaData.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                // No optimistic locking but the idiot wants a column for that :-)
                versionMapping = new VersionMapping.VersionLongMapping(this, mapMgr.getMapping(Long.class));
            }
            else if (versionMetaData.getVersionStrategy() == VersionStrategy.DATE_TIME)
            {
                if (!dba.supportsOption(DatastoreAdapter.DATETIME_STORES_MILLISECS))
                {
                    // TODO Localise this
                    throw new NucleusException("Class " + cmd.getFullClassName() + " is defined " +
                        "to use date-time versioning, yet this datastore doesnt support storing " +
                        "milliseconds in DATETIME/TIMESTAMP columns. Use version-number");
                }
                versionMapping = new VersionMapping.VersionTimestampMapping(this, mapMgr.getMapping(Timestamp.class));
            }
            if (versionMapping != null)
            {
                logMapping("VERSION", versionMapping);
            }
        }

        DiscriminatorMetaData dismd = cmd.getDiscriminatorMetaDataForTable();
        if (dismd != null)
        {
            // Surrogate discriminator
            discriminatorMetaData = dismd;
            if (storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_DISCRIM_PER_SUBCLASS_TABLE))
            {
                // Backwards compatibility only. Creates discriminator in all subclass tables even though not needed
                // TODO Remove this in the future
                discriminatorMapping = DiscriminatorMapping.createDiscriminatorMapping(this, dismd);
            }
            else
            {
                // Create discriminator column only in top most table that needs it
                ClassTable tableWithDiscrim = getTableWithDiscriminator();
                if (tableWithDiscrim == this)
                {
                    // No superclass with a discriminator so add it in this table
                    discriminatorMapping = DiscriminatorMapping.createDiscriminatorMapping(this, dismd);
                }
            }
            if (discriminatorMapping != null)
            {
                logMapping("DISCRIMINATOR", discriminatorMapping);
            }
        }

        // TODO Only put on root table (i.e "if (supertable != null)" then omit)
        MultitenancyMetaData mtmd = cmd.getMultitenancyMetaData();
        if (mtmd != null)
        {
            // Surrogate multi-tenancy discriminator
            ColumnMetaData colmd = mtmd.getColumnMetaData();
            if (colmd == null)
            {
                colmd = new ColumnMetaData();
                if (mtmd.getColumnName() != null)
                {
                    colmd.setName(mtmd.getColumnName());
                }
            }

            String colName = (colmd.getName() != null) ? colmd.getName() : "TENANT_ID";
            String typeName = (colmd.getJdbcType() == JdbcType.INTEGER) ? Integer.class.getName() : String.class.getName();

            multitenancyMapping = (typeName.equals(Integer.class.getName())) ? new IntegerMapping() : new StringMapping();
            multitenancyMapping.setTable(this);
            multitenancyMapping.initialize(storeMgr, typeName);
            Column tenantColumn = addColumn(typeName, storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colName), multitenancyMapping, colmd);
            storeMgr.getMappingManager().createColumnMapping(multitenancyMapping, tenantColumn, typeName);
            logMapping("MULTITENANCY", multitenancyMapping);
        }

        SoftDeleteMetaData sdmd = cmd.getSoftDeleteMetaData();
        if (sdmd != null)
        {
            // Surrogate "SoftDelete" flag column
            ColumnMetaData colmd = sdmd.getColumnMetaData();
            if (colmd == null)
            {
                colmd = new ColumnMetaData();
                if (sdmd.getColumnName() != null)
                {
                    colmd.setName(sdmd.getColumnName());
                }
            }

            String colName = (colmd.getName() != null) ? colmd.getName() : "DELETED";
            String typeName = Boolean.class.getName(); // TODO Allow integer using JDBC type

            softDeleteMapping = new BooleanMapping();
            softDeleteMapping.setTable(this);
            softDeleteMapping.initialize(storeMgr, typeName);
            Column tenantColumn = addColumn(typeName, storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colName), softDeleteMapping, colmd);
            storeMgr.getMappingManager().createColumnMapping(softDeleteMapping, tenantColumn, typeName);
            logMapping("SOFTDELETE", softDeleteMapping);
        }

        if (cmd.hasExtension(MetaData.EXTENSION_CLASS_CREATEUSER))
        {
            // Surrogate "create user" column
            ColumnMetaData colmd = new ColumnMetaData();
            if (cmd.hasExtension(MetaData.EXTENSION_CLASS_CREATEUSER_COLUMN_NAME))
            {
                colmd.setName(cmd.getValueForExtension(MetaData.EXTENSION_CLASS_CREATEUSER_COLUMN_NAME));
            }
            if (cmd.hasExtension(MetaData.EXTENSION_CLASS_CREATEUSER_COLUMN_LENGTH))
            {
                colmd.setLength(cmd.getValueForExtension(MetaData.EXTENSION_CLASS_CREATEUSER_COLUMN_LENGTH));
            }

            String colName = (colmd.getName() != null) ? colmd.getName() : "CREATE_USER";
            String typeName = String.class.getName();

            createUserMapping = new StringMapping();
            createUserMapping.setTable(this);
            createUserMapping.initialize(storeMgr, typeName);
            Column auditColumn = addColumn(typeName, storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colName), createUserMapping, colmd);
            storeMgr.getMappingManager().createColumnMapping(createUserMapping, auditColumn, typeName);
            logMapping("CREATEUSER", createUserMapping);
        }
        if (cmd.hasExtension(MetaData.EXTENSION_CLASS_CREATETIMESTAMP))
        {
            // Surrogate "create timestamp" column
            ColumnMetaData colmd = new ColumnMetaData();
            if (cmd.hasExtension(MetaData.EXTENSION_CLASS_CREATETIMESTAMP_COLUMN_NAME))
            {
                colmd.setName(cmd.getValueForExtension(MetaData.EXTENSION_CLASS_CREATETIMESTAMP_COLUMN_NAME));
            }

            String colName = (colmd.getName() != null) ? colmd.getName() : "CREATE_TIMESTAMP";
            String typeName = Timestamp.class.getName();

            createTimestampMapping = new SqlTimestampMapping();
            createTimestampMapping.setTable(this);
            createTimestampMapping.initialize(storeMgr, typeName);
            Column auditColumn = addColumn(typeName, storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colName), createTimestampMapping, colmd);
            storeMgr.getMappingManager().createColumnMapping(createTimestampMapping, auditColumn, typeName);
            logMapping("CREATETIMESTAMP", createTimestampMapping);
        }

        if (cmd.hasExtension(MetaData.EXTENSION_CLASS_UPDATEUSER))
        {
            // Surrogate "update user" column
            ColumnMetaData colmd = new ColumnMetaData();
            if (cmd.hasExtension(MetaData.EXTENSION_CLASS_UPDATEUSER_COLUMN_NAME))
            {
                colmd.setName(cmd.getValueForExtension(MetaData.EXTENSION_CLASS_UPDATEUSER_COLUMN_NAME));
            }
            if (cmd.hasExtension(MetaData.EXTENSION_CLASS_UPDATEUSER_COLUMN_LENGTH))
            {
                colmd.setLength(cmd.getValueForExtension(MetaData.EXTENSION_CLASS_UPDATEUSER_COLUMN_LENGTH));
            }

            String colName = (colmd.getName() != null) ? colmd.getName() : "UPDATE_USER";
            String typeName = String.class.getName();

            updateUserMapping = new StringMapping();
            updateUserMapping.setTable(this);
            updateUserMapping.initialize(storeMgr, typeName);
            Column auditColumn = addColumn(typeName, storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colName), updateUserMapping, colmd);
            storeMgr.getMappingManager().createColumnMapping(updateUserMapping, auditColumn, typeName);
            logMapping("UPDATEUSER", updateUserMapping);
        }
        if (cmd.hasExtension(MetaData.EXTENSION_CLASS_UPDATETIMESTAMP))
        {
            // Surrogate "update timestamp" column
            ColumnMetaData colmd = new ColumnMetaData();
            if (cmd.hasExtension(MetaData.EXTENSION_CLASS_UPDATETIMESTAMP_COLUMN_NAME))
            {
                colmd.setName(cmd.getValueForExtension(MetaData.EXTENSION_CLASS_UPDATETIMESTAMP_COLUMN_NAME));
            }

            String colName = (colmd.getName() != null) ? colmd.getName() : "UPDATE_TIMESTAMP";
            String typeName = Timestamp.class.getName();

            updateTimestampMapping = new SqlTimestampMapping();
            updateTimestampMapping.setTable(this);
            updateTimestampMapping.initialize(storeMgr, typeName);
            Column auditColumn = addColumn(typeName, storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colName), updateTimestampMapping, colmd);
            storeMgr.getMappingManager().createColumnMapping(updateTimestampMapping, auditColumn, typeName);
            logMapping("UPDATETIMESTAMP", updateTimestampMapping);
        }

        // Initialise any SecondaryTables
        if (secondaryTables != null)
        {
            Iterator<Map.Entry<String, SecondaryTable>> secondaryTableEntryIter = secondaryTables.entrySet().iterator();
            while (secondaryTableEntryIter.hasNext())
            {
                Map.Entry<String, SecondaryTable> secondaryTableEntry = secondaryTableEntryIter.next();
                SecondaryTable second = secondaryTableEntry.getValue();
                if (!second.isInitialized())
                {
                    second.initialize(clr);
                }
            }
        }

        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057023", this));
        }
        storeMgr.registerTableInitialized(this);
        state = TABLE_STATE_INITIALIZED;
    }
    
    /**
     * Post initilize. For things that must be set after all classes have been initialized before 
     * @param clr the ClassLoaderResolver
     */
    public void postInitialize(final ClassLoaderResolver clr)
    {
        assertIsInitialized();
        runCallBacks(clr);

        if (tableDef != null)
        {
            createStatementDDL = tableDef.substituteMacros(new MacroString.MacroHandler()
                {
                    public void onIdentifierMacro(MacroString.IdentifierMacro im)
                    {
                        storeMgr.resolveIdentifierMacro(im, clr);
                    }
    
                    public void onParameterMacro(MacroString.ParameterMacro pm)
                    {
                        throw new NucleusUserException(Localiser.msg("057033", cmd.getFullClassName(), pm));
                    }
                }, clr
            );
        }
    }

    /** Name of class currently being processed in manageClass (if any). */
    protected transient String managingClassCurrent = null;

    /** Flag to run the callbacks after the current class is managed fully. */
    protected boolean runCallbacksAfterManageClass = false;

    /**
     * Method that adds the specified class to be managed by this table.
     * Will provide mapping of all persistent fields to their underlying columns, map all necessary
     * identity fields, and manage all "unmapped" columns that have no associated field.
     * where the columns are defined for each mapping.
     * @param theCmd ClassMetaData for the class to be managed
     * @param clr The ClassLoaderResolver
     */
    public void manageClass(AbstractClassMetaData theCmd, ClassLoaderResolver clr)
    {
        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057024", toString(), theCmd.getFullClassName(), theCmd.getInheritanceMetaData().getStrategy().toString()));
        }

        managingClassCurrent = theCmd.getFullClassName();
        managedClassMetaData.add(theCmd);

        // Manage all fields of this class and all fields of superclasses that this is overriding
        manageMembers(theCmd, clr, theCmd.getManagedMembers());
        manageMembers(theCmd, clr, theCmd.getOverriddenMembers());

        // Manage all "unmapped" columns (that have no field)
        manageUnmappedColumns(theCmd, clr);

        managingClassCurrent = null;
        if (runCallbacksAfterManageClass)
        {
            // We need to run the callbacks now that this class is fully managed
            runCallBacks(clr);
            runCallbacksAfterManageClass = false;
        }
    }

    /**
     * Accessor for the names of all classes managed by this table.
     * @return Names of the classes managed (stored) here
     */
    public String[] getManagedClasses()
    {
        String[] classNames = new String[managedClassMetaData.size()];
        Iterator<AbstractClassMetaData> iter = managedClassMetaData.iterator();
        int i = 0;
        while (iter.hasNext())
        {
            classNames[i++] = (iter.next()).getFullClassName();
        }
        return classNames;
    }

    /**
     * Goes through all specified members for the specified class and adds a mapping for each.
     * Ignores primary-key fields which are added elsewhere.
     * @param theCmd ClassMetaData for the class to be managed
     * @param clr The ClassLoaderResolver
     * @param mmds the fields/properties to manage
     */
    private void manageMembers(AbstractClassMetaData theCmd, ClassLoaderResolver clr, AbstractMemberMetaData[] mmds)
    {
        // Go through the fields for this class and add columns for them
        for (int fieldNumber=0; fieldNumber<mmds.length; fieldNumber++)
        {
            // Primary key fields are added by the initialisePK method
            AbstractMemberMetaData mmd = mmds[fieldNumber];
            if (!mmd.isPrimaryKey())
            {
                if (managesMember(mmd.getFullFieldName()))
                {
                    if (!mmd.getClassName(true).equals(theCmd.getFullClassName()))
                    {
                        // Field already managed by this table so maybe we are overriding a superclass
                        JavaTypeMapping fieldMapping = getMappingForMemberName(mmd.getFullFieldName());
                        ColumnMetaData[] colmds = mmd.getColumnMetaData();
                        if (colmds != null && colmds.length > 0)
                        {
                            // Apply this set of ColumnMetaData to the existing mapping
                            int colnum = 0;
                            IdentifierFactory idFactory = getStoreManager().getIdentifierFactory();
                            for (int i=0;i<fieldMapping.getNumberOfColumnMappings();i++)
                            {
                                Column col = fieldMapping.getColumnMapping(i).getColumn();
                                col.setIdentifier(idFactory.newColumnIdentifier(colmds[colnum].getName()));
                                col.setColumnMetaData(colmds[colnum]);

                                colnum++;
                                if (colnum == colmds.length)
                                {
                                    // Reached end of specified metadata
                                    break;
                                }
                            }

                            // TODO Change this to reflect that we have updated the previous mapping
                            logMapping(mmd.getFullFieldName(), fieldMapping);
                        }
                    }
                }
                else
                {
                    // Manage the field if not already managed (may already exist if overriding a superclass field)
                    if (mmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                    {
                        boolean isPrimary = true;
                        if (mmd.getTable() != null && mmd.getJoinMetaData() == null)
                        {
                            // Field has a table specified and is not a 1-N with join table
                            // so is mapped to a secondary table
                            isPrimary = false;
                        }

                        if (isPrimary)
                        {
                            // Add the field to this table
                            JavaTypeMapping fieldMapping = storeMgr.getMappingManager().getMapping(this, mmd, clr, FieldRole.ROLE_FIELD);
                            if (theCmd != cmd && 
                                theCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUPERCLASS_TABLE &&
                                fieldMapping.getNumberOfColumnMappings() > 0)
                            {
                                // Field is for a subclass and so column(s) has to either allow nulls, or have default
                                int numCols = fieldMapping.getNumberOfColumnMappings();
                                for (int colNum = 0;colNum < numCols; colNum++)
                                {
                                    Column col = fieldMapping.getColumnMapping(colNum).getColumn();
                                    if (col.getDefaultValue() == null && !col.isNullable())
                                    {
                                        // Column needs to be nullable
                                        NucleusLogger.DATASTORE_SCHEMA.debug("Member " + mmd.getFullFieldName() +
                                            " uses superclass-table yet the field is not marked as nullable " +
                                            " nor does it have a default value, so setting the column as nullable");
                                        col.setNullable(true);
                                    }
                                }
                            }

                            addMemberMapping(fieldMapping);
                        }
                        else
                        {
                            // Add the field to the appropriate secondary table
                            if (secondaryTables == null)
                            {
                                secondaryTables = new HashMap();
                            }
                            SecondaryTable secTable = secondaryTables.get(mmd.getTable());
                            if (secTable == null)
                            {
                                // Secondary table doesnt exist yet so create it to users specifications.
                                List<JoinMetaData> joinmds = theCmd.getJoinMetaData();
                                JoinMetaData theJoinMD = null;
                                if (joinmds != null)
                                {
                                    for (JoinMetaData joinmd : joinmds)
                                    {
                                        if (joinmd.getTable().equalsIgnoreCase(mmd.getTable()) &&
                                            (joinmd.getCatalog() == null || (joinmd.getCatalog() != null && joinmd.getCatalog().equalsIgnoreCase(mmd.getCatalog()))) &&
                                            (joinmd.getSchema() == null || (joinmd.getSchema() != null && joinmd.getSchema().equalsIgnoreCase(mmd.getSchema()))))
                                        {
                                            theJoinMD = joinmd;
                                            break;
                                        }
                                    }
                                }

                                DatastoreIdentifier secTableIdentifier = 
                                    storeMgr.getIdentifierFactory().newTableIdentifier(mmd.getTable());
                                // Use specified catalog, else take catalog of the owning table
                                String catalogName = mmd.getCatalog();
                                if (catalogName == null)
                                {
                                    catalogName = getCatalogName();
                                }
                                // Use specified schema, else take schema of the owning table
                                String schemaName = mmd.getSchema();
                                if (schemaName == null)
                                {
                                    schemaName = getSchemaName();
                                }
                                secTableIdentifier.setCatalogName(catalogName);
                                secTableIdentifier.setSchemaName(schemaName);

                                secTable = new SecondaryTable(secTableIdentifier, storeMgr, this, theJoinMD, clr);
                                secTable.preInitialize(clr);
                                secTable.initialize(clr);
                                secTable.postInitialize(clr);
                                secondaryTables.put(mmd.getTable(), secTable);
                            }
                            secTable.addMemberMapping(storeMgr.getMappingManager().getMapping(secTable, mmd, 
                                clr, FieldRole.ROLE_FIELD));
                        }
                    }
                    else if (mmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL)
                    {
                        throw new NucleusException(Localiser.msg("057006",mmd.getName())).setFatal();
                    }

                    // Calculate if we need a FK adding due to a 1-N (FK) relationship
                    boolean needsFKToContainerOwner = false;
                    RelationType relationType = mmd.getRelationType(clr);
                    if (relationType == RelationType.ONE_TO_MANY_BI)
                    {
                        AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                        if (mmd.getJoinMetaData() == null && relatedMmds[0].getJoinMetaData() == null)
                        {
                            needsFKToContainerOwner = true;
                        }
                    }
                    else if (relationType == RelationType.ONE_TO_MANY_UNI && !mmd.isSingleCollection() )
                    {
                        if (mmd.getJoinMetaData() == null)
                        {
                            needsFKToContainerOwner = true;
                        }
                    }

                    if (needsFKToContainerOwner)
                    {
                        // 1-N uni/bidirectional using FK, so update the element side with a FK
                        if ((mmd.getCollection() != null && !SCOUtils.collectionHasSerialisedElements(mmd)) ||
                            (mmd.getArray() != null && !SCOUtils.arrayIsStoredInSingleColumn(mmd, storeMgr.getMetaDataManager())))
                        {
                            // 1-N ForeignKey collection/array, so add FK to element table
                            AbstractClassMetaData elementCmd = null;
                            if (mmd.hasCollection())
                            {
                                // Collection
                                elementCmd = storeMgr.getMetaDataManager().getMetaDataForClass(mmd.getCollection().getElementType(), clr); 
                            }
                            else
                            {
                                // Array
                                elementCmd = storeMgr.getMetaDataManager().getMetaDataForClass(mmd.getType().getComponentType(), clr);
                            }
                            if (elementCmd == null)
                            {
                                String[] implClassNames = storeMgr.getMetaDataManager().getClassesImplementingInterface(mmd.getCollection().getElementType(), clr);
                                if (implClassNames != null && implClassNames.length > 0)
                                {
                                    // Collection/array of interface type so apply callback to all implementation types
                                    AbstractClassMetaData[] elementCmds = new AbstractClassMetaData[implClassNames.length];
                                    for (int i=0;i<implClassNames.length;i++)
                                    {
                                        elementCmds[i] = storeMgr.getMetaDataManager().getMetaDataForClass(implClassNames[i], clr);
                                    }

                                    // Run callbacks for each of the element classes.
                                    for (int i=0;i<elementCmds.length;i++)
                                    {
                                        storeMgr.addSchemaCallback(elementCmds[i].getFullClassName(), mmd);
                                        DatastoreClass dc = storeMgr.getDatastoreClass(elementCmds[i].getFullClassName(), clr);
                                        if (dc == null)
                                        {
                                            throw new NucleusException("Unable to add foreign-key to " + 
                                                elementCmds[i].getFullClassName() + " to " + this + " since element has no table!");
                                        }
                                        if (dc instanceof ClassTable)
                                        {
                                            ClassTable ct = (ClassTable) dc;
                                            if (ct.isInitialized())
                                            {
                                                // if the target table is already initialized, run the callbacks
                                                ct.runCallBacks(clr);
                                            }
                                        }
                                        else
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.info("Table " + toString() + " has to manage member " + mmd.getFullFieldName() +
                                                " yet the related element uses a VIEW so not remotely adding element FK owner column; assumed to be part of the VIEW definition");
                                        }
                                    }
                                }
                                else
                                {
                                    // Elements that are reference types or non-PC will come through here
                                    if (mmd.hasCollection())
                                    {
                                        NucleusLogger.METADATA.warn(Localiser.msg("057016", theCmd.getFullClassName(), mmd.getCollection().getElementType()));
                                    }
                                    else
                                    {
                                        NucleusLogger.METADATA.warn(Localiser.msg("057014", theCmd.getFullClassName(), mmd.getType().getComponentType().getName()));
                                    }
                                }
                            }
                            else
                            {
                                AbstractClassMetaData[] elementCmds = null;
                                // TODO : Cater for interface elements, and get the metadata for the implementation classes here
                                if (elementCmd.getBaseAbstractClassMetaData().getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
                                {
                                    // COMPLETE-TABLE inheritance in element, so really need FK in each!
                                    Collection<String> elementSubclassNames = storeMgr.getSubClassesForClass(elementCmd.getFullClassName(), true, clr);
                                    elementCmds = new ClassMetaData[elementSubclassNames != null ? 1+elementSubclassNames.size() : 1];
                                    int elemNo = 0;
                                    elementCmds[elemNo++] = elementCmd;
                                    if (elementSubclassNames != null)
                                    {
                                        for (String elementSubclassName : elementSubclassNames)
                                        {
                                            AbstractClassMetaData elemSubCmd = storeMgr.getMetaDataManager().getMetaDataForClass(elementSubclassName, clr);
                                            elementCmds[elemNo++] = elemSubCmd;
                                        }
                                    }
                                }
                                else if (elementCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
                                {
                                    elementCmds = storeMgr.getClassesManagingTableForClass(elementCmd, clr);
                                }
                                else
                                {
                                    elementCmds = new ClassMetaData[1];
                                    elementCmds[0] = elementCmd;
                                }

                                ElementMetaData elemmd = mmd.getElementMetaData();
                                if (elemmd != null && !StringUtils.isWhitespace(elemmd.getTable()))
                                {
                                    DatastoreIdentifier requiredTableId = storeMgr.getIdentifierFactory().newTableIdentifier(elemmd.getTable());
                                    DatastoreClass requiredTable = storeMgr.getDatastoreClass(requiredTableId);
                                    if (requiredTable != null)
                                    {
                                        // TODO Respect specification of table in ElementMetaData rather than just defaulting to table of element type
                                        // Note that this will need updates to FKListStore, FKSetStore etc to look for the table
                                        NucleusLogger.GENERAL.warn("Member=" + mmd.getFullFieldName() + " has 1-N FK with required table=" + requiredTable +
                                                " : we don't currently support specification of the element table, and always take the default table for the element type");
                                        /*for (int i=0;i<elementCmds.length;i++)
                                        {
                                            AbstractClassMetaData theElementCmd = elementCmds[i];
                                            while (theElementCmd != null)
                                            {
                                                if (requiredTable.managesClass(theElementCmd.getFullClassName()))
                                                {
                                                    if (theElementCmd != elementCmds[i])
                                                    {
                                                        elementCmds = new ClassMetaData[1];
                                                        elementCmds[0] = theElementCmd;
                                                        break;
                                                    }
                                                }
                                                theElementCmd = theElementCmd.getSuperAbstractClassMetaData();
                                            }
                                        }*/
                                    }
                                    else
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " specified element FK in table=" + elemmd.getTable() + 
                                            " but table not known. Ignoring.");
                                    }
                                }

                                // Run callbacks for each of the element classes
                                for (int i=0;i<elementCmds.length;i++)
                                {
                                    storeMgr.addSchemaCallback(elementCmds[i].getFullClassName(), mmd);
                                    DatastoreClass dc = storeMgr.getDatastoreClass(elementCmds[i].getFullClassName(), clr);
                                    if (dc != null) // If dc is null then we assume the (possible) element is abstract so no FK needed
                                    {
                                        if (dc instanceof ClassTable)
                                        {
                                            ClassTable ct = (ClassTable) dc;
                                            if (ct.isInitialized())
                                            {
                                                // if the target table is already initialized, run the callbacks
                                                ct.runCallBacks(clr);
                                            }
                                        }
                                        else
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.info("Table " + toString() + " has to manage member " + mmd.getFullFieldName() +
                                                    " yet the related element uses a VIEW so not remotely adding element FK owner column; assumed to be part of the VIEW definition");
                                        }
                                    }
                                }
                            }
                        }
                        else if (mmd.getMap() != null && !SCOUtils.mapHasSerialisedKeysAndValues(mmd))
                        {
                            // 1-N ForeignKey map, so add FK to value table
                            if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getMappedBy() != null)
                            {
                                // Key is stored in the value table so add the FK to the value table
                                AbstractClassMetaData valueCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(mmd.getMap().getValueType(), clr);
                                if (valueCmd == null)
                                {
                                    // Interface elements will come through here and java.lang.String and others as well
                                    NucleusLogger.METADATA.warn(Localiser.msg("057018", theCmd.getFullClassName(), mmd.getMap().getValueType()));
                                }
                                else
                                {
                                    AbstractClassMetaData[] valueCmds = null;
                                    // TODO : Cater for interface values, and get the metadata for the implementation classes here
                                    if (valueCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
                                    {
                                        valueCmds = storeMgr.getClassesManagingTableForClass(valueCmd, clr);
                                    }
                                    else
                                    {
                                        valueCmds = new ClassMetaData[1];
                                        valueCmds[0] = valueCmd;
                                    }
                                    
                                    // Run callbacks for each of the value classes.
                                    for (int i=0;i<valueCmds.length;i++)
                                    {
                                        storeMgr.addSchemaCallback(valueCmds[i].getFullClassName(), mmd);
                                        DatastoreClass dc = storeMgr.getDatastoreClass(valueCmds[i].getFullClassName(), clr);
                                        if (dc instanceof ClassTable)
                                        {
                                            ClassTable ct = (ClassTable) dc;
                                            if (ct.isInitialized())
                                            {
                                                // if the target table is already initialized, run the callbacks
                                                ct.runCallBacks(clr);
                                            }
                                        }
                                        else
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.info("Table " + toString() + " has to manage member " + mmd.getFullFieldName() +
                                                " yet the related value uses a VIEW so not remotely adding value owner FK column; assumed to be part of the VIEW definition");
                                        }
                                    }
                                }
                            }
                            else if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getMappedBy() != null)
                            {
                                // Value is stored in the key table so add the FK to the key table
                                AbstractClassMetaData keyCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(mmd.getMap().getKeyType(), clr);
                                if (keyCmd == null)
                                {
                                    // Interface elements will come through here and java.lang.String and others as well
                                    NucleusLogger.METADATA.warn(Localiser.msg("057019", theCmd.getFullClassName(), mmd.getMap().getKeyType()));
                                }
                                else
                                {
                                    AbstractClassMetaData[] keyCmds = null;
                                    // TODO : Cater for interface keys, and get the metadata for the implementation classes here
                                    if (keyCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
                                    {
                                        keyCmds = storeMgr.getClassesManagingTableForClass(keyCmd, clr);
                                    }
                                    else
                                    {
                                        keyCmds = new ClassMetaData[1];
                                        keyCmds[0] = keyCmd;
                                    }

                                    // Run callbacks for each of the key classes.
                                    for (int i=0;i<keyCmds.length;i++)
                                    {
                                        storeMgr.addSchemaCallback(keyCmds[i].getFullClassName(), mmd);
                                        DatastoreClass dc = storeMgr.getDatastoreClass(keyCmds[i].getFullClassName(), clr);
                                        if (dc instanceof ClassTable)
                                        {
                                            ClassTable ct = (ClassTable) dc;
                                            if (ct.isInitialized())
                                            {
                                                // if the target table is already initialized, run the callbacks
                                                ct.runCallBacks(clr);
                                            }
                                        }
                                        else
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.info("Table " + toString() + " has to manage member " + mmd.getFullFieldName() +
                                                " yet the related key uses a VIEW so not remotely adding key FK owner column; assumed to be part of the VIEW definition");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Adds on management of the columns in the defined MetaData that are "unmapped" (have no field associated).
     * @param theCmd ClassMetaData for the class to be managed
     * @param clr The ClassLoaderResolver
     */
    private void manageUnmappedColumns(AbstractClassMetaData theCmd, ClassLoaderResolver clr)
    {
        List cols = theCmd.getUnmappedColumns();
        if (cols != null && cols.size() > 0)
        {
            Iterator colsIter = cols.iterator();
            while (colsIter.hasNext())
            {
                ColumnMetaData colmd = (ColumnMetaData)colsIter.next();

                // Create a column with the specified name and jdbc-type
                if (colmd.getJdbcType() == JdbcType.VARCHAR && colmd.getLength() == null)
                {
                    colmd.setLength(storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STRING_DEFAULT_LENGTH));
                }
                IdentifierFactory idFactory = getStoreManager().getIdentifierFactory();
                DatastoreIdentifier colIdentifier = idFactory.newIdentifier(IdentifierType.COLUMN, colmd.getName());
                Column col = addColumn(null, colIdentifier, null, colmd);
                SQLTypeInfo sqlTypeInfo = storeMgr.getSQLTypeInfoForJDBCType(dba.getJDBCTypeForName(colmd.getJdbcTypeName()));
                col.setTypeInfo(sqlTypeInfo);

                if (unmappedColumns == null)
                {
                    unmappedColumns = new HashSet();
                }

                if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057011", col.toString(), colmd.getJdbcType()));
                }
                unmappedColumns.add(col);
            }
        }
    }

    /**
     * Accessor for whether this table manages the specified class
     * @param className Name of the class
     * @return Whether it is managed by this table
     */
    public boolean managesClass(String className)
    {
        if (className == null)
        {
            return false;
        }

        Iterator<AbstractClassMetaData> iter = managedClassMetaData.iterator();
        while (iter.hasNext())
        {
            AbstractClassMetaData managedCmd = iter.next();
            if (managedCmd.getFullClassName().equals(className))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Method to initialise the table primary key field(s).
     * @param clr The ClassLoaderResolver
     */
    protected void initializePK(ClassLoaderResolver clr)
    {
        assertIsPKUninitialized();
        AbstractMemberMetaData[] membersToAdd = new AbstractMemberMetaData[cmd.getNoOfPrimaryKeyMembers()];

        // Initialise Primary Key mappings for application id with PK fields in this class
        int pkFieldNum=0;
        int fieldCount = cmd.getNoOfManagedMembers();
        boolean hasPrimaryKeyInThisClass = false;
        if (cmd.getNoOfPrimaryKeyMembers() > 0)
        {
            pkMappings = new JavaTypeMapping[cmd.getNoOfPrimaryKeyMembers()];
            if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
            {
                // COMPLETE-TABLE so use root class metadata and add PK members
                // TODO Does this allow for overridden PK field info ?
                AbstractClassMetaData baseCmd = cmd.getBaseAbstractClassMetaData();
                fieldCount = baseCmd.getNoOfManagedMembers();
                for (int relFieldNum = 0; relFieldNum < fieldCount; ++relFieldNum)
                {
                    AbstractMemberMetaData mmd = baseCmd.getMetaDataForManagedMemberAtRelativePosition(relFieldNum);
                    if (mmd.isPrimaryKey())
                    {
                        AbstractMemberMetaData overriddenMmd = cmd.getOverriddenMember(mmd.getName());
                        if (overriddenMmd != null)
                        {
                            // PK field is overridden so use the overriding definition
                            mmd = overriddenMmd;
                        }

                        if (mmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                        {
                            membersToAdd[pkFieldNum++] = mmd;
                            hasPrimaryKeyInThisClass = true;
                        }
                        else if (mmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL)
                        {
                            throw new NucleusException(Localiser.msg("057006", mmd.getName())).setFatal();
                        }

                        // Check if auto-increment and that it is supported by this RDBMS
                        if ((mmd.getValueStrategy() == ValueGenerationStrategy.IDENTITY) && !dba.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS))
                        {
                            throw new NucleusException(Localiser.msg("057020", cmd.getFullClassName(), mmd.getName())).setFatal();
                        }
                    }
                }
            }
            else
            {
                for (int relFieldNum=0; relFieldNum<fieldCount; ++relFieldNum)
                {
                    AbstractMemberMetaData fmd = cmd.getMetaDataForManagedMemberAtRelativePosition(relFieldNum);
                    if (fmd.isPrimaryKey())
                    {
                        if (fmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                        {
                            membersToAdd[pkFieldNum++] = fmd;
                            hasPrimaryKeyInThisClass = true;
                        }
                        else if (fmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL)
                        {
                            throw new NucleusException(Localiser.msg("057006",fmd.getName())).setFatal();
                        }

                        // Check if auto-increment and that it is supported by this RDBMS
                        if ((fmd.getValueStrategy() == ValueGenerationStrategy.IDENTITY) && !dba.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS))
                        {
                            throw new NucleusException(Localiser.msg("057020", cmd.getFullClassName(), fmd.getName())).setFatal();
                        }
                    }
                }
            }
        }

        // No Primary Key defined, so search for superclass or handle datastore id
        if (!hasPrimaryKeyInThisClass)
        {
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                // application-identity
                // TODO rewrite this to just use metadata to get the PKs of the superclass(es). Any reason why not?
                DatastoreClass superTable = storeMgr.getDatastoreClass(cmd.getPersistableSuperclass(), clr);
                if (isPKInitialized())
                {
                    // The above call could have triggered a population of the PK here
                    return;
                }
                if (superTable == null && cmd.getPersistableSuperclass() != null)
                {
                    // The superclass doesn't have its own table, so keep going up til we find the next table
                    AbstractClassMetaData supercmd = cmd.getSuperAbstractClassMetaData();
                    while (true)
                    {
                        if (supercmd.getPersistableSuperclass() == null)
                        {
                            break;
                        }
                        superTable = storeMgr.getDatastoreClass(supercmd.getPersistableSuperclass(), clr);
                        if (isPKInitialized())
                        {
                            // The above call could have triggered a population of the PK here
                            return;
                        }
                        if (superTable != null)
                        {
                            break;
                        }
                        supercmd = supercmd.getSuperAbstractClassMetaData();
                        if (supercmd == null)
                        {
                            break;
                        }
                    }
                }

                if (superTable != null)
                {
                    // Superclass has a table so copy its PK mappings
                    ColumnMetaDataContainer colContainer = null;
                    if (cmd.getInheritanceMetaData() != null)
                    {
                        // Try via <inheritance><join>...</join></inheritance>
                        colContainer = cmd.getInheritanceMetaData().getJoinMetaData();
                    }
                    if (colContainer == null)
                    {
                        // Try via <primary-key>...</primary-key>
                        colContainer = cmd.getPrimaryKeyMetaData();
                    }

                    addApplicationIdUsingClassTableId(colContainer, superTable, clr, cmd);
                }
                else
                {
                    // No supertable to copy, so find superclass with PK fields and create new mappings and columns
                    AbstractClassMetaData pkCmd = getClassWithPrimaryKeyForClass(cmd.getSuperAbstractClassMetaData(), clr);
                    if (pkCmd != null)
                    {
                        // TODO Just use cmd.getPKMemberPositions to avoid iteration to find PKs
                        pkMappings = new JavaTypeMapping[pkCmd.getNoOfPrimaryKeyMembers()];
                        pkFieldNum = 0;
                        fieldCount = pkCmd.getNoOfInheritedManagedMembers() + pkCmd.getNoOfManagedMembers();
                        for (int absFieldNum = 0; absFieldNum < fieldCount; ++absFieldNum)
                        {
                            AbstractMemberMetaData fmd = pkCmd.getMetaDataForManagedMemberAtAbsolutePosition(absFieldNum);
                            if (fmd.isPrimaryKey())
                            {
                                AbstractMemberMetaData overriddenFmd = cmd.getOverriddenMember(fmd.getName());
                                if (overriddenFmd != null)
                                {
                                    // PK field is overridden so use the overriding definition
                                    fmd = overriddenFmd;
                                }
                                else
                                {
                                    AbstractClassMetaData thisCmd = cmd;
                                    while (thisCmd.getSuperAbstractClassMetaData() != null && thisCmd.getSuperAbstractClassMetaData() != pkCmd)
                                    {
                                        thisCmd = thisCmd.getSuperAbstractClassMetaData();
                                        overriddenFmd = thisCmd.getOverriddenMember(fmd.getName());
                                        if (overriddenFmd != null)
                                        {
                                            // PK field is overridden so use the overriding definition
                                            fmd = overriddenFmd;
                                            break;
                                        }
                                    }
                                }

                                if (fmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                                {
                                    membersToAdd[pkFieldNum++] = fmd;
                                }
                                else if (fmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL)
                                {
                                    throw new NucleusException(Localiser.msg("057006",fmd.getName())).setFatal();
                                }
                            }
                        }
                    }
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // datastore-identity
                ColumnMetaData colmd = null;
                if (cmd.getDatastoreIdentityMetaData() != null && cmd.getDatastoreIdentityMetaData().getColumnMetaData() != null)
                {
                    // Try via <datastore-identity>...</datastore-identity>
                    colmd = cmd.getDatastoreIdentityMetaData().getColumnMetaData();
                }
                if (colmd == null)
                {
                    // Try via <primary-key>...</primary-key>
                    if (cmd.getPrimaryKeyMetaData() != null && cmd.getPrimaryKeyMetaData().getColumnMetaData() != null &&
                        cmd.getPrimaryKeyMetaData().getColumnMetaData().length > 0)
                    {
                        colmd = cmd.getPrimaryKeyMetaData().getColumnMetaData()[0];
                    }
                }
                addDatastoreId(colmd, null, cmd);
            }
            else if (cmd.getIdentityType() == IdentityType.NONDURABLE)
            {
                // Do nothing since no identity!
            }
        }

        //add field mappings in the end, so we compute all columns after the post initialize
        for (int i=0; i<membersToAdd.length; i++)
        {
            if (membersToAdd[i] != null)
            {
                try
                {
                    DatastoreClass datastoreClass = getStoreManager().getDatastoreClass(membersToAdd[i].getType().getName(), clr);
                    if (datastoreClass.getIdMapping() == null)
                    {
                        throw new NucleusException("Unsupported relationship with field "+membersToAdd[i].getFullFieldName()).setFatal();
                    }
                }
                catch (NoTableManagedException ex)
                {
                    //do nothing
                }
                JavaTypeMapping fieldMapping = storeMgr.getMappingManager().getMapping(this, membersToAdd[i], clr, FieldRole.ROLE_FIELD);
                addMemberMapping(fieldMapping);
                pkMappings[i] = fieldMapping;
            }
        }
        initializeIDMapping();
        
        state = TABLE_STATE_PK_INITIALIZED;
    }

    /**
     * Utility to navigate the inheritance hierarchy to find the base class that defines the primary keys for this tree. 
     * This has the assumption that there is no supertable, and will go up until it finds the superclass which has PK fields but no classes above.
     * @param cmd AbstractClassMetaData for this class
     * @param clr The ClassLoaderResolver
     * @return The AbstractClassMetaData for the class defining the primary keys
     */
    private AbstractClassMetaData getClassWithPrimaryKeyForClass(AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        if (cmd == null)
        {
            return null;
        }

        // Base class should have primary key fields. Note that in JPA that is not necessarily correct but why
        // anyone would put other things in the root but not the PK is beyond my limited understanding

        if (cmd.getSuperAbstractClassMetaData() == null)
        {
            // No more superclasses, so just return the last one
            return cmd;
        }

        if (cmd.getNoOfPrimaryKeyMembers() > 0 && cmd.getSuperAbstractClassMetaData().getNoOfPrimaryKeyMembers() == 0)
        {
            // This class has PK members but the superclass doesn't, so return this
            return cmd;
        }

        return getClassWithPrimaryKeyForClass(cmd.getSuperAbstractClassMetaData(), clr);
    }

    /**
     * Method to initialise this table to include all fields in the specified class.
     * This is used to recurse up the hierarchy so that we include all immediate superclasses
     * that have "subclass-table" specified as their inheritance strategy. If we encounter the parent of
     * this class with other than "subclass-table" we stop the process.
     * @param theCmd The ClassMetaData for the class
     */
    private void initializeForClass(AbstractClassMetaData theCmd, ClassLoaderResolver clr)
    {
        String columnOrdering = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_TABLE_COLUMN_ORDER);
        if (columnOrdering.equalsIgnoreCase("superclass-first"))
        {
            // Superclasses persisted into this table
            AbstractClassMetaData parentCmd = theCmd.getSuperAbstractClassMetaData();
            if (parentCmd != null)
            {
                if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
                {
                    // Managed class requires all superclasses managed here too
                    initializeForClass(parentCmd, clr);
                }
                else if (parentCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
                {
                    // Superclass uses "subclass-table" so needs managing here
                    initializeForClass(parentCmd, clr);
                }
            }

            // Owning class
            manageClass(theCmd, clr);
        }
        else
        {
            // Owning class
            manageClass(theCmd, clr);

            // Superclasses persisted into this table
            AbstractClassMetaData parentCmd = theCmd.getSuperAbstractClassMetaData();
            if (parentCmd != null)
            {
                if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
                {
                    // Managed class requires all superclasses managed here too
                    initializeForClass(parentCmd, clr);
                }
                else if (parentCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
                {
                    // Superclass uses "subclass-table" so needs managing here
                    initializeForClass(parentCmd, clr);
                }
            }
        }
    }

    /**
     * Execute the callbacks for the classes that this table maps to.
     * @param clr ClassLoader resolver
     */
    private void runCallBacks(ClassLoaderResolver clr)
    {
        // Run callbacks for all classes managed by this table
        Iterator<AbstractClassMetaData> cmdIter = managedClassMetaData.iterator();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData managedCmd = cmdIter.next();
            if (managingClassCurrent != null && managingClassCurrent.equals(managedCmd.getFullClassName()))
            {
                // We can't run callbacks for this class since it is still being initialised. Mark callbacks to run after it completes
                runCallbacksAfterManageClass = true;
                break;
            }

            Collection processedCallbacks = callbacksAppliedForManagedClass.get(managedCmd.getFullClassName());
            Collection c = (Collection)storeMgr.getSchemaCallbacks().get(managedCmd.getFullClassName());
            if (c != null)
            {
                if (processedCallbacks == null)
                {
                    processedCallbacks = new HashSet();
                    callbacksAppliedForManagedClass.put(managedCmd.getFullClassName(), processedCallbacks);
                }
                for (Iterator it = c.iterator(); it.hasNext();)
                {
                    AbstractMemberMetaData callbackMmd = (AbstractMemberMetaData) it.next();
                    if (processedCallbacks.contains(callbackMmd))
                    {
                        continue;
                    }

                    processedCallbacks.add(callbackMmd);

                    if (callbackMmd.getJoinMetaData() == null)
                    {
                        // 1-N FK relationship
                        AbstractMemberMetaData ownerFmd = callbackMmd;
                        if (ownerFmd.getMappedBy() != null)
                        {
                            // Bidirectional (element has a PC mapping to the owner)
                            // Check that the "mapped-by" field in the other class actually exists
                            AbstractMemberMetaData fmd = null;
                            if (ownerFmd.getMappedBy().indexOf('.') > 0)
                            {
                                // TODO Can we just use getRelatedMemberMetaData always?
                                AbstractMemberMetaData[] relMmds = ownerFmd.getRelatedMemberMetaData(clr);
                                fmd = (relMmds != null && relMmds.length > 0) ? relMmds[0] : null;
                            }
                            else
                            {
                                fmd = managedCmd.getMetaDataForMember(ownerFmd.getMappedBy());
                            }
                            if (fmd == null)
                            {
                                throw new NucleusUserException(Localiser.msg("057036", ownerFmd.getMappedBy(), managedCmd.getFullClassName(), ownerFmd.getFullFieldName()));
                            }

                            if (ownerFmd.getMap() != null && storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_UNIQUE_CONSTRAINTS_MAP_INVERSE))
                            {
                                initializeFKMapUniqueConstraints(ownerFmd);
                            }

                            boolean duplicate = false;
                            JavaTypeMapping fkDiscrimMapping = null;
                            JavaTypeMapping orderMapping = null;
                            if (ownerFmd.hasExtension(MetaData.EXTENSION_MEMBER_RELATION_DISCRIM_COLUMN))
                            {
                                // Collection has a relation discriminator so we need to share the FK. Check for the required discriminator
                                String colName = ownerFmd.getValueForExtension(MetaData.EXTENSION_MEMBER_RELATION_DISCRIM_COLUMN);
                                if (colName == null)
                                {
                                    // No column defined so use a fallback name
                                    colName = "RELATION_DISCRIM";
                                }
                                Set fkDiscrimEntries = getExternalFkDiscriminatorMappings().entrySet();
                                Iterator discrimMappingIter = fkDiscrimEntries.iterator();
                                while (discrimMappingIter.hasNext())
                                {
                                    Map.Entry entry = (Map.Entry)discrimMappingIter.next();
                                    JavaTypeMapping discrimMapping = (JavaTypeMapping)entry.getValue();
                                    String discrimColName = (discrimMapping.getColumnMapping(0).getColumn().getColumnMetaData()).getName();
                                    if (discrimColName.equalsIgnoreCase(colName))
                                    {
                                        duplicate = true;
                                        fkDiscrimMapping = discrimMapping;
                                        orderMapping = getExternalOrderMappings().get(entry.getKey());
                                        break;
                                    }
                                }

                                if (!duplicate)
                                {
                                    // Create the relation discriminator column since we dont have this discriminator
                                    ColumnMetaData colmd = new ColumnMetaData();
                                    colmd.setName(colName);
                                    colmd.setAllowsNull(Boolean.TRUE); // Allow for elements not in any discriminated collection
                                    fkDiscrimMapping = storeMgr.getMappingManager().getMapping(String.class); // Only support String discriminators currently
                                    fkDiscrimMapping.setTable(this);
                                    ColumnCreator.createIndexColumn(fkDiscrimMapping, storeMgr, clr, this, colmd, false);
                                }

                                if (fkDiscrimMapping != null)
                                {
                                    getExternalFkDiscriminatorMappings().put(ownerFmd, fkDiscrimMapping);
                                }
                            }

                            // Add the order mapping as necessary
                            addOrderMapping(ownerFmd, orderMapping, clr);
                        }
                        else
                        {
                            // Unidirectional (element knows nothing about the owner)
                            String ownerClassName = ownerFmd.getAbstractClassMetaData().getFullClassName();
                            JavaTypeMapping fkMapping = new PersistableMapping();
                            fkMapping.setTable(this);
                            fkMapping.initialize(storeMgr, ownerClassName);
                            JavaTypeMapping fkDiscrimMapping = null;
                            JavaTypeMapping orderMapping = null;
                            boolean duplicate = false;

                            try
                            {
                                // Get the owner id mapping of the "1" end
                                DatastoreClass ownerTbl = storeMgr.getDatastoreClass(ownerClassName, clr);
                                if (ownerTbl == null)
                                {
                                    // Class doesn't have its own table (subclass-table) so find where it persists
                                    AbstractClassMetaData[] ownerParentCmds = storeMgr.getClassesManagingTableForClass(ownerFmd.getAbstractClassMetaData(), clr);
                                    if (ownerParentCmds.length > 1)
                                    {
                                        throw new NucleusUserException("Relation (" + ownerFmd.getFullFieldName() +
                                            ") with multiple related tables (using subclass-table). Not supported");
                                    }
                                    ownerClassName = ownerParentCmds[0].getFullClassName();
                                    ownerTbl = storeMgr.getDatastoreClass(ownerClassName, clr);
                                    if (ownerTbl == null)
                                    {
                                        throw new NucleusException("Failed to get owner table at other end of relation for field=" + ownerFmd.getFullFieldName());
                                    }
                                }

                                JavaTypeMapping ownerIdMapping = ownerTbl.getIdMapping();
                                ColumnMetaDataContainer colmdContainer = null;
                                if (ownerFmd.hasCollection() || ownerFmd.hasArray())
                                {
                                    // 1-N Collection/array
                                    colmdContainer = ownerFmd.getElementMetaData();
                                }
                                else if (ownerFmd.hasMap() && ownerFmd.getKeyMetaData() != null && ownerFmd.getKeyMetaData().getMappedBy() != null)
                                {
                                    // 1-N Map with key stored in the value
                                    colmdContainer = ownerFmd.getValueMetaData();
                                }
                                else if (ownerFmd.hasMap() && ownerFmd.getValueMetaData() != null && ownerFmd.getValueMetaData().getMappedBy() != null)
                                {
                                    // 1-N Map with value stored in the key
                                    colmdContainer = ownerFmd.getKeyMetaData();
                                }
                                CorrespondentColumnsMapper correspondentColumnsMapping = new CorrespondentColumnsMapper(colmdContainer, this, ownerIdMapping, true);
                                int countIdFields = ownerIdMapping.getNumberOfColumnMappings();
                                for (int i=0; i<countIdFields; i++)
                                {
                                    ColumnMapping refColumnMapping = ownerIdMapping.getColumnMapping(i);
                                    JavaTypeMapping mapping = storeMgr.getMappingManager().getMapping(refColumnMapping.getJavaTypeMapping().getJavaType());
                                    ColumnMetaData colmd = correspondentColumnsMapping.getColumnMetaDataByIdentifier(refColumnMapping.getColumn().getIdentifier());
                                    if (colmd == null)
                                    {
                                        throw new NucleusUserException(Localiser.msg("057035", refColumnMapping.getColumn().getIdentifier(), toString())).setFatal();
                                    }

                                    DatastoreIdentifier identifier = null;
                                    IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
                                    if (colmd.getName() == null || colmd.getName().length() < 1)
                                    {
                                        // No user provided name so generate one
                                        identifier = idFactory.newForeignKeyFieldIdentifier(ownerFmd, null, refColumnMapping.getColumn().getIdentifier(), 
                                            storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(mapping.getJavaType()), FieldRole.ROLE_OWNER);
                                    }
                                    else
                                    {
                                        // User-defined name
                                        identifier = idFactory.newColumnIdentifier(colmd.getName());
                                    }
                                    Column refColumn = addColumn(mapping.getJavaType().getName(), identifier, mapping, colmd);
                                    refColumnMapping.getColumn().copyConfigurationTo(refColumn);

                                    if ((colmd.getAllowsNull() == null) || (colmd.getAllowsNull() != null && colmd.isAllowsNull()))
                                    {
                                        // User either wants it nullable, or haven't specified anything, so make it nullable
                                        refColumn.setNullable(true);
                                    }

                                    fkMapping.addColumnMapping(getStoreManager().getMappingManager().createColumnMapping(mapping, refColumn, refColumnMapping.getJavaTypeMapping().getJavaType().getName()));
                                    ((PersistableMapping)fkMapping).addJavaTypeMapping(mapping);
                                }
                            }
                            catch (DuplicateColumnException dce)
                            {
                                // If the user hasnt specified "relation-discriminator-column" here we dont allow the sharing of columns
                                if (!ownerFmd.hasExtension(MetaData.EXTENSION_MEMBER_RELATION_DISCRIM_COLUMN))
                                {
                                    throw dce;
                                }

                                // Find the FK using this column and use it instead of creating a new one since we're sharing
                                Iterator fkIter = getExternalFkMappings().entrySet().iterator();
                                fkMapping = null;
                                while (fkIter.hasNext())
                                {
                                    Map.Entry entry = (Map.Entry)fkIter.next();
                                    JavaTypeMapping existingFkMapping = (JavaTypeMapping)entry.getValue();
                                    for (int j=0;j<existingFkMapping.getNumberOfColumnMappings();j++)
                                    {
                                        if (existingFkMapping.getColumnMapping(j).getColumn().getIdentifier().toString().equals(dce.getConflictingColumn().getIdentifier().toString()))
                                        {
                                            // The FK is shared (and so if it is a List we also share the index)
                                            fkMapping = existingFkMapping;
                                            fkDiscrimMapping = externalFkDiscriminatorMappings.get(entry.getKey());
                                            orderMapping = getExternalOrderMappings().get(entry.getKey());
                                            break;
                                        }
                                    }
                                }
                                if (fkMapping == null)
                                {
                                    // Should never happen since we know there is a col duplicating ours
                                    throw dce;
                                }
                                duplicate = true;
                            }

                            if (!duplicate && ownerFmd.hasExtension(MetaData.EXTENSION_MEMBER_RELATION_DISCRIM_COLUMN))
                            {
                                // Create the relation discriminator column
                                String colName = ownerFmd.getValueForExtension(MetaData.EXTENSION_MEMBER_RELATION_DISCRIM_COLUMN);
                                if (colName == null)
                                {
                                    // No column defined so use a fallback name
                                    colName = "RELATION_DISCRIM";
                                }
                                ColumnMetaData colmd = new ColumnMetaData();
                                colmd.setName(colName);
                                colmd.setAllowsNull(Boolean.TRUE); // Allow for elements not in any discriminated collection
                                fkDiscrimMapping = storeMgr.getMappingManager().getMapping(String.class); // Only support String discriminators currently
                                fkDiscrimMapping.setTable(this);
                                ColumnCreator.createIndexColumn(fkDiscrimMapping, storeMgr, clr, this, colmd, false);
                            }

                            // Save the external FK
                            getExternalFkMappings().put(ownerFmd, fkMapping);
                            if (fkDiscrimMapping != null)
                            {
                                getExternalFkDiscriminatorMappings().put(ownerFmd, fkDiscrimMapping);
                            }

                            // Add the order mapping as necessary
                            addOrderMapping(ownerFmd, orderMapping, clr);
                        }
                    }
                }
            }
        }
    }

    /**
     * Convenience method to add an order mapping to the table. Used with 1-N FK "indexed List"/array relations.
     * @param mmd Owner field MetaData
     * @param orderMapping The order mapping (maybe already set and just needs adding)
     * @param clr ClassLoader resolver
     * @return The order mapping (if updated)
     */
    private JavaTypeMapping addOrderMapping(AbstractMemberMetaData mmd, JavaTypeMapping orderMapping, ClassLoaderResolver clr)
    {
        boolean needsOrderMapping = false;
        OrderMetaData omd = mmd.getOrderMetaData();
        if (mmd.hasArray())
        {
            // Array field always has the index mapping
            needsOrderMapping = true;
        }
        else if (List.class.isAssignableFrom(mmd.getType()))
        {
            // List field
            needsOrderMapping = true;
            if (omd != null && !omd.isIndexedList())
            {
                // "ordered List" so no order mapping is needed
                needsOrderMapping = false;
            }
        }
        else if (java.util.Collection.class.isAssignableFrom(mmd.getType()) && omd != null && omd.isIndexedList())
        {
            // Collection field with <order> and is indexed list so needs order mapping
            needsOrderMapping = true;
            if (omd.getMappedBy() != null)
            {
                // Try to find the mapping if already created
                orderMapping = getMemberMapping(omd.getMappedBy());
            }
        }

        if (needsOrderMapping)
        {
            // if the field is list or array type, add index column
            state = TABLE_STATE_NEW;
            if (orderMapping == null)
            {
                // Create new order mapping since we need one and we aren't using a shared FK
                orderMapping = this.addOrderColumn(mmd, clr);
            }
            getExternalOrderMappings().put(mmd, orderMapping);
            state = TABLE_STATE_INITIALIZED;
        }

        return orderMapping;
    }

    /**
     * Accessor for the main class represented.
     * @return The name of the class
     **/
    public String getType()
    {
        return cmd.getFullClassName();
    }

    /**
     * Accessor for the identity-type. 
     * @return identity-type tag value
     */    
    public IdentityType getIdentityType()
    {
        return cmd.getIdentityType();
    }

    /**
     * Accessor for versionMetaData
     * @return Returns the versionMetaData.
     */
    public final VersionMetaData getVersionMetaData()
    {
        return versionMetaData;
    }

    /**
     * Accessor for Discriminator MetaData
     * @return Returns the Discriminator MetaData.
     */
    public final DiscriminatorMetaData getDiscriminatorMetaData()
    {
        return discriminatorMetaData;
    }

    /**
     * Convenience method to return the root table with a discriminator in this inheritance tree.
     * @return The root table which has the discriminator in this inheritance tree
     */
    public final ClassTable getTableWithDiscriminator()
    {
        if (supertable != null)
        {
            ClassTable tbl = supertable.getTableWithDiscriminator();
            if (tbl != null)
            {
                return tbl;
            }
        }

        if (discriminatorMetaData != null)
        {
            // Initialised and discriminator metadata set so return this
            return this;
        }
        else if (cmd.getInheritanceMetaData() != null && cmd.getInheritanceMetaData().getDiscriminatorMetaData() != null)
        {
            // Not initialised but has discriminator MetaData so return this
            return this;
        }

        return null;
    }

    /**
     * Whether this table or super table has id (primary key) attributed by the datastore
     * @return true if the id attributed by the datastore
     */
    public boolean isObjectIdDatastoreAttributed()
    {
        boolean attributed = storeMgr.isValueGenerationStrategyDatastoreAttributed(cmd, -1);
        if (attributed)
        {
            return true;
        }
        for (int i=0; i<columns.size(); i++)
        {
            Column col = (Column)columns.get(i);
            if (col.isPrimaryKey() && col.isIdentity())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether this table is the base table in the inheritance hierarchy.
     * @return true if this table is a root table
     */
    public boolean isBaseDatastoreClass()
    {
        return supertable == null ? true : false;
    }

    public DatastoreClass getBaseDatastoreClass()
    {
        if (supertable != null)
        {
            return supertable.getBaseDatastoreClass();
        }
        return this;
    }

    /**
     * Accessor for the supertable for this table.
     * @return The supertable
     **/
    public DatastoreClass getSuperDatastoreClass()
    {
        assertIsInitialized();

        return supertable;
    }

    /**
     * Accessor whether the supplied DatastoreClass is a supertable of this table.
     * @param table The DatastoreClass to check
     * @return Whether it is a supertable (somewhere up the inheritance tree)
     */
    public boolean isSuperDatastoreClass(DatastoreClass table)
    {
        if (this == table)
        {
            return true;
        }
        else if (supertable != null)
        {
            if (table == supertable)
            {
                return true;
            }

            return supertable.isSuperDatastoreClass(table);
        }
        return false;
    }

    /**
     * Accessor for any secondary tables for this table.
     * @return Secondary tables (if any)
     */
    public Collection getSecondaryDatastoreClasses()
    {
        return (secondaryTables != null ? secondaryTables.values() : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Table#getSurrogateMapping(org.datanucleus.store.schema.table.SurrogateColumnType, boolean)
     */
    @Override
    public JavaTypeMapping getSurrogateMapping(SurrogateColumnType colType, boolean allowSuperclasses)
    {
        if (colType == SurrogateColumnType.DISCRIMINATOR)
        {
            if (discriminatorMapping != null)
            {
                // We have the mapping so return it
                return discriminatorMapping;
            }
            if (allowSuperclasses && supertable != null)
            {
                // Return what the supertable has if it has the mapping
                return supertable.getSurrogateMapping(colType, allowSuperclasses);
            }
        }
        else if (colType == SurrogateColumnType.VERSION)
        {
            if (versionMapping != null)
            {
                // We have the mapping so return it
                return versionMapping;
            }
            if (allowSuperclasses && supertable != null)
            {
                // Return what the supertable has if it has the mapping
                return supertable.getSurrogateMapping(colType, allowSuperclasses);
            }
        }

        return super.getSurrogateMapping(colType, allowSuperclasses);
    }

    public ClassTable getTableManagingMapping(JavaTypeMapping mapping)
    {
        if (managesMapping(mapping))
        {
            return this;
        }
        else if (supertable != null)
        {
            return supertable.getTableManagingMapping(mapping);
        }
        return null;
    }

    /**
     * Utility to find the table above this one. Will recurse to cater for inheritance
     * strategies where fields are handed up to the super class, or down to this class.
     * @param theCmd ClassMetaData of the class to find the supertable for.
     * @return The table above this one in any inheritance hierarchy
     */
    private ClassTable getSupertable(AbstractClassMetaData theCmd, ClassLoaderResolver clr)
    {
        if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
        {
            // "complete-table" has no super table. All is persisted into this table
            return null;
        }

        AbstractClassMetaData superCmd = theCmd.getSuperAbstractClassMetaData();
        if (superCmd != null)
        {
            if (superCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.NEW_TABLE)
            {
                // This class has its own table, so return it.
                return (ClassTable) storeMgr.getDatastoreClass(superCmd.getFullClassName(), clr);
            }
            else if (superCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
            {
                // This class is mapped to the same table, so go up another level.
                return getSupertable(superCmd, clr);
            }
            else
            {
                // This class is mapped to its superclass table, so go up to that and try again.
                return getSupertable(superCmd, clr);
            }
        }
        return null;
    }

    /**
     * Convenience accessor for the base table for this table which has the specified field.
     * @param mmd Field MetaData for this field
     * @return The base table which has the field specified
     */
    public DatastoreClass getBaseDatastoreClassWithMember(AbstractMemberMetaData mmd)
    {
        if (mmd == null)
        {
            return null;
        }
        if (mmd.isPrimaryKey())
        {
            // If this is a PK field then it will be in all tables up to the base so we need to continue navigating up
            if (getSuperDatastoreClass() != null)
            {
                return getSuperDatastoreClass().getBaseDatastoreClassWithMember(mmd);
            }
        }
        if (memberMappingsMap.get(mmd) != null)
        {
            // We have this field so return this table
            return this;
        }
        else if (externalFkMappings != null && externalFkMappings.get(mmd) != null)
        {
            return this;
        }
        else if (externalFkDiscriminatorMappings != null && externalFkDiscriminatorMappings.get(mmd) != null)
        {
            return this;
        }
        else if (externalOrderMappings != null && externalOrderMappings.get(mmd) != null)
        {
            return this;
        }
        else if (getSuperDatastoreClass() == null)
        {
            // We don't have the field, but have no superclass, so return null
            return this;
        }
        else
        {
            // Return the superclass since we don't have it
            return getSuperDatastoreClass().getBaseDatastoreClassWithMember(mmd);
        }
    }

    /**
     * Accessor for the (primary) class MetaData.
     * Package-level access to restrict to other table types only.
     * @return The (primary) class MetaData
     **/
    public ClassMetaData getClassMetaData()
    {
        return cmd;
    }

    /**
     * Accessor for the indices for this table. This includes both the
     * user-defined indices (via MetaData), and the ones required by foreign
     * keys (required by relationships).
     * @param clr The ClassLoaderResolver
     * @return The indices
     */
    protected Set<Index> getExpectedIndices(ClassLoaderResolver clr)
    {
        // Auto mode allows us to decide which indices are needed as well as using what is in the users MetaData
        boolean autoMode = false;
        if (storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE).equals("DataNucleus"))
        {
            autoMode = true;
        }

        Set<Index> indices = new HashSet();

        // Add on any user-required indices for the fields/properties
        Set memberNumbersSet = memberMappingsMap.keySet();
        Iterator iter = memberNumbersSet.iterator();
        while (iter.hasNext())
        {
            AbstractMemberMetaData fmd = (AbstractMemberMetaData) iter.next();
            JavaTypeMapping fieldMapping = memberMappingsMap.get(fmd);
            if (fieldMapping instanceof EmbeddedPCMapping)
            {
                // Add indexes for fields of this embedded PC object
                EmbeddedPCMapping embMapping = (EmbeddedPCMapping)fieldMapping;
                for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
                {
                    JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                    IndexMetaData imd = embFieldMapping.getMemberMetaData().getIndexMetaData();
                    if (imd != null)
                    {
                        Index index = TableUtils.getIndexForField(this, imd, embFieldMapping);
                        if (index != null)
                        {
                            indices.add(index);
                        }
                    }
                }
            }
            else if (fieldMapping instanceof SerialisedMapping)
            {
                // Don't index these
            }
            else
            {
                // Add any required index for this field
                IndexMetaData imd = fmd.getIndexMetaData();
                if (imd != null)
                {
                    // Index defined so add it
                    Index index = TableUtils.getIndexForField(this, imd, fieldMapping);
                    if (index != null)
                    {
                        indices.add(index);
                    }
                }
                else if (autoMode)
                {
                    if (fmd.getIndexed() == null)
                    {
                        // Indexing not set, so add where we think it is appropriate
                        if (!fmd.isPrimaryKey()) // Ignore PKs since they will be indexed anyway
                        {
                            // TODO Some RDBMS create index automatically for all FK cols so we don't need to really
                            RelationType relationType = fmd.getRelationType(clr);
                            if (relationType == RelationType.ONE_TO_ONE_UNI)
                            {
                                // 1-1 with FK at this side so index the FK
                                if (fieldMapping instanceof ReferenceMapping)
                                {
                                    ReferenceMapping refMapping = (ReferenceMapping)fieldMapping;
                                    if (refMapping.getMappingStrategy() == ReferenceMapping.PER_IMPLEMENTATION_MAPPING)
                                    {
                                        // Cols per implementation : index each of implementations
                                        if (refMapping.getJavaTypeMapping() != null)
                                        {
                                            int colNum = 0;
                                            JavaTypeMapping[] implMappings = refMapping.getJavaTypeMapping();
                                            for (int i=0;i<implMappings.length;i++)
                                            {
                                                int numColsInImpl = implMappings[i].getNumberOfColumnMappings();
                                                Index index = new Index(this, false, null);
                                                for (int j=0;j<numColsInImpl;j++)
                                                {
                                                    index.setColumn(j, fieldMapping.getColumnMapping(colNum++).getColumn());
                                                }
                                                indices.add(index);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    Index index = new Index(this, false, null);
                                    for (int i=0;i<fieldMapping.getNumberOfColumnMappings();i++)
                                    {
                                        index.setColumn(i, fieldMapping.getColumnMapping(i).getColumn());
                                    }
                                    indices.add(index);
                                }
                            }
                            else if (relationType == RelationType.ONE_TO_ONE_BI && fmd.getMappedBy() == null)
                            {
                                // 1-1 with FK at this side so index the FK
                                Index index = new Index(this, false, null);
                                for (int i=0;i<fieldMapping.getNumberOfColumnMappings();i++)
                                {
                                    index.setColumn(i, fieldMapping.getColumnMapping(i).getColumn());
                                }
                                indices.add(index);
                            }
                            else if (relationType == RelationType.MANY_TO_ONE_BI)
                            {
                                // N-1 with FK at this side so index the FK
                                AbstractMemberMetaData relMmd = fmd.getRelatedMemberMetaData(clr)[0];
                                if (relMmd.getJoinMetaData() == null && fmd.getJoinMetaData() == null)
                                {
                                    if (fieldMapping.getNumberOfColumnMappings() > 0)
                                    {
                                        Index index = new Index(this, false, null);
                                        for (int i=0;i<fieldMapping.getNumberOfColumnMappings();i++)
                                        {
                                            index.setColumn(i, fieldMapping.getColumnMapping(i).getColumn());
                                        }
                                        indices.add(index);
                                    }
                                    else
                                    {
                                        // TODO How do we get this?
                                        NucleusLogger.DATASTORE_SCHEMA.warn("Table " + this + " manages member " + fmd.getFullFieldName() + 
                                            " which is a N-1 but there is no column for this mapping so not adding index!");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check if any version column needs indexing
        if (versionMapping != null)
        {
            IndexMetaData idxmd = getVersionMetaData().getIndexMetaData();
            if (idxmd != null)
            {
                Index index = new Index(this, idxmd.isUnique(), idxmd.getExtensions());
                if (idxmd.getName() != null)
                {
                    index.setName(idxmd.getName());
                }
                int countVersionFields = versionMapping.getNumberOfColumnMappings();
                for (int i=0; i<countVersionFields; i++)
                {
                    index.addColumn(versionMapping.getColumnMapping(i).getColumn());
                }
                indices.add(index);
            }
        }

        // Check if any discriminator column needs indexing
        if (discriminatorMapping != null)
        {
            DiscriminatorMetaData dismd = getDiscriminatorMetaData();
            IndexMetaData idxmd = dismd.getIndexMetaData();
            if (idxmd != null)
            {
                Index index = new Index(this, idxmd.isUnique(), idxmd.getExtensions());
                if (idxmd.getName() != null)
                {
                    index.setName(idxmd.getName());
                }
                int numCols = discriminatorMapping.getNumberOfColumnMappings();
                for (int i=0; i<numCols; i++)
                {
                    index.addColumn(discriminatorMapping.getColumnMapping(i).getColumn());
                }
                indices.add(index);
            }
        }

        // Check if multitenancy discriminator present and needs indexing
        if (multitenancyMapping != null)
        {
            MultitenancyMetaData mtmd = cmd.getMultitenancyMetaData();
            IndexMetaData idxmd = mtmd.getIndexMetaData();
            if (idxmd != null)
            {
                Index index = new Index(this, false, null);
                if (idxmd.getName() != null)
                {
                    index.setName(idxmd.getName());
                }
                int numCols = multitenancyMapping.getNumberOfColumnMappings();
                for (int i=0;i<numCols;i++)
                {
                    index.addColumn(multitenancyMapping.getColumnMapping(i).getColumn());
                }
                indices.add(index);
            }
        }

        // Check if soft delete column present and needs indexing
        if (softDeleteMapping != null)
        {
            SoftDeleteMetaData sdmd = cmd.getSoftDeleteMetaData();
            IndexMetaData idxmd = sdmd.getIndexMetaData();
            if (idxmd != null)
            {
                Index index = new Index(this, false, null);
                if (idxmd.getName() != null)
                {
                    index.setName(idxmd.getName());
                }
                int numCols = softDeleteMapping.getNumberOfColumnMappings();
                for (int i=0;i<numCols;i++)
                {
                    index.addColumn(softDeleteMapping.getColumnMapping(i).getColumn());
                }
                indices.add(index);
            }
        }

        // Add on any order fields (for lists, arrays, collections) that need indexing
        Set orderMappingsEntries = getExternalOrderMappings().entrySet();
        Iterator orderMappingsEntriesIter = orderMappingsEntries.iterator();
        while (orderMappingsEntriesIter.hasNext())
        {
            Map.Entry entry = (Map.Entry)orderMappingsEntriesIter.next();
            AbstractMemberMetaData fmd = (AbstractMemberMetaData)entry.getKey();
            JavaTypeMapping mapping = (JavaTypeMapping)entry.getValue();
            OrderMetaData omd = fmd.getOrderMetaData();
            if (omd != null && omd.getIndexMetaData() != null)
            {
                Index index = getIndexForIndexMetaDataAndMapping(omd.getIndexMetaData(), mapping);
                if (index != null)
                {
                    indices.add(index);
                }
            }
        }

        // Add on any user-required indices for the class(es) as a whole (subelement of <class>)
        Iterator<AbstractClassMetaData> cmdIter = managedClassMetaData.iterator();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData thisCmd = cmdIter.next();
            List<IndexMetaData> classIndices = thisCmd.getIndexMetaData();
            if (classIndices != null)
            {
                for (IndexMetaData idxmd : classIndices)
                {
                    Index index = getIndexForIndexMetaData(idxmd);
                    if (index != null)
                    {
                        indices.add(index);
                    }
                }
            }
        }

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Make sure there is no reuse of PK fields that cause a duplicate index for the PK. Remove it if required
            PrimaryKey pk = getPrimaryKey();
            Iterator<Index> indicesIter = indices.iterator();
            while (indicesIter.hasNext())
            {
                Index idx = indicesIter.next();
                if (idx.getColumnList().equals(pk.getColumnList()))
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug("Index " + idx + " is for the same columns as the PrimaryKey so being removed from expected set of indices. PK is always indexed");
                    indicesIter.remove();
                }
            }
        }

        return indices;
    }

    /**
     * Convenience method to convert an IndexMetaData and a mapping into an Index.
     * @param imd The Index MetaData
     * @param mapping The mapping
     * @return The Index
     */
    private Index getIndexForIndexMetaDataAndMapping(IndexMetaData imd, JavaTypeMapping mapping)
    {
        // Verify if a unique index is needed
        boolean unique = imd.isUnique();

        Index index = new Index(this, unique, imd.getExtensions());

        // Set the index name if required
        if (imd.getName() != null)
        {
            index.setName(imd.getName());
        }

        int numCols = mapping.getNumberOfColumnMappings();
        for (int i=0;i<numCols;i++)
        {
            index.addColumn(mapping.getColumnMapping(i).getColumn());
        }

        return index;
    }

    /**
     * Convenience method to convert an IndexMetaData into an Index.
     * @param imd The Index MetaData
     * @return The Index
     */
    private Index getIndexForIndexMetaData(IndexMetaData imd)
    {
        // Verify if a unique index is needed
        boolean unique = imd.isUnique();

        Index index = new Index(this, unique, imd.getExtensions());

        // Set the index name if required
        if (imd.getName() != null)
        {
            index.setName(imd.getName());
        }

        // Set the column(s) to index
        // Class-level index so use its column definition
        if (imd.getNumberOfColumns() > 0)
        {
            // a). Columns specified directly
            String[] columnNames = imd.getColumnNames();
            for (String columnName : columnNames)
            {
                DatastoreIdentifier colName = storeMgr.getIdentifierFactory().newColumnIdentifier(columnName);
                Column col = columnsByIdentifier.get(colName);
                if (col == null)
                {
                    NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058001", toString(), index.getName(), columnName));
                    break;
                }

                index.addColumn(col);
            }

            // Apply any user-provided ordering of the columns
            String idxOrdering = imd.getValueForExtension(MetaData.EXTENSION_INDEX_COLUMN_ORDERING);
            if (!StringUtils.isWhitespace(idxOrdering))
            {
                index.setColumnOrdering(idxOrdering);
            }
        }
        else if (imd.getNumberOfMembers() > 0)
        {
            // b). Columns specified using members
            String[] memberNames = imd.getMemberNames();
            for (int i=0;i<memberNames.length;i++)
            {
                // Find the metadata for the actual field with the same name as this "index" field
                AbstractMemberMetaData realMmd = getMetaDataForMember(memberNames[i]);
                if (realMmd == null)
                {
                    throw new NucleusUserException("Table " + this + " has index specified on member " + memberNames[i] + 
                        " but that member does not exist in the class that this table represents");
                }

                JavaTypeMapping fieldMapping = memberMappingsMap.get(realMmd);
                int countFields = fieldMapping.getNumberOfColumnMappings();
                for (int j=0; j<countFields; j++)
                {
                    index.addColumn(fieldMapping.getColumnMapping(j).getColumn());
                }

                // Apply any user-provided ordering of the columns
                String idxOrdering = imd.getValueForExtension(MetaData.EXTENSION_INDEX_COLUMN_ORDERING);
                if (!StringUtils.isWhitespace(idxOrdering))
                {
                    index.setColumnOrdering(idxOrdering);
                }
            }
        }
        else
        {
            // We can't have an index of no columns
            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058002", toString(), index.getName()));
            return null;
        }

        return index;
    }

    /**
     * Accessor for the expected foreign keys for this table.
     * @param clr The ClassLoaderResolver
     * @return The expected foreign keys.
     */
    public List<ForeignKey> getExpectedForeignKeys(ClassLoaderResolver clr)
    {
        assertIsInitialized();

        // Auto mode allows us to decide which FKs are needed as well as using what is in the users MetaData.
        boolean autoMode = false;
        if (storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE).equals("DataNucleus"))
        {
            autoMode = true;
        }

        ArrayList<ForeignKey> foreignKeys = new ArrayList<>();

        // Check each field for FK requirements (user-defined, or required)
        // <field><foreign-key>...</foreign-key></field>
        Set memberNumbersSet = memberMappingsMap.keySet();
        Iterator iter = memberNumbersSet.iterator();
        while (iter.hasNext())
        {
            AbstractMemberMetaData mmd = (AbstractMemberMetaData) iter.next();
            JavaTypeMapping memberMapping = memberMappingsMap.get(mmd);

            if (memberMapping instanceof EmbeddedPCMapping)
            {
                EmbeddedPCMapping embMapping = (EmbeddedPCMapping)memberMapping;
                addExpectedForeignKeysForEmbeddedPCField(foreignKeys, autoMode, clr, embMapping);
            }
            else
            {
                if (ClassUtils.isReferenceType(mmd.getType()) && memberMapping instanceof ReferenceMapping)
                {
                    // Field is a reference type, so add a FK to the table of the PC for each PC implementation
                    Collection fks = TableUtils.getForeignKeysForReferenceField(memberMapping, mmd, autoMode, storeMgr, clr);
                    foreignKeys.addAll(fks);
                }
                else if (storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr) != null &&
                        memberMapping.getNumberOfColumnMappings() > 0 && memberMapping instanceof PersistableMapping)
                {
                    // Field is for a PC class with the FK at this side, so add a FK to the table of this PC
                    ForeignKey fk = TableUtils.getForeignKeyForPCField(memberMapping, mmd, autoMode, storeMgr, clr);
                    if (fk != null)
                    {
                        // Check for dups (can happen if we override a persistent property for 1-1/N-1 in a subclass)
                        boolean exists = false;
                        for (ForeignKey theFK : foreignKeys)
                        {
                            if (theFK.isEqual(fk))
                            {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists)
                        {
                            foreignKeys.add(fk);
                        }
                    }
                }
            }
        }

        // FK from id column(s) to id column(s) of superclass, as specified by
        // <inheritance><join><foreign-key ...></join></inheritance>
        ForeignKeyMetaData idFkmd = (cmd.getInheritanceMetaData().getJoinMetaData() != null) ? 
                cmd.getInheritanceMetaData().getJoinMetaData().getForeignKeyMetaData() : null;
        if (supertable != null && (autoMode || (idFkmd != null && idFkmd.getDeleteAction() != ForeignKeyAction.NONE)))
        {
            ForeignKey fk = new ForeignKey(getIdMapping(), dba, supertable, false);
            if (idFkmd != null && idFkmd.getName() != null)
            {
                fk.setName(idFkmd.getName());
            }
            foreignKeys.add(0, fk);
        }

        // Add any user-required FKs for the class as a whole
        // <class><foreign-key>...</foreign-key></field>
        Iterator<AbstractClassMetaData> cmdIter = managedClassMetaData.iterator();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData thisCmd = cmdIter.next();
            List<ForeignKeyMetaData> fkmds = thisCmd.getForeignKeyMetaData();
            if (fkmds != null)
            {
                for (ForeignKeyMetaData fkmd : fkmds)
                {
                    ForeignKey fk = getForeignKeyForForeignKeyMetaData(fkmd);
                    if (fk != null)
                    {
                        foreignKeys.add(fk);
                    }
                }
            }
        }

        Map externalFks = getExternalFkMappings();
        if (!externalFks.isEmpty())
        {
            // 1-N FK relationships - FK to id column(s) of owner table where this is the element table and we have a FK
            Collection externalFkKeys = externalFks.entrySet();
            Iterator<Map.Entry<AbstractMemberMetaData, JavaTypeMapping>> externalFkKeysIter = externalFkKeys.iterator();
            while (externalFkKeysIter.hasNext())
            {
                Map.Entry<AbstractMemberMetaData, JavaTypeMapping> entry = externalFkKeysIter.next();
                AbstractMemberMetaData fmd = entry.getKey();
                DatastoreClass referencedTable = storeMgr.getDatastoreClass(fmd.getAbstractClassMetaData().getFullClassName(), clr);
                if (referencedTable != null)
                {
                    // Take <foreign-key> from either <field> or <element>
                    ForeignKeyMetaData fkmd = fmd.getForeignKeyMetaData();
                    if (fkmd == null && fmd.getElementMetaData() != null)
                    {
                        fkmd = fmd.getElementMetaData().getForeignKeyMetaData();
                    }
                    if ((fkmd != null && fkmd.getDeleteAction() != ForeignKeyAction.NONE) || autoMode)
                    {
                        // Either has been specified by user, or using autoMode, so add FK
                        JavaTypeMapping fkMapping = entry.getValue();
                        ForeignKey fk = new ForeignKey(fkMapping, dba, referencedTable, true);
                        fk.setForMetaData(fkmd); // Does nothing when no FK MetaData
                        if (!foreignKeys.contains(fk))
                        {
                            // Only add when not already present (in the case of shared FKs there can be dups here)
                            foreignKeys.add(fk);
                        }
                    }
                }
            }
        }

        return foreignKeys;
    }

    /**
     * Convenience method to add the expected FKs for an embedded PC field.
     * @param foreignKeys The list of FKs to add the FKs to
     * @param autoMode Whether operating in "auto-mode" where DataNucleus can create its own FKs
     * @param clr ClassLoader resolver
     * @param embeddedMapping The embedded PC mapping
     */
    private void addExpectedForeignKeysForEmbeddedPCField(List foreignKeys, boolean autoMode, ClassLoaderResolver clr, EmbeddedPCMapping embeddedMapping)
    {
        for (int i=0;i<embeddedMapping.getNumberOfJavaTypeMappings();i++)
        {
            JavaTypeMapping embFieldMapping = embeddedMapping.getJavaTypeMapping(i);
            if (embFieldMapping instanceof EmbeddedPCMapping)
            {
                // Nested embedded PC so add the FKs for that
                addExpectedForeignKeysForEmbeddedPCField(foreignKeys, autoMode, clr, (EmbeddedPCMapping)embFieldMapping);
            }
            else
            {
                AbstractMemberMetaData embFmd = embFieldMapping.getMemberMetaData();
                if (ClassUtils.isReferenceType(embFmd.getType()) && embFieldMapping instanceof ReferenceMapping)
                {
                    // Field is a reference type, so add a FK to the table of the PC for each PC implementation
                    Collection fks = TableUtils.getForeignKeysForReferenceField(embFieldMapping, embFmd, autoMode, storeMgr, clr);
                    foreignKeys.addAll(fks);
                }
                else if (storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(embFmd.getType(), clr) != null &&
                        embFieldMapping.getNumberOfColumnMappings() > 0 && embFieldMapping instanceof PersistableMapping)
                {
                    // Field is for a PC class with the FK at this side, so add a FK to the table of this PC
                    ForeignKey fk = TableUtils.getForeignKeyForPCField(embFieldMapping, embFmd, autoMode, storeMgr, clr);
                    if (fk != null)
                    {
                        foreignKeys.add(fk);
                    }
                }
            }
        }
    }

    /**
     * Convenience method to create a FK for the specified ForeignKeyMetaData.
     * Used for foreign-keys specified at &lt;class&gt; level.
     * @param fkmd ForeignKey MetaData
     * @return The ForeignKey
     */
    private ForeignKey getForeignKeyForForeignKeyMetaData(ForeignKeyMetaData fkmd)
    {
        if (fkmd == null)
        {
            return null;
        }

        // Create the ForeignKey base details
        ForeignKey fk = new ForeignKey(dba, fkmd.isDeferred());
        fk.setForMetaData(fkmd);

        if (fkmd.getFkDefinitionApplies())
        {
            // User-defined FK definition should be used
            return fk;
        }

        // Find the target of the foreign-key
        AbstractClassMetaData acmd = cmd;
        if (fkmd.getTable() == null)
        {
            // Can't create a FK if we don't know where it goes to
            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058105", acmd.getFullClassName()));
            return null;
        }
        DatastoreIdentifier tableId = storeMgr.getIdentifierFactory().newTableIdentifier(fkmd.getTable());
        ClassTable refTable = (ClassTable)storeMgr.getDatastoreClass(tableId);
        if (refTable == null)
        {
            // TODO Go to the datastore and query for this table to get the columns of the PK
            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058106", acmd.getFullClassName(), fkmd.getTable()));
            return null;
        }
        PrimaryKey pk = refTable.getPrimaryKey();
        List targetCols = pk.getColumns();

        // Generate the columns for the source of the foreign-key
        List<Column> sourceCols = new ArrayList<>();
        ColumnMetaData[] colmds = fkmd.getColumnMetaData();
        String[] memberNames = fkmd.getMemberNames();
        if (colmds != null && colmds.length > 0)
        {
            // FK specified via <column>
            for (int i=0;i<colmds.length;i++)
            {
                // Find the column and add to the source columns for the FK
                DatastoreIdentifier colId = storeMgr.getIdentifierFactory().newColumnIdentifier(colmds[i].getName());
                Column sourceCol = columnsByIdentifier.get(colId);
                if (sourceCol == null)
                {
                    NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058107", acmd.getFullClassName(), fkmd.getTable(), colmds[i].getName(), toString()));
                    return null;
                }
                sourceCols.add(sourceCol);
            }
        }
        else if (memberNames != null && memberNames.length > 0)
        {
            // FK specified via <field>
            for (int i=0;i<memberNames.length;i++)
            {
                // Find the metadata for the actual field with the same name as this "foreign-key" field
                // and add all columns to the source columns for the FK
                AbstractMemberMetaData realMmd = getMetaDataForMember(memberNames[i]);
                if (realMmd == null)
                {
                    throw new NucleusUserException("Table " + this + " has foreign-key specified on member " + memberNames[i] + 
                        " but that member does not exist in the class that this table represents");
                }

                JavaTypeMapping fieldMapping = memberMappingsMap.get(realMmd);
                int countCols = fieldMapping.getNumberOfColumnMappings();
                for (int j=0; j<countCols; j++)
                {
                    // Add each column of this field to the FK definition
                    sourceCols.add(fieldMapping.getColumnMapping(j).getColumn());
                }
            }
        }

        if (sourceCols.size() != targetCols.size())
        {
            // Different number of cols in this table and target table
            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058108",
                acmd.getFullClassName(), fkmd.getTable(), "" + sourceCols.size(), "" + targetCols.size()));
        }

        // Add all column mappings to the ForeignKey
        if (sourceCols.size() > 0)
        {
            for (int i=0;i<sourceCols.size();i++)
            {
                Column source = sourceCols.get(i);
                String targetColName = (colmds != null && colmds[i] != null) ? colmds[i].getTarget() : null;
                Column target = (Column)targetCols.get(i); // Default to matching via the natural order
                if (targetColName != null)
                {
                    // User has specified the target column for this col so try it in our target list
                    for (int j=0;j<targetCols.size();j++)
                    {
                        Column targetCol = (Column)targetCols.get(j);
                        if (targetCol.getIdentifier().getName().equalsIgnoreCase(targetColName))
                        {
                            // Found the required column
                            target = targetCol;
                            break;
                        }
                    }
                }
                fk.addColumn(source, target);
            }
        }

        return fk;
    }

    /**
     * Accessor for the expected candidate keys for this table.
     * @return The expected candidate keys.
     */
    protected List<CandidateKey> getExpectedCandidateKeys()
    {
        assertIsInitialized();

        // The candidate keys required by the basic table
        List<CandidateKey> candidateKeys = super.getExpectedCandidateKeys();

        // Add any constraints required for a FK map
        Iterator<CandidateKey> cks = candidateKeysByMapField.values().iterator();
        while (cks.hasNext())
        {
            CandidateKey ck = cks.next();
            candidateKeys.add(ck);
        }

        // Add on any user-required candidate keys for the fields
        Set fieldNumbersSet = memberMappingsMap.keySet();
        Iterator iter = fieldNumbersSet.iterator();
        while (iter.hasNext())
        {
            AbstractMemberMetaData fmd=(AbstractMemberMetaData) iter.next();
            JavaTypeMapping fieldMapping = memberMappingsMap.get(fmd);
            if (fieldMapping instanceof EmbeddedPCMapping)
            {
                // Add indexes for fields of this embedded PC object
                EmbeddedPCMapping embMapping = (EmbeddedPCMapping)fieldMapping;
                for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
                {
                    JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                    UniqueMetaData umd = embFieldMapping.getMemberMetaData().getUniqueMetaData();
                    if (umd != null)
                    {
                        CandidateKey ck = TableUtils.getCandidateKeyForField(this, umd, embFieldMapping);
                        if (ck != null)
                        {
                            candidateKeys.add(ck);
                        }
                    }
                }
            }
            else
            {
                // Add any required candidate key for this field
                UniqueMetaData umd = fmd.getUniqueMetaData();
                if (umd != null)
                {
                    CandidateKey ck = TableUtils.getCandidateKeyForField(this, umd, fieldMapping);
                    if (ck != null)
                    {
                        candidateKeys.add(ck);
                    }
                }
            }
        }

        // Add on any user-required candidate keys for the class(es) as a whole (composite keys)
        Iterator<AbstractClassMetaData> cmdIter = managedClassMetaData.iterator();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData thisCmd = cmdIter.next();
            List<UniqueMetaData> classCKs = thisCmd.getUniqueMetaData();
            if (classCKs != null)
            {
                for (UniqueMetaData unimd : classCKs)
                {
                    CandidateKey ck = getCandidateKeyForUniqueMetaData(unimd);
                    if (ck != null)
                    {
                        candidateKeys.add(ck);
                    }
                }
            }
        }

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Make sure there is no reuse of PK fields that cause a duplicate index for the PK. Remove it if required
            PrimaryKey pk = getPrimaryKey();
            Iterator<CandidateKey> candidatesIter = candidateKeys.iterator();
            while (candidatesIter.hasNext())
            {
                CandidateKey key = candidatesIter.next();
                if (key.getColumnList().equals(pk.getColumnList()))
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug("Candidate key " + key + " is for the same columns as the PrimaryKey so being removed from expected set of candidates. PK is always unique");
                    candidatesIter.remove();
                }
            }
        }

        return candidateKeys;
    }

    /**
     * Convenience method to convert a UniqueMetaData into a CandidateKey.
     * @param umd The Unique MetaData
     * @return The Candidate Key
     */
    private CandidateKey getCandidateKeyForUniqueMetaData(UniqueMetaData umd)
    {
        CandidateKey ck = new CandidateKey(this, umd.getExtensions());

        // Set the key name if required
        if (umd.getName() != null)
        {
            ck.setName(umd.getName());
        }

        // Class-level index so use its column definition
        // a). Columns specified directly
        if (umd.getNumberOfColumns() > 0)
        {
            String[] columnNames = umd.getColumnNames();
            for (String columnName : columnNames)
            {
                DatastoreIdentifier colName = storeMgr.getIdentifierFactory().newColumnIdentifier(columnName);
                Column col = columnsByIdentifier.get(colName);
                if (col == null)
                {
                    NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058202", toString(), ck.getName(), columnName));
                    break;
                }

                ck.addColumn(col);
            }
        }
        // b). Columns specified using fields
        else if (umd.getNumberOfMembers() > 0)
        {
            String[] memberNames = umd.getMemberNames();
            for (String memberName : memberNames)
            {
                // Find the metadata for the actual field with the same name as this "unique" field
                AbstractMemberMetaData realMmd = getMetaDataForMember(memberName);
                if (realMmd == null)
                {
                    throw new NucleusUserException("Table " + this + " has unique key specified on member " + memberName + 
                        " but that member does not exist in the class that this table represents");
                }

                JavaTypeMapping memberMapping = memberMappingsMap.get(realMmd);
                int countFields = memberMapping.getNumberOfColumnMappings();
                for (int j=0; j<countFields; j++)
                {
                    ck.addColumn(memberMapping.getColumnMapping(j).getColumn());
                }
            }
        }
        else
        {
            // We can't have an index of no columns
            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058203", toString(), ck.getName()));
            return null;
        }

        return ck;
    }

    /**
     * Accessor for the primary key for this table. Overrides the method in TableImpl
     * to add on any specification of PK name in the metadata.
     * @return The primary key.
     */
    public PrimaryKey getPrimaryKey()
    {
        PrimaryKey pk = super.getPrimaryKey();
        PrimaryKeyMetaData pkmd = cmd.getPrimaryKeyMetaData();
        if (pkmd != null && pkmd.getName() != null)
        {
            pk.setName(pkmd.getName());
        }

        return pk;
    }

    /**
     * Accessor for the CREATE statements for this table.
     * Creates this table followed by all secondary tables (if any).
     * @param props Properties for creating the table
     * @return the SQL statements to be executed for creation
     */
    protected List<String> getSQLCreateStatements(Properties props)
    {
        List<String> stmts;

        // Create statements for this table
        Properties tableProps = null;
        if (createStatementDDL != null)
        {
            // User has specified the DDL
            stmts = new ArrayList<>();
            StringTokenizer tokens = new StringTokenizer(createStatementDDL, ";");
            while (tokens.hasMoreTokens())
            {
                stmts.add(tokens.nextToken());
            }
        }
        else
        {
            // Create the DDL
            Map<String, String> emds = cmd.getExtensions();
            if (emds != null)
            {
                tableProps = new Properties();
                if (emds.size() > 0)
                {
                    tableProps.putAll(emds);
                }
            }
            stmts = super.getSQLCreateStatements(tableProps);
        }

        // Create statements for any secondary tables
        // Since the secondary tables are managed by us, we need to create their table
        if (secondaryTables != null)
        {
            Set secondaryTableNames = secondaryTables.keySet();
            Iterator iter = secondaryTableNames.iterator();
            while (iter.hasNext())
            {
                SecondaryTable secTable = secondaryTables.get(iter.next());
                stmts.addAll(secTable.getSQLCreateStatements(tableProps)); // Use same tableProps as the primary table
            }
        }

        return stmts;
    }

    /**
     * Accessor for the DROP statements for this table.
     * Drops all secondary tables (if any) followed by the table itself.
     * @return List of statements
     */
    protected List<String> getSQLDropStatements()
    {
        assertIsInitialized();

        List<String> stmts = new ArrayList<>();

        // Drop any secondary tables
        if (secondaryTables != null)
        {
            Set secondaryTableNames = secondaryTables.keySet();
            Iterator iter = secondaryTableNames.iterator();
            while (iter.hasNext())
            {
                SecondaryTable secTable = secondaryTables.get(iter.next());
                stmts.addAll(secTable.getSQLDropStatements());
            }
        }

        // Drop this table
        stmts.add(dba.getDropTableStatement(this));

        return stmts;
    }

    /**
     * Method to initialise unique constraints for 1-N Map using FK.
     * @param ownerMmd metadata for the field/property with the map in the owner class
     */
    private void initializeFKMapUniqueConstraints(AbstractMemberMetaData ownerMmd)
    {
        // Load "mapped-by"
        AbstractMemberMetaData mfmd = null;
        String map_field_name = ownerMmd.getMappedBy(); // Field in our class that maps back to the owner class
        if (map_field_name != null)
        {
            // Bidirectional
            mfmd = cmd.getMetaDataForMember(map_field_name);
            if (mfmd == null)
            {
                // Field not in primary class so may be in subclass so check all managed classes
                Iterator<AbstractClassMetaData> cmdIter = managedClassMetaData.iterator();
                while (cmdIter.hasNext())
                {
                    AbstractClassMetaData managedCmd = cmdIter.next();
                    mfmd = managedCmd.getMetaDataForMember(map_field_name);
                    if (mfmd != null)
                    {
                        break;
                    }
                }
            }
            if (mfmd == null)
            {
                // "mapped-by" refers to a field in our class that doesnt exist!
                throw new NucleusUserException(Localiser.msg("057036", map_field_name, cmd.getFullClassName(), ownerMmd.getFullFieldName()));                       
            }

            if (ownerMmd.getJoinMetaData() == null)
            {
                // Load field of key in value
                if (ownerMmd.getKeyMetaData() != null && ownerMmd.getKeyMetaData().getMappedBy() != null)
                {
                    // Key field is stored in the value table
                    AbstractMemberMetaData kmd = null;
                    String key_field_name = ownerMmd.getKeyMetaData().getMappedBy();
                    if (key_field_name != null)
                    {
                        kmd = cmd.getMetaDataForMember(key_field_name);
                    }
                    if (kmd == null)
                    {
                        // Field not in primary class so may be in subclass so check all managed classes
                        Iterator<AbstractClassMetaData> cmdIter = managedClassMetaData.iterator();
                        while (cmdIter.hasNext())
                        {
                            AbstractClassMetaData managedCmd = cmdIter.next();
                            kmd = managedCmd.getMetaDataForMember(key_field_name);
                            if (kmd != null)
                            {
                                break;
                            }
                        }
                    }
                    if (kmd == null)
                    {
                        throw new ClassDefinitionException(Localiser.msg("057007", mfmd.getFullFieldName(), key_field_name));
                    }

                    JavaTypeMapping ownerMapping = getMemberMapping(map_field_name);
                    JavaTypeMapping keyMapping = getMemberMapping(kmd.getName());

                    if (dba.supportsOption(DatastoreAdapter.NULLS_IN_CANDIDATE_KEYS) || (!ownerMapping.isNullable() && !keyMapping.isNullable()))
                    {
                        // If the owner and key fields are represented in this table then we can impose
                        // a unique constraint on them. If the key field is in a superclass then we
                        // cannot do this so just omit it.
                        if (keyMapping.getTable() == this && ownerMapping.getTable() == this)
                        {
                            CandidateKey ck = new CandidateKey(this, null);

                            // This HashSet is to avoid duplicate adding of columns.
                            HashSet addedColumns = new HashSet();

                            // Add columns for the owner field
                            int countOwnerFields = ownerMapping.getNumberOfColumnMappings();
                            for (int i = 0; i < countOwnerFields; i++)
                            {
                                Column col = ownerMapping.getColumnMapping(i).getColumn();
                                addedColumns.add(col);
                                ck.addColumn(col);
                            }

                            // Add columns for the key field
                            int countKeyFields = keyMapping.getNumberOfColumnMappings();
                            for (int i = 0; i < countKeyFields; i++)
                            {
                                Column col = keyMapping.getColumnMapping(i).getColumn();
                                if (!addedColumns.contains(col))
                                {
                                    addedColumns.add(col);
                                    ck.addColumn(col);
                                }
                                else
                                {
                                    NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("057041", ownerMmd.getName()));
                                }
                            }

                            if (candidateKeysByMapField.put(mfmd, ck) != null)
                            {
                                // We have multiple "mapped-by" coming to this field so give a warning that this may potentially
                                // cause problems. For example if they have the key field defined here for 2 different relations
                                // so you may get keys/values appearing in the other relation that shouldn't be.
                                // Logged as a WARNING for now. 
                                // If there is a situation where this should throw an exception, please update this AND COMMENT WHY.
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("057012", mfmd.getFullFieldName(), ownerMmd.getFullFieldName()));
                            }
                        }
                    }
                }
                else if (ownerMmd.getValueMetaData() != null && ownerMmd.getValueMetaData().getMappedBy() != null)
                {
                    // Value field is stored in the key table
                    AbstractMemberMetaData vmd = null;
                    String value_field_name = ownerMmd.getValueMetaData().getMappedBy();
                    if (value_field_name != null)
                    {
                        vmd = cmd.getMetaDataForMember(value_field_name);
                    }
                    if (vmd == null)
                    {
                        throw new ClassDefinitionException(Localiser.msg("057008", mfmd));
                    }

                    JavaTypeMapping ownerMapping = getMemberMapping(map_field_name);
                    JavaTypeMapping valueMapping = getMemberMapping(vmd.getName());

                    if (dba.supportsOption(DatastoreAdapter.NULLS_IN_CANDIDATE_KEYS) || (!ownerMapping.isNullable() && !valueMapping.isNullable()))
                    {
                        // If the owner and value fields are represented in this table then we can impose
                        // a unique constraint on them. If the value field is in a superclass then we
                        // cannot do this so just omit it.
                        if (valueMapping.getTable() == this && ownerMapping.getTable() == this)
                        {
                            CandidateKey ck = new CandidateKey(this, null);

                            // This HashSet is to avoid duplicate adding of columns.
                            Set<Column> addedColumns = new HashSet<>();

                            // Add columns for the owner field
                            int countOwnerFields = ownerMapping.getNumberOfColumnMappings();
                            for (int i = 0; i < countOwnerFields; i++)
                            {
                                Column col = ownerMapping.getColumnMapping(i).getColumn();
                                addedColumns.add(col);
                                ck.addColumn(col);
                            }

                            // Add columns for the value field
                            int countValueFields = valueMapping.getNumberOfColumnMappings();
                            for (int i = 0; i < countValueFields; i++)
                            {
                                Column col = valueMapping.getColumnMapping(i).getColumn();
                                if (!addedColumns.contains(col))
                                {
                                    addedColumns.add(col);
                                    ck.addColumn(col);
                                }
                                else
                                {
                                    NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("057042", ownerMmd.getName()));
                                }
                            }

                            if (candidateKeysByMapField.put(mfmd, ck) != null)
                            {
                                // We have multiple "mapped-by" coming to this field so give a warning that this may potentially
                                // cause problems. For example if they have the key field defined here for 2 different relations
                                // so you may get keys/values appearing in the other relation that shouldn't be.
                                // Logged as a WARNING for now. 
                                // If there is a situation where this should throw an exception, please update this AND COMMENT WHY.
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("057012", mfmd.getFullFieldName(), ownerMmd.getFullFieldName()));
                            }
                        }
                    }
                }
                else
                {
                    // We can only have either the key stored in the value or the value stored in the key but nothing else!
                    throw new ClassDefinitionException(Localiser.msg("057009", ownerMmd.getFullFieldName()));
                }
            }
        }
    }

    /**
     * Initialize the ID Mapping.
     * For datastore identity this will be a PCMapping that contains the OIDMapping.
     * For application identity this will be a PCMapping that contains the PK mapping(s).
     */
    private void initializeIDMapping()
    {
        if (idMapping != null)
        {
            return;
        }

        final PersistableMapping mapping = new PersistableMapping();
        mapping.setTable(this);
        mapping.initialize(getStoreManager(), cmd.getFullClassName());

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
        else
        {
            // Nothing to do for nondurable since no identity
        }

        idMapping = mapping;        
    }
    
    /**
     * Accessor for a mapping for the ID (persistable) for this table.
     * @return The (persistable) ID mapping.
     */
    public JavaTypeMapping getIdMapping()
    {
        return idMapping;
    }

    /**
     * Accessor for all of the order mappings (used by FK Lists, Collections, Arrays)
     * @return The mappings for the order columns.
     **/
    private Map<AbstractMemberMetaData, JavaTypeMapping> getExternalOrderMappings()
    {
        if (externalOrderMappings == null)
        {       
            externalOrderMappings = new HashMap();
        }
        return externalOrderMappings;
    }

    public boolean hasExternalFkMappings()
    {
        return externalFkMappings != null && externalFkMappings.size() > 0;
    }

    /**
     * Accessor for all of the external FK mappings.
     * @return The mappings for external FKs
     **/
    private Map<AbstractMemberMetaData, JavaTypeMapping> getExternalFkMappings()
    {
        if (externalFkMappings == null)
        {       
            externalFkMappings = new HashMap();
        }
        return externalFkMappings;
    }

    /**
     * Accessor for an external mapping for the specified field of the required type.
     * @param mmd MetaData for the field/property
     * @param mappingType Type of mapping
     * @return The (external) mapping
     */
    public JavaTypeMapping getExternalMapping(AbstractMemberMetaData mmd, MappingType mappingType)
    {
        if (mappingType == MappingType.EXTERNAL_FK)
        {
            return getExternalFkMappings().get(mmd);
        }
        else if (mappingType == MappingType.EXTERNAL_FK_DISCRIMINATOR)
        {
            return getExternalFkDiscriminatorMappings().get(mmd);
        }
        else if (mappingType == MappingType.EXTERNAL_INDEX)
        {
            return getExternalOrderMappings().get(mmd);
        }
        else
        {
            return null;
        }
    }

    /**
     * Accessor for the MetaData for the (owner) field that an external mapping corresponds to.
     * @param mapping The mapping
     * @param mappingType The mapping type
     * @return metadata for the external mapping
     */
    public AbstractMemberMetaData getMetaDataForExternalMapping(JavaTypeMapping mapping, MappingType mappingType)
    {
        if (mappingType == MappingType.EXTERNAL_FK)
        {
            Set entries = getExternalFkMappings().entrySet();
            Iterator iter = entries.iterator();
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry)iter.next();
                if (entry.getValue() == mapping)
                {
                    return (AbstractMemberMetaData)entry.getKey();
                }
            }
        }
        else if (mappingType == MappingType.EXTERNAL_FK_DISCRIMINATOR)
        {
            Set entries = getExternalFkDiscriminatorMappings().entrySet();
            Iterator iter = entries.iterator();
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry)iter.next();
                if (entry.getValue() == mapping)
                {
                    return (AbstractMemberMetaData)entry.getKey();
                }
            }
        }
        else if (mappingType == MappingType.EXTERNAL_INDEX)
        {
            Set entries = getExternalOrderMappings().entrySet();
            Iterator iter = entries.iterator();
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry)iter.next();
                if (entry.getValue() == mapping)
                {
                    return (AbstractMemberMetaData)entry.getKey();
                }
            }
        }
        return null;
    }

    /**
     * Accessor for all of the external FK discriminator mappings.
     * @return The mappings for external FKs
     **/
    private Map<AbstractMemberMetaData, JavaTypeMapping> getExternalFkDiscriminatorMappings()
    {
        if (externalFkDiscriminatorMappings == null)
        {
            externalFkDiscriminatorMappings = new HashMap();
        }
        return externalFkDiscriminatorMappings;
    }

    /**
     * Accessor for the field mapping for the specified field.
     * The field can be managed by a supertable of this table.
     * @param mmd MetaData for this field/property
     * @return the Mapping for the field/property
     */
    public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd)
    {
        if (mmd == null)
        {
            return null;
        }

        if (mmd instanceof PropertyMetaData && mmd.getAbstractClassMetaData() instanceof InterfaceMetaData)
        {
            // "Persistent Interfaces" don't do lookups correctly in here so just use the field name for now
            // TODO Fix Persistent Interfaces lookup
            return getMemberMapping(mmd.getName());
        }

        if (mmd.isPrimaryKey())
        {
            assertIsPKInitialized();
        }
        else
        {
            assertIsInitialized();
        }

        // Check if we manage this field
        JavaTypeMapping m = memberMappingsMap.get(mmd);
        if (m != null)
        {
            return m;
        }
        if (mmd.isPrimaryKey() && pkMappings != null)
        {
            // pkMappings aren't in memberMappingsMap when in subclasses
            for (int i=0;i<pkMappings.length;i++)
            {
                if (pkMappings[i].getMemberMetaData().equals(mmd))
                {
                    return pkMappings[i];
                }
            }
        }

        Iterator<Map.Entry<AbstractMemberMetaData, JavaTypeMapping>> memberMapIter = memberMappingsMap.entrySet().iterator();
        while (memberMapIter.hasNext())
        {
            // If we have overridden this member then it may have a different AbstractMemberMetaData passed in (i.e the override)
            Map.Entry<AbstractMemberMetaData, JavaTypeMapping> entry = memberMapIter.next();
            if (entry.getKey().getFullFieldName().equals(mmd.getFullFieldName()))
            {
                return entry.getValue();
            }
        }

        // Check supertable
        int ifc = cmd.getNoOfInheritedManagedMembers();
        if (mmd.getAbsoluteFieldNumber() < ifc)
        {
            if (supertable != null)
            {
                m = supertable.getMemberMapping(mmd);
                if (m != null)
                {
                    return m;
                }
            }
        }

        // Check secondary tables
        if (secondaryTables != null)
        {
            Collection secTables = secondaryTables.values();
            Iterator iter = secTables.iterator();
            while (iter.hasNext())
            {
                SecondaryTable secTable = (SecondaryTable)iter.next();
                m = secTable.getMemberMapping(mmd);
                if (m != null)
                {
                    return m;
                }
            }
        }

        return null;
    }

    /**
     * Accessor for the mapping for the specified field only in this datastore class.
     * @param mmd Metadata of the field/property
     * @return The Mapping for the field/property (or null if not present here)
     */
    public JavaTypeMapping getMemberMappingInDatastoreClass(AbstractMemberMetaData mmd)
    {
        if (mmd == null)
        {
            return null;
        }

        if (mmd instanceof PropertyMetaData && mmd.getAbstractClassMetaData() instanceof InterfaceMetaData)
        {
            // "Persistent Interfaces" don't do lookups correctly in here so just use the field name for now
            // TODO Fix Persistent Interfaces lookup
            return getMemberMapping(mmd.getName());
        }

        if (mmd.isPrimaryKey())
        {
            assertIsPKInitialized();
        }
        else
        {
            assertIsInitialized();
        }

        // Check if we manage this field
        JavaTypeMapping m = memberMappingsMap.get(mmd);
        if (m != null)
        {
            return m;
        }

        // Check if it is a PK field
        if (pkMappings != null)
        {
            for (int i=0;i<pkMappings.length;i++)
            {
                JavaTypeMapping pkMapping = pkMappings[i];
                if (pkMapping.getMemberMetaData() == mmd)
                {
                    return pkMapping;
                }
            }
        }

        return null;
    }

    /**
     * Accessor for the field mapping for the named field.
     * The field may exist in a parent table or a secondary table.
     * Throws a NoSuchPersistentFieldException if the field name is not found.
     * TODO Use of this is discouraged since the fieldName is not fully qualified
     * and if a superclass-table inheritance is used we could have 2 fields of that name here.
     * @param memberName Name of field/property
     * @return The mapping.
     * @throws NoSuchPersistentFieldException Thrown when the field/property is not found
     */
    public JavaTypeMapping getMemberMapping(String memberName)
    {
        assertIsInitialized();
        AbstractMemberMetaData mmd = getMetaDataForMember(memberName);
        JavaTypeMapping m = getMemberMapping(mmd);
        if (m == null)
        {
            throw new NoSuchPersistentFieldException(cmd.getFullClassName(), memberName);
        }
        return m;
    }

    /**
     * Acessor for the FieldMetaData for the field with the specified name.
     * Searches the MetaData of all classes managed by this table.
     * Doesn't allow for cases where the table manages subclasses with the same field name.
     * In that case you should use the equivalent method passing FieldMetaData.
     * TODO Support subclasses with fields of the same name
     * TODO Use of this is discouraged since the fieldName is not fully qualified
     * and if a superclass-table inheritance is used we could have 2 fields of that name here.
     * @param memberName the field/property name
     * @return metadata for the member
     */
    AbstractMemberMetaData getMetaDataForMember(String memberName)
    {
        // TODO Dont search "cmd" since it is included in "managedClassMetaData"
        AbstractMemberMetaData mmd = cmd.getMetaDataForMember(memberName);
        if (mmd == null)
        {
            Iterator<AbstractClassMetaData> iter = managedClassMetaData.iterator();
            while (iter.hasNext())
            {
                AbstractClassMetaData theCmd = iter.next();
                final AbstractMemberMetaData foundMmd = theCmd.getMetaDataForMember(memberName);
                if (foundMmd != null)
                {
                    if (mmd != null && (!mmd.toString().equalsIgnoreCase(foundMmd.toString()) || mmd.getType() != foundMmd.getType()))
                    {
                        final String errMsg = "Table " + getIdentifier() + " manages at least 2 subclasses that both define a field \"" + memberName + "\", " + 
                            "and the fields' metadata is different or they have different type! That means you can get e.g. wrong fetch results.";
                        NucleusLogger.DATASTORE_SCHEMA.error(errMsg);
                        throw new NucleusException(errMsg).setFatal();
                    }
                    mmd = foundMmd;
                }
            }
        }
        return mmd;
    }

    /**
     * Utility to throw an exception if the object is not persistable.
     * @param op The ObjectProvider for the object
     */ 
    void assertPCClass(ObjectProvider op)
    {
        Class c = op.getObject().getClass();

        if (!op.getExecutionContext().getClassLoaderResolver().isAssignableFrom(cmd.getFullClassName(),c))
        {
            throw new NucleusException(Localiser.msg("057013",cmd.getFullClassName(),c)).setFatal();
        }
    }

    /**
     * Adds an ordering column to the element table (this) in FK list relationships.
     * Used to store the position of the element in the List.
     * If the &lt;order&gt; provides a mapped-by, this will return the existing column mapping.
     * @param mmd The MetaData of the field/property with the list for the column to map to
     * @return The Mapping for the order column
     */
    private JavaTypeMapping addOrderColumn(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        Class indexType = Integer.class;
        JavaTypeMapping orderIndexMapping = new OrderIndexMapping();
        orderIndexMapping.initialize(storeMgr, indexType.getName());
        orderIndexMapping.setMemberMetaData(mmd);
        orderIndexMapping.setTable(this);
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        DatastoreIdentifier indexColumnName = null;
        ColumnMetaData colmd = null;

        // Allow for any user definition in OrderMetaData
        OrderMetaData omd = mmd.getOrderMetaData();
        if (omd != null)
        {
            colmd = (omd.getColumnMetaData() != null && omd.getColumnMetaData().length > 0 ? omd.getColumnMetaData()[0] : null);
            if (omd.getMappedBy() != null)
            {
                // User has defined ordering using the column(s) of an existing field.
                state = TABLE_STATE_INITIALIZED; // Not adding anything so just set table back to "initialised"
                JavaTypeMapping orderMapping = getMemberMapping(omd.getMappedBy());
                if (!(orderMapping instanceof IntegerMapping) && !(orderMapping instanceof LongMapping))
                {
                    throw new NucleusUserException(Localiser.msg("057022", mmd.getFullFieldName(), omd.getMappedBy()));
                }
                return orderMapping;
            }

            String colName = null;
            if (omd.getColumnMetaData() != null && omd.getColumnMetaData().length > 0 && omd.getColumnMetaData()[0].getName() != null)
            {
                // User-defined name so create an identifier using it
                colName = omd.getColumnMetaData()[0].getName();
                indexColumnName = idFactory.newColumnIdentifier(colName);
            }
        }
        if (indexColumnName == null)
        {
            // No name defined so generate one
            indexColumnName = idFactory.newForeignKeyFieldIdentifier(mmd, null, null, 
                storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(indexType), FieldRole.ROLE_INDEX);
        }

        Column column = addColumn(indexType.getName(), indexColumnName, orderIndexMapping, colmd);
        if (colmd == null || (colmd.getAllowsNull() == null) || (colmd.getAllowsNull() != null && colmd.isAllowsNull()))
        {
            // User either wants it nullable, or havent specified anything, so make it nullable
            column.setNullable(true);
        }

        storeMgr.getMappingManager().createColumnMapping(orderIndexMapping, column, indexType.getName());

        return orderIndexMapping;
    }

    /**
     * Provide the mappings to the consumer for all primary-key fields mapped to
     * this table.
     * @param consumer Consumer for the mappings
     */
    public void providePrimaryKeyMappings(MappingConsumer consumer)
    {
        consumer.preConsumeMapping(highestMemberNumber + 1);

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

    /**
     * Provide the mappings to the consumer for all external fields mapped to this table
     * of the specified type
     * @param consumer Consumer for the mappings
     * @param mappingType Type of external mapping
     */
    final public void provideExternalMappings(MappingConsumer consumer, MappingType mappingType)
    {
        if (mappingType == MappingType.EXTERNAL_FK && externalFkMappings != null)
        {
            consumer.preConsumeMapping(highestMemberNumber + 1);

            Iterator<AbstractMemberMetaData> iter = externalFkMappings.keySet().iterator();
            while (iter.hasNext())
            {
                AbstractMemberMetaData fmd = iter.next();
                JavaTypeMapping fieldMapping = externalFkMappings.get(fmd);
                if (fieldMapping != null)
                {
                    consumer.consumeMapping(fieldMapping, MappingType.EXTERNAL_FK);
                }
            }
        }
        else if (mappingType == MappingType.EXTERNAL_FK_DISCRIMINATOR && externalFkDiscriminatorMappings != null)
        {
            consumer.preConsumeMapping(highestMemberNumber + 1);

            Iterator<AbstractMemberMetaData> iter = externalFkDiscriminatorMappings.keySet().iterator();
            while (iter.hasNext())
            {
                AbstractMemberMetaData fmd = iter.next();
                JavaTypeMapping fieldMapping = externalFkDiscriminatorMappings.get(fmd);
                if (fieldMapping != null)
                {
                    consumer.consumeMapping(fieldMapping, MappingType.EXTERNAL_FK_DISCRIMINATOR);
                }
            }
        }
        else if (mappingType == MappingType.EXTERNAL_INDEX && externalOrderMappings != null)
        {
            consumer.preConsumeMapping(highestMemberNumber + 1);

            Iterator<AbstractMemberMetaData> iter = externalOrderMappings.keySet().iterator();
            while (iter.hasNext())
            {
                AbstractMemberMetaData fmd = iter.next();
                JavaTypeMapping fieldMapping = externalOrderMappings.get(fmd);
                if (fieldMapping != null)
                {
                    consumer.consumeMapping(fieldMapping, MappingType.EXTERNAL_INDEX);
                }
            }
        }
    }

    /**
     * Provide the mappings to the consumer for all absolute field Numbers in this table
     * that are container in the fieldNumbers parameter.
     * @param consumer Consumer for the mappings
     * @param fieldMetaData MetaData for the fields to provide mappings for
     * @param includeSecondaryTables Whether to provide fields in secondary tables
     */
    public void provideMappingsForMembers(MappingConsumer consumer, AbstractMemberMetaData[] fieldMetaData, boolean includeSecondaryTables)
    {
        super.provideMappingsForMembers(consumer, fieldMetaData, true);
        if (includeSecondaryTables && secondaryTables != null)
        {
            Collection secTables = secondaryTables.values();
            Iterator iter = secTables.iterator();
            while (iter.hasNext())
            {
                SecondaryTable secTable = (SecondaryTable)iter.next();
                secTable.provideMappingsForMembers(consumer, fieldMetaData, false);
            }
        }
    }

    /**
     * Method to provide all unmapped columns to the consumer.
     * @param consumer Consumer of information
     */
    public void provideUnmappedColumns(MappingConsumer consumer)
    {
        if (unmappedColumns != null)
        {
            Iterator<Column> iter = unmappedColumns.iterator();
            while (iter.hasNext())
            {
                consumer.consumeUnmappedColumn(iter.next());
            }
        }
    }

    /**
     * Method to validate the constraints of this table.
     * @param conn Connection to use in validation
     * @param autoCreate Whether to auto create the constraints
     * @param autoCreateErrors Whether to log a warning only on errors during "auto create"
     * @param clr The ClassLoaderResolver
     * @return Whether the DB was modified
     * @throws SQLException Thrown when an error occurs in validation
     */
    public boolean validateConstraints(Connection conn, boolean autoCreate, Collection autoCreateErrors, ClassLoaderResolver clr)
    throws SQLException
    {
        boolean modified = false;
        if (super.validateConstraints(conn, autoCreate, autoCreateErrors, clr))
        {
            modified = true;
        }

        // Validate our secondary tables since we manage them
        if (secondaryTables != null)
        {
            Collection secTables = secondaryTables.values();
            Iterator iter = secTables.iterator();
            while (iter.hasNext())
            {
                SecondaryTable secTable = (SecondaryTable)iter.next();
                if (secTable.validateConstraints(conn, autoCreate, autoCreateErrors, clr))
                {
                    modified = true;
                }
            }
        }

        return modified;
    }
}
