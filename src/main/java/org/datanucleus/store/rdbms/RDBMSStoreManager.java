/**********************************************************************
Copyright (c) 2003 Mike Martin (TJDO) and others. All rights reserved. 
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
2003 Kelly Grizzle (TJDO)
2003 Erik Bengtson  - removed exist() operation
2003 Erik Bengtson  - refactored the persistent id generator System property
2003 Andy Jefferson - added localiser
2003 Andy Jefferson - updated exception handling with SchemaTable
2003 Andy Jefferson - restructured to remove SchemaTable, and add StoreManagerHelper
2003 Andy Jefferson - updated getSubClassesForClass to recurse
2004 Erik Bengtson  - removed unused method and variables 
2004 Erik Bengtson  - fixed problem with getObjectById for App ID in getClassForOID
2004 Andy Jefferson - re-emergence of SchemaTable. Addition of addClass().
2004 Andy Jefferson - Addition of AutoStartMechanism interface
2004 Andy Jefferson - Update to use Logger
2004 Andy Jefferson - Addition of Catalog name to accompany Schema name
2004 Marco Schulze  - replaced catch(NotPersistenceCapableException ...)
                  by advance-check via TypeManager.isSupportedType(...)
2004 Andy Jefferson - split StoreData into superclass.
2004 Andy Jefferson - added support for other inheritance types
2004 Andy Jefferson - added capability to dynamically add columns
2005 Marco Schulze - prevent closing starter during recursion of ClassAdder.addClassTables(...)
    ...
**********************************************************************/
package org.datanucleus.store.rdbms;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.datanucleus.ClassConstants;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.flush.FlushOrdered;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MapMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.QueryLanguage;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.metadata.TableGeneratorMetaData;
import org.datanucleus.state.ActivityState;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.state.ReferentialStateManagerImpl;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.BackedSCOStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.NucleusConnectionImpl;
import org.datanucleus.store.NucleusSequence;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.autostart.AutoStartMechanism;
import org.datanucleus.store.connection.ConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapterFactory;
import org.datanucleus.store.rdbms.autostart.SchemaAutoStarter;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.exceptions.UnsupportedDataTypeException;
import org.datanucleus.store.rdbms.fieldmanager.ParameterSetter;
import org.datanucleus.store.rdbms.fieldmanager.ResultSetGetter;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.mapping.MappedTypeManager;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.datastore.DatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.ArrayMapping;
import org.datanucleus.store.rdbms.mapping.java.CollectionMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.MapMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.schema.JDBCTypeInfo;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.RDBMSSchemaHandler;
import org.datanucleus.store.rdbms.schema.RDBMSSchemaInfo;
import org.datanucleus.store.rdbms.schema.RDBMSTableInfo;
import org.datanucleus.store.rdbms.schema.RDBMSTypesInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.scostore.FKArrayStore;
import org.datanucleus.store.rdbms.scostore.FKListStore;
import org.datanucleus.store.rdbms.scostore.FKMapStore;
import org.datanucleus.store.rdbms.scostore.FKSetStore;
import org.datanucleus.store.rdbms.scostore.JoinArrayStore;
import org.datanucleus.store.rdbms.scostore.JoinListStore;
import org.datanucleus.store.rdbms.scostore.JoinMapStore;
import org.datanucleus.store.rdbms.scostore.JoinPersistableRelationStore;
import org.datanucleus.store.rdbms.scostore.JoinSetStore;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.ArrayTable;
import org.datanucleus.store.rdbms.table.ClassTable;
import org.datanucleus.store.rdbms.table.ClassView;
import org.datanucleus.store.rdbms.table.CollectionTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.rdbms.table.PersistableJoinTable;
import org.datanucleus.store.rdbms.table.ProbeTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.store.rdbms.table.ViewImpl;
import org.datanucleus.store.rdbms.valuegenerator.SequenceTable;
import org.datanucleus.store.rdbms.valuegenerator.TableGenerator;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.SchemaScriptAwareStoreManager;
import org.datanucleus.store.types.IncompatibleFieldTypeException;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.scostore.ArrayStore;
import org.datanucleus.store.types.scostore.CollectionStore;
import org.datanucleus.store.types.scostore.MapStore;
import org.datanucleus.store.types.scostore.PersistableRelationStore;
import org.datanucleus.store.types.scostore.Store;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator.ConnectionPreference;
import org.datanucleus.store.valuegenerator.ValueGenerationConnectionProvider;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.transaction.TransactionIsolation;
import org.datanucleus.transaction.TransactionUtils;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.MacroString;
import org.datanucleus.util.MultiMap;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * StoreManager for RDBMS datastores. 
 * Provided by the "store-manager" extension key "rdbms" and accepts datastore URLs valid for JDBC.
 * <p>
 * The RDBMS manager's responsibilities extend those for StoreManager to add :
 * <ul>
 * <li>creates and controls access to the data sources required for this datastore instance</li>
 * <li>implements insert(), fetch(), update(), delete() in the interface to the ObjectProvider.</li>
 * <li>Providing cached access to JDBC database metadata (in particular column information).</li>
 * <li>Resolving SQL identifier macros to actual SQL identifiers.</li>
 * </ul>
 * TODO Change RDBMSManager to share schema information (DatabaseMetaData) with other RDBMSManager.
 */
public class RDBMSStoreManager extends AbstractStoreManager implements BackedSCOStoreManager, SchemaAwareStoreManager, SchemaScriptAwareStoreManager
{
    static
    {
        Localiser.registerBundle("org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());
    }

    public static final String METADATA_NONDURABLE_REQUIRES_TABLE = "requires-table";

    /** Adapter for the datastore being used. */
    protected DatastoreAdapter dba;

    /** Factory for identifiers for this datastore. */
    protected IdentifierFactory identifierFactory;

    /** Catalog name for the database (if supported). */
    protected String catalogName = null;

    /** Schema name for the database (if supported). */
    protected String schemaName = null;

    /** TypeManager for mapped information. */
    protected MappedTypeManager mappedTypeMgr = null;

    /** Manager for the mapping between Java and datastore types. */
    protected MappingManager mappingManager;

    /**
     * Map of DatastoreClass keyed by ObjectProvider, for objects currently being inserted.
     * Defines to what level an object is inserted in the datastore.
     */
    protected Map<ObjectProvider, DatastoreClass> insertedDatastoreClassByObjectProvider = new ConcurrentHashMap();

    /** 
     * Lock object aimed at providing a lock on the schema definition managed here, preventing
     * reads while it is being updated etc.
     */
    protected ReadWriteLock schemaLock = new ReentrantReadWriteLock();

    /** Controller for SQL executed on this store. */
    private SQLController sqlController = null;

    /** Factory for expressions using the generic query SQL mechanism. */
    protected SQLExpressionFactory expressionFactory;

    /** Calendar for this datastore. */
    private transient Calendar dateTimezoneCalendar = null;

    /**
     * The active class adder transaction, if any. Some RDBMSManager methods are called recursively in the course
     * of adding new classes. This field allows such methods to coordinate with the active ClassAdder transaction.
     * Write access is controlled via the "lock" object.
     */
    private ClassAdder classAdder = null;

    /** Writer for use when this RDBMSManager is configured to write DDL. */
    private Writer ddlWriter = null;

    /** Flag for when generating schema as DDL and wanting complete DDL (as opposed to upgrade DDL). */
    private boolean completeDDL = false;

    /**  DDL statements already written when in DDL mode. Used to eliminate dupe statements from bidir relations. */
    private Set<String> writtenDdlStatements = null;

    /** State variable for schema generation of the callback information to be processed. TODO Move to ClassTable. */
    private MultiMap schemaCallbacks = new MultiMap();

    private Map<String, Store> backingStoreByMemberName = new ConcurrentHashMap<>();

    /**
     * Constructs a new RDBMSManager. 
     * On successful return the new RDBMSManager will have successfully connected to the database with the given
     * credentials and determined the schema name, but will not have inspected the schema contents any further. 
     * The contents (tables, views, etc.) will be subsequently created and/or validated on-demand as the application
     * accesses persistent classes.
     * @param clr the ClassLoaderResolver
     * @param ctx The corresponding Context. This factory's non-tx data source will be used to get database connections as needed to perform management functions.
     * @param props Properties for the datastore
     * @exception NucleusDataStoreException If the database could not be accessed or the name of the schema could not be determined.
     */
    public RDBMSStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super("rdbms", clr, ctx, props);

        mappedTypeMgr = new MappedTypeManager(nucleusContext);
        persistenceHandler = new RDBMSPersistenceHandler(this);
        flushProcess = new FlushOrdered(); // TODO Change this to FlushReferential when we have it complete
        schemaHandler = new RDBMSSchemaHandler(this);
        expressionFactory = new SQLExpressionFactory(this);

        // Retrieve the Database Adapter for this datastore
        try
        {
            ManagedConnection mc = getConnection(-1);
            Connection conn = (Connection)mc.getConnection();
            if (conn == null)
            {
                //somehow we haven't got an exception from the JDBC driver
                //to troubleshoot the user should telnet to ip/port of database and check if he can open a connection
                //this may be due to security / firewall things.
                throw new NucleusDataStoreException(Localiser.msg("050007"));
            }

            try
            {
                dba = DatastoreAdapterFactory.getInstance().getDatastoreAdapter(clr, conn, 
                    getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_DATASTORE_ADAPTER_CLASS_NAME), ctx.getPluginManager());
                dba.initialiseTypes(schemaHandler, mc);
                dba.removeUnsupportedMappings(schemaHandler, mc);

                // User specified default catalog/schema name - check for validity, and store
                if (hasPropertyNotNull(PropertyNames.PROPERTY_MAPPING_CATALOG))
                {
                    if (!dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS))
                    {
                        NucleusLogger.DATASTORE.warn(Localiser.msg("050002", getStringProperty(PropertyNames.PROPERTY_MAPPING_CATALOG)));
                    }
                    else
                    {
                        catalogName = getStringProperty(PropertyNames.PROPERTY_MAPPING_CATALOG);
                    }
                }
                if (hasPropertyNotNull(PropertyNames.PROPERTY_MAPPING_SCHEMA))
                {
                    if (!dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS))
                    {
                        NucleusLogger.DATASTORE.warn(Localiser.msg("050003", getStringProperty(PropertyNames.PROPERTY_MAPPING_SCHEMA)));
                    }
                    else
                    {
                        schemaName = getStringProperty(PropertyNames.PROPERTY_MAPPING_SCHEMA);
                    }
                }

                // Create an identifier factory - needs the database adapter to exist first
                initialiseIdentifierFactory(ctx);

                // Now that we have the identifier factory, make sure any user-provided names were valid!
                if (schemaName != null)
                {
                    String validSchemaName = identifierFactory.getIdentifierInAdapterCase(schemaName);
                    if (!validSchemaName.equals(schemaName))
                    {
                        NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("020192", "schema", schemaName, validSchemaName));
                        schemaName = validSchemaName;
                    }
                }
                if (catalogName != null)
                {
                    String validCatalogName = identifierFactory.getIdentifierInAdapterCase(catalogName);
                    if (!validCatalogName.equals(catalogName))
                    {
                        NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("020192", "catalog", catalogName, validCatalogName));
                        catalogName = validCatalogName;
                    }
                }

                // Create the SQL controller
                sqlController = new SQLController(dba.supportsOption(DatastoreAdapter.STATEMENT_BATCHING), 
                    getIntProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STATEMENT_BATCH_LIMIT),
                    getIntProperty(PropertyNames.PROPERTY_DATASTORE_READ_TIMEOUT),
                    getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STATEMENT_LOGGING));

                // TODO These ought to be stored with the StoreManager, not the NucleusContext
                // Initialise any properties controlling the adapter
                // Just use properties matching the pattern "datanucleus.rdbms.adapter.*"
                Map<String, Object> dbaProps = new HashMap();
                Map<String, Object> persistenceProps = ctx.getConfiguration().getPersistenceProperties();
                Iterator<Map.Entry<String, Object>> propIter = persistenceProps.entrySet().iterator();
                while (propIter.hasNext())
                {
                    Map.Entry<String, Object> entry = propIter.next();
                    String prop = entry.getKey();
                    if (prop.startsWith("datanucleus.rdbms.adapter."))
                    {
                        dbaProps.put(prop, entry.getValue());
                    }
                }
                if (dbaProps.size() > 0)
                {
                    dba.setProperties(dbaProps);
                }

                // Initialise the Schema
                initialiseSchema(conn, clr);

                // Log the configuration of the RDBMS
                logConfiguration();
            }
            catch (Exception e)
            {
                NucleusLogger.GENERAL.info("Error in initialisation of RDBMSStoreManager", e);
                throw e;
            }
            finally
            {
                mc.release();
            }
        }
        catch (NucleusException ne)
        {
            NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("050004"), ne);
            throw ne.setFatal();
        }
        catch (Exception e1)
        {
            // Unknown type of exception so wrap it in a NucleusUserException for later handling
            String msg = Localiser.msg("050004") + ' ' + Localiser.msg("050006") + ' ' + Localiser.msg("048000",e1);
            NucleusLogger.DATASTORE_SCHEMA.error(msg, e1);
            throw new NucleusUserException(msg, e1).setFatal();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#getQueryCacheKey()
     */
    public String getQueryCacheKey()
    {
        // Return "rdbms-hsqldb", etc
        return getStoreManagerKey() + "-" + getDatastoreAdapter().getVendorID();
    }

    /**
     * Method to create the IdentifierFactory to be used by this store.
     * Relies on the datastore adapter existing before creation
     * @param nucleusContext context
     */
    protected void initialiseIdentifierFactory(NucleusContext nucleusContext)
    {
        if (dba == null)
        {
            throw new NucleusException("DatastoreAdapter not yet created so cannot create IdentifierFactory!");
        }

        String idFactoryName = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_FACTORY);
        String idFactoryClassName = nucleusContext.getPluginManager().getAttributeValueForExtension("org.datanucleus.store.rdbms.identifierfactory", 
            "name", idFactoryName, "class-name");
        if (idFactoryClassName == null)
        {
            throw new NucleusUserException(Localiser.msg("039003", idFactoryName)).setFatal();
        }

        try
        {
            // Create the control properties for identifier generation
            Map props = new HashMap();
            if (catalogName != null)
            {
                props.put("DefaultCatalog", catalogName);
            }
            if (schemaName != null)
            {
                props.put("DefaultSchema", schemaName);
            }

            String val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_CASE);
            props.put("RequiredCase", val != null ? val : getDefaultIdentifierCase());

            val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_WORD_SEPARATOR);
            if (val != null)
            {
                props.put("WordSeparator", val);
            }
            val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_TABLE_PREFIX);
            if (val != null)
            {
                props.put("TablePrefix", val);
            }
            val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_TABLE_SUFFIX);
            if (val != null)
            {
                props.put("TableSuffix", val);
            }
            props.put("NamingFactory", getNamingFactory());

            // Create the IdentifierFactory
            Class[] argTypes = new Class[] {DatastoreAdapter.class, ClassConstants.CLASS_LOADER_RESOLVER, Map.class};
            Object[] args = new Object[] {dba, nucleusContext.getClassLoaderResolver(null), props};
            identifierFactory = (IdentifierFactory)nucleusContext.getPluginManager().createExecutableExtension(
                "org.datanucleus.store.rdbms.identifierfactory", "name", idFactoryName, "class-name", argTypes, args);
        }
        catch (ClassNotFoundException cnfe)
        {
            throw new NucleusUserException(Localiser.msg("039004", idFactoryName, idFactoryClassName), cnfe).setFatal();
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception creating IdentifierFactory", e);
            throw new NucleusException(Localiser.msg("039005", idFactoryClassName), e).setFatal();
        }
    }

    /**
     * Accessor for whether this value strategy is supported.
     * Overrides the setting in the superclass for identity/sequence if the adapter doesn't support them.
     * @param strategy The strategy
     * @return Whether it is supported.
     */
    public boolean supportsValueStrategy(String strategy)
    {
        // "identity" doesn't have an explicit entry in plugin since uses datastore capabilities
        if (strategy.equalsIgnoreCase("IDENTITY") || super.supportsValueStrategy(strategy))
        {
            if (strategy.equalsIgnoreCase("IDENTITY") && !dba.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS))
            {
                return false; // adapter doesn't support identity so we don't
            }
            else if (strategy.equalsIgnoreCase("SEQUENCE") && !dba.supportsOption(DatastoreAdapter.SEQUENCES))
            {
                return false; // adapter doesn't support sequences so we don't
            }
            else if (strategy.equalsIgnoreCase("uuid-string"))
            {
                return dba.supportsOption(DatastoreAdapter.VALUE_GENERATION_UUID_STRING);
            }
            return true;
        }
        return false;
    }

    /**
     * Accessor for the manager of mapped type information.
     * @return MappedTypeManager
     */
    public MappedTypeManager getMappedTypeManager()
    {
        return mappedTypeMgr;
    }

    /**
     * Accessor for the factory for creating identifiers (table/column names etc).
     * @return Identifier factory
     */
    public IdentifierFactory getIdentifierFactory()
    {
        return identifierFactory;
    }

    /**
     * Gets the DatastoreAdapter to use for this store.
     * @return Returns the DatastoreAdapter
     */
    public DatastoreAdapter getDatastoreAdapter()
    {
        return dba;
    }

    /**
     * Gets the MappingManager to use for this store.
     * @return Returns the MappingManager.
     */
    public MappingManager getMappingManager()
    {
        if (mappingManager == null)
        {
            mappingManager = dba.getMappingManager(this);
        }
        return mappingManager;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getDefaultObjectProviderClassName()
     */
    @Override
    public String getDefaultObjectProviderClassName()
    {
        return ReferentialStateManagerImpl.class.getName();
    }

    /**
     * Utility to return all StoreData for a Datastore Container identifier.
     * Returns StoreData with this table identifier and where the class is the owner of the table.
     * @param tableIdentifier Identifier for the table
     * @return The StoreData for this table (if managed).
     */
    public StoreData[] getStoreDataForDatastoreContainerObject(DatastoreIdentifier tableIdentifier)
    {
        schemaLock.readLock().lock();
        try
        {
            return storeDataMgr.getStoreDataForProperties("tableId", tableIdentifier, "table-owner", "true");
        }
        finally
        {
            schemaLock.readLock().unlock();
        }
    }

    /**
     * Returns the datastore container (table) for the specified field. 
     * Returns 'null' if the field is not (yet) known to the store manager.
     * @param mmd The metadata for the field.
     * @return The corresponding datastore container, or 'null'.
     */
    public Table getTable(AbstractMemberMetaData mmd)
    {
        schemaLock.readLock().lock();
        try
        {
            StoreData sd = storeDataMgr.get(mmd);
            if (sd != null && sd instanceof RDBMSStoreData)
            {
                return (Table) sd.getTable();
            }
            return null;
        }
        finally
        {
            schemaLock.readLock().unlock();
        }
    }

    /**
     * Returns the primary datastore table serving as backing for the given class. 
     * If the class is not yet known to the store manager, {@link #manageClasses} is called
     * to add it. Classes which have inheritance strategy of "new-table" and
     * "superclass-table" will return a table here, whereas "subclass-table" will
     * return null since it doesn't have a table as such.
     * <p>
     * @param className Name of the class whose table is be returned.
     * @param clr The ClassLoaderResolver
     * @return The corresponding class table.
     * @exception NoTableManagedException If the given class has no table managed in the database.
     */
    public DatastoreClass getDatastoreClass(String className, ClassLoaderResolver clr)
    {
        DatastoreClass ct = null;
        if (className == null)
        {
            NucleusLogger.PERSISTENCE.error(Localiser.msg("032015"));
            return null;
        }

        schemaLock.readLock().lock();
        try
        {
            StoreData sd = storeDataMgr.get(className);
            if (sd != null && sd instanceof RDBMSStoreData)
            {
                ct = (DatastoreClass)sd.getTable();
                if (ct != null)
                {
                    // Class known about
                    return ct;
                }
            }
        }
        finally
        {
            schemaLock.readLock().unlock();
        }

        // Class not known so consider adding it to our list of supported classes.
        // Currently we only consider PC classes
        boolean toBeAdded = false;
        if (clr != null)
        {
            Class cls = clr.classForName(className);
            ApiAdapter api = getApiAdapter();
            if (cls != null && !cls.isInterface() && api.isPersistable(cls))
            {
                toBeAdded = true;
            }
        }
        else
        {
            toBeAdded = true;
        }

        boolean classKnown = false;
        if (toBeAdded)
        {
            // Add the class to our supported list
            manageClasses(clr, className);

            // Retry
            schemaLock.readLock().lock();
            try
            {
                StoreData sd = storeDataMgr.get(className);
                if (sd != null && sd instanceof RDBMSStoreData)
                {
                    classKnown = true;
                    ct = (DatastoreClass)sd.getTable();
                }
            }
            finally
            {
                schemaLock.readLock().unlock();
            }
        }

        // Throw an exception if class still not known and no table
        // Note : "subclass-table" inheritance strategies will return null from this method
        if (!classKnown && ct == null)
        {
            throw new NoTableManagedException(className);
        }

        return ct;
    }

    /**
     * Returns the datastore table having the given identifier.
     * Returns 'null' if no such table is (yet) known to the store manager.
     * @param name The identifier name of the table.
     * @return The corresponding table, or 'null'
     */
    public DatastoreClass getDatastoreClass(DatastoreIdentifier name)
    {
        schemaLock.readLock().lock();
        try
        {
            Iterator iterator = storeDataMgr.getManagedStoreData().iterator();
            while (iterator.hasNext())
            {
                StoreData sd = (StoreData) iterator.next();
                if (sd instanceof RDBMSStoreData)
                {
                    RDBMSStoreData tsd = (RDBMSStoreData)sd;
                    if (tsd.hasTable() && tsd.getDatastoreIdentifier().equals(name))
                    {
                        return (DatastoreClass) tsd.getTable();
                    }
                }
            }
            return null;
        }
        finally
        {
            schemaLock.readLock().unlock();
        }
    }

    /**
     * Method to return the class(es) that has a table managing the persistence of
     * the fields of the supplied class. For the 3 inheritance strategies, the following
     * occurs :-
     * <UL>
     * <LI>new-table : will return the same ClassMetaData</LI>
     * <LI>subclass-table : will return all subclasses that have a table managing its fields</LI>
     * <LI>superclass-table : will return the next superclass that has a table</LI>
     * </UL> 
     * @param cmd The supplied class.
     * @param clr ClassLoader resolver
     * @return The ClassMetaData's managing the fields of the supplied class
     */
    public AbstractClassMetaData[] getClassesManagingTableForClass(AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        // Null input, so just return null;
        if (cmd == null)
        {
            return null;
        }

        if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE ||
            cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.NEW_TABLE)
        {
            // Class manages a table so return the classes metadata.
            return new AbstractClassMetaData[] {cmd};
        }
        else if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
        {
            // Check the subclasses that we have metadata for and make sure they are managed before proceeding
            String[] subclasses = getMetaDataManager().getSubclassesForClass(cmd.getFullClassName(), true);
            if (subclasses != null)
            {
                for (int i=0;i<subclasses.length;i++)
                {
                    if (!storeDataMgr.managesClass(subclasses[i]))
                    {
                        manageClasses(clr, subclasses[i]);
                    }
                }
            }

            // Find subclasses who manage the tables winto which our class is persisted
            HashSet managingClasses=new HashSet();
            Iterator managedClassesIter = storeDataMgr.getManagedStoreData().iterator();
            while (managedClassesIter.hasNext())
            {
                StoreData data = (StoreData)managedClassesIter.next();
                if (data.isFCO() && ((AbstractClassMetaData)data.getMetaData()).getSuperAbstractClassMetaData() != null &&
                    ((AbstractClassMetaData)data.getMetaData()).getSuperAbstractClassMetaData().getFullClassName().equals(cmd.getFullClassName()))
                {
                    AbstractClassMetaData[] superCmds = getClassesManagingTableForClass((AbstractClassMetaData)data.getMetaData(), clr);
                    if (superCmds != null)
                    {
                        for (int i=0;i<superCmds.length;i++)
                        {
                            managingClasses.add(superCmds[i]);
                        }
                    }
                }
            }

            Iterator managingClassesIter = managingClasses.iterator();
            AbstractClassMetaData managingCmds[] = new AbstractClassMetaData[managingClasses.size()];
            int i=0;
            while (managingClassesIter.hasNext())
            {
                managingCmds[i++] = (AbstractClassMetaData)(managingClassesIter.next());
            }
            return managingCmds;
        }
        else if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUPERCLASS_TABLE)
        {
            // Fields managed by superclass, so recurse to that
            return getClassesManagingTableForClass(cmd.getSuperAbstractClassMetaData(), clr);
        }
        return null;
    }

    /**
     * Accessor for whether the specified field of the object is inserted in the datastore yet.
     * @param op ObjectProvider for the object
     * @param fieldNumber (Absolute) field number for the object
     * @return Whether it is persistent
     */
    public boolean isObjectInserted(ObjectProvider op, int fieldNumber)
    {
        if (op == null)
        {
            return false;
        }
        if (!op.isInserting())
        {
            // StateManager isn't inserting so must be persistent
            return true;
        }

        DatastoreClass latestTable = insertedDatastoreClassByObjectProvider.get(op);
        if (latestTable == null)
        {
            // Not yet inserted anything
            return false;
        }

        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd == null)
        {
            // Specified field doesn't exist for this object type!
            return false;
        }

        String className = mmd.getClassName();
        if (mmd.isPrimaryKey())
        {
            // PK field so need to check if the latestTable manages the actual class here
            className = op.getObject().getClass().getName();
        }

        DatastoreClass datastoreCls = latestTable;
        while (datastoreCls != null)
        {
            if (datastoreCls.managesClass(className))
            {
                return true; // This datastore class manages the specified class so it is inserted
            }
            datastoreCls = datastoreCls.getSuperDatastoreClass();
        }
        return false;
    }

    /**
     * Returns whether this object is inserted in the datastore far enough to be considered to be the
     * supplied type. For example if we have base class A, B extends A and this object is a B, and we 
     * pass in A here then this returns whether the A part of the object is now inserted.
     * @param op ObjectProvider for the object
     * @param className Name of class that we want to check the insertion level for.
     * @return Whether the object is inserted in the datastore to this level
     */
    public boolean isObjectInserted(ObjectProvider op, String className)
    {
        if (op == null)
        {
            return false;
        }
        if (!op.isInserting())
        {
            return false;
        }

        DatastoreClass latestTable = insertedDatastoreClassByObjectProvider.get(op);
        if (latestTable != null)
        {
            DatastoreClass datastoreCls = latestTable;
            while (datastoreCls != null)
            {
                if (datastoreCls.managesClass(className))
                {
                    return true; // This datastore class manages the specified class so it is inserted
                }
                datastoreCls = datastoreCls.getSuperDatastoreClass();
            }
        }

        return false;
    }

    /**
     * Method to set that the specified object is inserted down to the defined datastore class.
     * When the object is fully inserted (the table is the primary table for this object type)
     * it is removed from the map of objects being inserted.
     * @param op ObjectProvider for the object
     * @param table Table to which it is now inserted
     */
    public void setObjectIsInsertedToLevel(ObjectProvider op, DatastoreClass table)
    {
        insertedDatastoreClassByObjectProvider.put(op, table);

        if (table.managesClass(op.getClassMetaData().getFullClassName()))
        {
            // Full insertion has just completed so update activity state in StateManager
            op.changeActivityState(ActivityState.INSERTING_CALLBACKS);
            insertedDatastoreClassByObjectProvider.remove(op);
        }
    }

    /**
     * Accessor for the backing store for the specified member.
     * @param clr The ClassLoaderResolver
     * @param mmd metadata for the member to be persisted by this Store
     * @param type instantiated type or prefered type
     * @return The backing store
     */
    public Store getBackingStoreForField(ClassLoaderResolver clr, AbstractMemberMetaData mmd, Class type)
    {
        if (mmd == null || mmd.isSerialized())
        {
            return null;
        }

        Store store = backingStoreByMemberName.get(mmd.getFullFieldName());
        if (store != null)
        {
            return store;
        }

        synchronized (backingStoreByMemberName)
        {
            // Just in case we synced just after someone added since our previous lookup above
            store = backingStoreByMemberName.get(mmd.getFullFieldName());
            if (store != null)
            {
                return store;
            }

            Class expectedMappingType = null;
            if (mmd.getMap() != null)
            {
                expectedMappingType = MapMapping.class;
            }
            else if (mmd.getArray() != null)
            {
                expectedMappingType = ArrayMapping.class;
            }
            else if (mmd.getCollection() != null)
            {
                expectedMappingType = CollectionMapping.class;
            }
            else
            {
                expectedMappingType = PersistableMapping.class;
            }

            // Validate the mapping type matches the table
            try
            {
                DatastoreClass ownerTable = getDatastoreClass(mmd.getClassName(), clr);

                if (ownerTable == null)
                {
                    // Class doesn't manage its own table (uses subclass-table, or superclass-table?)
                    AbstractClassMetaData fieldTypeCmd = getMetaDataManager().getMetaDataForClass(mmd.getClassName(), clr);
                    AbstractClassMetaData[] tableOwnerCmds = getClassesManagingTableForClass(fieldTypeCmd, clr);
                    if (tableOwnerCmds != null && tableOwnerCmds.length == 1)
                    {
                        ownerTable = getDatastoreClass(tableOwnerCmds[0].getFullClassName(), clr);
                    }
                }

                if (ownerTable != null)
                {
                    JavaTypeMapping m = ownerTable.getMemberMapping(mmd);
                    if (!expectedMappingType.isAssignableFrom(m.getClass()))
                    {
                        String requiredType = type != null ? type.getName() : mmd.getTypeName();
                        NucleusLogger.PERSISTENCE.warn("Member " + mmd.getFullFieldName() + " in table=" + ownerTable + " has mapping=" + m + " but expected mapping type=" + expectedMappingType);
                        throw new IncompatibleFieldTypeException(mmd.getFullFieldName(), requiredType, m.getType());
                    }
                }
            }
            catch (NoTableManagedException ntme)
            {
                // Embedded, so just pass through
            }

            if (mmd.getMap() != null)
            {
                store = getBackingStoreForMap(mmd, clr);
            }
            else if (mmd.getArray() != null)
            {
                store = getBackingStoreForArray(mmd, clr);
            }
            else if (mmd.getCollection() != null)
            {
                store = getBackingStoreForCollection(mmd, clr, type);
            }
            else
            {
                store = getBackingStoreForPersistableRelation(mmd, clr);
            }
            backingStoreByMemberName.put(mmd.getFullFieldName(), store);
            return store;
        }
    }

    /**
     * Method to return a backing store for a Collection, consistent with this store and the instantiated type.
     * @param mmd MetaData for the field that has this collection
     * @param clr ClassLoader resolver
     * @param type Type of the field (optional)
     * @return The backing store of this collection in this store
     */
    protected CollectionStore getBackingStoreForCollection(AbstractMemberMetaData mmd, ClassLoaderResolver clr, Class type)
    {
        Table datastoreTable = getTable(mmd);
        if (type == null)
        {
            // No type to base it on so create it based on the field declared type
            if (datastoreTable == null)
            {
                // We need a "FK" relation
                if (Set.class.isAssignableFrom(mmd.getType()))
                {
                    return new FKSetStore(mmd, this, clr);
                }
                else if (List.class.isAssignableFrom(mmd.getType()) || Queue.class.isAssignableFrom(mmd.getType()))
                {
                    return new FKListStore(mmd, this, clr);
                }
                else if (mmd.getOrderMetaData() != null)
                {
                    // User has requested ordering
                    return new FKListStore(mmd, this, clr);
                }

                return new FKSetStore(mmd, this, clr);
            }

            // We need a "JoinTable" relation.
            if (Set.class.isAssignableFrom(mmd.getType()))
            {
                return new JoinSetStore(mmd, (CollectionTable)datastoreTable, clr);
            }
            else if (List.class.isAssignableFrom(mmd.getType()) || Queue.class.isAssignableFrom(mmd.getType()))
            {
                return new JoinListStore(mmd, (CollectionTable)datastoreTable, clr);
            }
            else if (mmd.getOrderMetaData() != null)
            {
                // User has requested ordering
                return new JoinListStore(mmd, (CollectionTable)datastoreTable, clr);
            }

            return new JoinSetStore(mmd, (CollectionTable)datastoreTable, clr);
        }

        // Instantiated type specified, so use it to pick the associated backing store
        if (datastoreTable == null)
        {
            if (SCOUtils.isListBased(type))
            {
                // List required
                return new FKListStore(mmd, this, clr);
            }

            // Set required
            return new FKSetStore(mmd, this, clr);
        }

        if (SCOUtils.isListBased(type))
        {
            // List required
            return new JoinListStore(mmd, (CollectionTable)datastoreTable, clr);
        }

        // Set required
        return new JoinSetStore(mmd, (CollectionTable)datastoreTable, clr);
    }

    /**
     * Method to return a backing store for a Map, consistent with this store and the instantiated type.
     * @param mmd MetaData for the field that has this map
     * @param clr ClassLoader resolver
     * @return The backing store of this map in this store
     */
    protected MapStore getBackingStoreForMap(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        Table datastoreTable = getTable(mmd);
        if (datastoreTable == null)
        {
            return new FKMapStore(mmd, this, clr);
        }

        return new JoinMapStore((MapTable)datastoreTable, clr);
    }

    /**
     * Method to return a backing store for an array, consistent with this store and the instantiated type.
     * @param mmd MetaData for the field/property that has this array
     * @param clr ClassLoader resolver
     * @return The backing store of this array in this store
     */
    protected ArrayStore getBackingStoreForArray(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        Table datastoreTable = getTable(mmd);
        if (datastoreTable != null)
        {
            return new JoinArrayStore(mmd, (ArrayTable)datastoreTable, clr);
        }

        return new FKArrayStore(mmd, this, clr);
    }

    /**
     * Method to return a backing store for a persistable relation (N-1 uni via join).
     * @param mmd MetaData for the member being stored
     * @param clr ClassLoader resolver
     * @return The backing store of this persistable relation in this store
     */
    protected PersistableRelationStore getBackingStoreForPersistableRelation(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        return new JoinPersistableRelationStore(mmd, (PersistableJoinTable)getTable(mmd), clr);
    }

    /**
     * Method to return the default identifier case.
     * @return Identifier case to use if not specified by the user
     */
    public String getDefaultIdentifierCase()
    {
        return "UPPERCASE";
    }

    public MultiMap getSchemaCallbacks()
    {
        return schemaCallbacks;
    }

    public void addSchemaCallback(String className, AbstractMemberMetaData mmd)
    {
        Collection coll = (Collection)schemaCallbacks.get(className);
        if (coll == null || !coll.contains(mmd))
        {
            schemaCallbacks.put(className, mmd);
        }
        else
        {
            NucleusLogger.DATASTORE_SCHEMA.debug("RDBMSStoreManager.addSchemaCallback called for " + mmd.getFullFieldName() + " on class=" + className + " but already registered");
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#isJdbcStore()
     */
    @Override
    public boolean isJdbcStore()
    {
        return true;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getNativeQueryLanguage()
     */
    @Override
    public String getNativeQueryLanguage()
    {
        return QueryLanguage.SQL.toString();
    }

    /**
     * Convenience method to log the configuration of this store manager.
     */
    protected void logConfiguration()
    {
        super.logConfiguration();

        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug("Datastore Adapter : " + dba.getClass().getName());
            NucleusLogger.DATASTORE.debug("Datastore : name=\"" + dba.getDatastoreProductName() + "\" version=\"" + dba.getDatastoreProductVersion() + "\"");
            NucleusLogger.DATASTORE.debug("Datastore Driver : name=\"" + dba.getDatastoreDriverName() + "\" version=\"" + dba.getDatastoreDriverVersion() + "\"");

            // Connection Information
            String primaryDS = null;
            if (getConnectionFactory() != null)
            {
                primaryDS = "DataSource[input DataSource]";
            }
            else if (getConnectionFactoryName() != null)
            {
                primaryDS = "JNDI[" + getConnectionFactoryName() + "]";
            }
            else
            {
                primaryDS = "URL[" + getConnectionURL() + "]";
            }
            NucleusLogger.DATASTORE.debug("Primary Connection Factory : " + primaryDS);
            String secondaryDS = null;
            if (getConnectionFactory2() != null)
            {
                secondaryDS = "DataSource[input DataSource]";
            }
            else if (getConnectionFactory2Name() != null)
            {
                secondaryDS = "JNDI[" + getConnectionFactory2Name() + "]";
            }
            else
            {
                if (getConnectionURL() != null)
                {
                    secondaryDS = "URL[" + getConnectionURL() + "]";
                }
                else
                {
                    secondaryDS = primaryDS;
                }
            }
            NucleusLogger.DATASTORE.debug("Secondary Connection Factory : " + secondaryDS);

            if (identifierFactory != null)
            {
                NucleusLogger.DATASTORE.debug("Datastore Identifiers :" +
                    " factory=\"" + getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_FACTORY) + "\"" +
                    " case=" + identifierFactory.getNamingCase().toString() +
                    (catalogName != null ? (" catalog=" + catalogName) : "") +
                    (schemaName != null ? (" schema=" + schemaName) : ""));
                NucleusLogger.DATASTORE.debug("Supported Identifier Cases : " +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE) ? "lowercase " : "") +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE_QUOTED) ? "\"lowercase\" " : "") +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE) ? "MixedCase " : "") +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED) ? "\"MixedCase\" " : "") +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE) ? "UPPERCASE " : "") +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE_QUOTED) ? "\"UPPERCASE\" " : "") +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_SENSITIVE) ? "MixedCase-Sensitive " : "") +
                    (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED_SENSITIVE) ? "\"MixedCase-Sensitive\" " : ""));
                NucleusLogger.DATASTORE.debug("Supported Identifier Lengths (max) :" +
                    " Table=" + dba.getDatastoreIdentifierMaxLength(IdentifierType.TABLE) +
                    " Column=" + dba.getDatastoreIdentifierMaxLength(IdentifierType.COLUMN) +
                    " Constraint=" + dba.getDatastoreIdentifierMaxLength(IdentifierType.CANDIDATE_KEY) +
                    " Index=" + dba.getDatastoreIdentifierMaxLength(IdentifierType.INDEX) +
                    " Delimiter=" + dba.getIdentifierQuoteString());
                NucleusLogger.DATASTORE.debug("Support for Identifiers in DDL :" +
                    " catalog=" + dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS) +
                    " schema=" + dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS));
            }
            NucleusLogger.DATASTORE.debug("Datastore : " +
                (getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CHECK_EXISTS_TABLES_VIEWS) ? "checkTableViewExistence" : "") +
                ", rdbmsConstraintCreateMode=" + getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE) +
                ", initialiseColumnInfo=" + getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_INIT_COLUMN_INFO));

            int batchLimit = getIntProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STATEMENT_BATCH_LIMIT);
            boolean supportBatching = dba.supportsOption(DatastoreAdapter.STATEMENT_BATCHING);
            if (supportBatching)
            {
                NucleusLogger.DATASTORE.debug("Support Statement Batching : yes (max-batch-size=" + 
                    (batchLimit == -1 ? "UNLIMITED" : "" + batchLimit) + ")");
            }
            else
            {
                NucleusLogger.DATASTORE.debug("Support Statement Batching : no");
            }

            NucleusLogger.DATASTORE.debug("Queries : Results " +
                "direction=" + getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_FETCH_DIRECTION) + 
                ", type=" + getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_TYPE) +
                ", concurrency=" + getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_CONCURRENCY));

            // JDBC Types
            NucleusLogger.DATASTORE.debug("Java-Types : string-default-length=" + getIntProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STRING_DEFAULT_LENGTH));
            RDBMSTypesInfo typesInfo = (RDBMSTypesInfo)schemaHandler.getSchemaData(null, "types", null);
            if (typesInfo != null && typesInfo.getNumberOfChildren() > 0)
            {
                StringBuilder typeStr = new StringBuilder();
                Iterator jdbcTypesIter = typesInfo.getChildren().keySet().iterator();
                while (jdbcTypesIter.hasNext())
                {
                    String jdbcTypeStr = (String)jdbcTypesIter.next();
                    int jdbcTypeNumber = 0;
                    try
                    {
                        jdbcTypeNumber = Short.parseShort(jdbcTypeStr);
                    }
                    catch (NumberFormatException nfe) { }

                    String typeName = dba.getNameForJDBCType(jdbcTypeNumber);
                    if (typeName == null)
                    {
                        typeName = "[id=" + jdbcTypeNumber + "]";
                    }
                    typeStr.append(typeName);
                    if (jdbcTypesIter.hasNext())
                    {
                        typeStr.append(", ");
                    }
                }
                NucleusLogger.DATASTORE.debug("JDBC-Types : " + typeStr);
            }

            NucleusLogger.DATASTORE.debug("===========================================================");
        }
    }

    /**
     * Release of resources
     */
    public synchronized void close()
    {
        dba = null;
        super.close();
        classAdder = null;
    }

    /**
     * Method to return a datastore sequence for this datastore matching the passed sequence MetaData.
     * @param ec execution context
     * @param seqmd SequenceMetaData
     * @return The Sequence
     */
    public NucleusSequence getNucleusSequence(ExecutionContext ec, SequenceMetaData seqmd)
    {
        return new NucleusSequenceImpl(ec, this, seqmd);
    }

    /**
     * Method to return a NucleusConnection for the ExecutionContext.
     * @param ec execution context
     * @return The NucleusConnection
     */
    public NucleusConnection getNucleusConnection(final ExecutionContext ec)
    {
        final ManagedConnection mc;

        final boolean enlisted;
        if (!ec.getTransaction().isActive())
        {
            // no active transaction so don't enlist
            enlisted = false;
        }
        else
        {
            enlisted = true;
        }
        ConnectionFactory cf = null;
        if (enlisted)
        {
            cf = connectionMgr.lookupConnectionFactory(primaryConnectionFactoryName);
        }
        else
        {
            cf = connectionMgr.lookupConnectionFactory(secondaryConnectionFactoryName);
        }
        mc = cf.getConnection(enlisted ? ec : null, ec.getTransaction(), null); // Will throw exception if already locked

        // Lock the connection now that it is in use by the user
        mc.lock();

        Runnable closeRunnable = new Runnable()
        {
            public void run()
            {
                // Unlock the connection now that the user has finished with it
                mc.unlock();
                if (!enlisted)
                {
                    // Close the (unenlisted) connection (committing its statements)
                    try
                    {
                        ((Connection)mc.getConnection()).close();
                    }
                    catch (SQLException sqle)
                    {
                        throw new NucleusDataStoreException(sqle.getMessage());
                    }
                }
            }
        };
        return new NucleusConnectionImpl(mc.getConnection(), closeRunnable);
    }

    /**
     * Accessor for the SQL controller.
     * @return The SQL controller
     */
    public SQLController getSQLController()
    {
        return sqlController;
    }

    /**
     * Accessor for the SQL expression factory to use when generating SQL statements.
     * @return SQL expression factory
     */
    public SQLExpressionFactory getSQLExpressionFactory()
    {
        return expressionFactory;
    }

    /**
     * Initialises the schema name for the datastore, and (optionally) the schema table.
     * @param conn A connection to the database
     * @param clr ClassLoader resolver
     */
    private void initialiseSchema(Connection conn, ClassLoaderResolver clr)
    throws Exception
    {
        if (schemaName == null && catalogName == null)
        {
            // Initialise the Catalog/Schema names
            if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS) || dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS))
            {
                // User didn't provide catalog/schema and the datastore supports one or other so determine the defaults from the datastore
                try
                {
                    try
                    {
                        catalogName = dba.getCatalogName(conn);
                        schemaName = dba.getSchemaName(conn);
                    }
                    catch (UnsupportedOperationException e)
                    {
                        if (!getBooleanProperty(PropertyNames.PROPERTY_DATASTORE_READONLY) && getSchemaHandler().isAutoCreateTables())
                        {
                            // If we aren't a read-only datastore, try to create a table and then 
                            // retrieve its details, so as to obtain the catalog, schema. 
                            ProbeTable pt = new ProbeTable(this);
                            pt.initialize(clr);
                            pt.create(conn);
                            try
                            {
                                String[] schema_details = pt.findSchemaDetails(conn);
                                if (schema_details != null)
                                {
                                    catalogName = schema_details[0];
                                    schemaName = schema_details[1];
                                }
                            }
                            finally
                            {
                                pt.drop(conn);
                            }
                        }
                    }
                }
                catch (SQLException e)
                {
                    String msg = Localiser.msg("050005", e.getMessage()) + ' ' + Localiser.msg("050006");
                    NucleusLogger.DATASTORE_SCHEMA.warn(msg);
                    // This is only logged as a warning since if the JDBC driver has some issue creating the ProbeTable we would be stuck
                    // We need to allow SchemaTool "dbinfo" mode to work in all circumstances.
                }
            }
        }
        // TODO If catalogName/schemaName are set convert them to the adapter case

        if (getBooleanProperty(PropertyNames.PROPERTY_DATASTORE_READONLY))
        {
            // AutoStarter - Don't allow usage of SchemaTable mechanism if fixed/readonly schema
            String autoStartMechanismName = nucleusContext.getConfiguration().getStringProperty(PropertyNames.PROPERTY_AUTOSTART_MECHANISM);
            if ("SchemaTable".equals(autoStartMechanismName))
            {
                // Schema fixed and user requires an auto-starter needing schema content so turn it off
                nucleusContext.getConfiguration().setProperty(PropertyNames.PROPERTY_AUTOSTART_MECHANISM, "None");
            }
        }
        else
        {
            // Provide any add-ons for the datastore that may be needed later
            dba.initialiseDatastore(conn);
        }
    }

    /**
     * Clears all knowledge of tables, cached requests, metadata, etc and resets
     * the store manager to its initial state.
     */
    private void clearSchemaData()
    {
        deregisterAllStoreData();

        // Clear and reinitialise the schemaHandler
        schemaHandler.clear();
        ManagedConnection mc = getConnection(-1);
        try
        {
            dba.initialiseTypes(schemaHandler, mc);
        }
        finally
        {
            mc.release();
        }

        ((RDBMSPersistenceHandler)persistenceHandler).removeAllRequests();
    }

    /**
     * Accessor for the (default) RDBMS catalog name.
     * @return The catalog name.
     */
    public String getCatalogName()
    {
        return catalogName;
    }

    /**
     * Accessor for the (default) RDBMS schema name.
     * @return The schema name.
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * Get the date/time of the datastore.
     * @return Date/time of the datastore
     */
    public Date getDatastoreDate()
    {
        Date serverDate = null;

        String dateStmt = dba.getDatastoreDateStatement();
        ManagedConnection mconn = null;
        try
        {
            mconn = getConnection(TransactionIsolation.NONE);

            PreparedStatement ps = null;
            ResultSet rs = null;
            try
            {
                ps = getSQLController().getStatementForQuery(mconn, dateStmt);
                rs = getSQLController().executeStatementQuery(null, mconn, dateStmt, ps);
                if (rs.next())
                {
                    // Retrieve the timestamp for the server date/time using the server TimeZone from OMF
                    // Assume that the dateStmt returns 1 column and is Timestamp
                    Timestamp time = rs.getTimestamp(1, getCalendarForDateTimezone());
                    serverDate = new Date(time.getTime());
                }
                else
                {
                    return null;
                }
            }
            catch (SQLException sqle)
            {
                String msg = Localiser.msg("050052", sqle.getMessage());
                NucleusLogger.DATASTORE.warn(msg, sqle);
                throw new NucleusUserException(msg, sqle).setFatal();
            }
            finally
            {
                if (rs != null)
                {
                    rs.close();
                }
                if (ps != null)
                {
                    getSQLController().closeStatement(mconn, ps);
                }
            }
        }
        catch (SQLException sqle)
        {
            String msg = Localiser.msg("050052", sqle.getMessage());
            NucleusLogger.DATASTORE.warn(msg, sqle);
            throw new NucleusException(msg, sqle).setFatal();
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }

        return serverDate;
    }

    // ----------------------------- Class Management -------------------------------

    /**
     * Method to add several persistable classes to the store manager's set of supported classes.
     * This will create any necessary database objects (tables, views, constraints, indexes etc). 
     * This will also cause the addition of any related classes.
     * @param clr The ClassLoaderResolver
     * @param classNames Name of the class(es) to be added.
     */
    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null || classNames.length == 0)
        {
            return;
        }

        boolean allManaged = true;
        for (int i=0;i<classNames.length;i++)
        {
            if (!managesClass(classNames[i]))
            {
                 allManaged = false;
                 break;
            }
        }
        if (allManaged)
        {
            return;
        }

        // Filter out any supported classes
        classNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);
        if (classNames.length == 0)
        {
            return;
        }

        try
        {
            schemaLock.writeLock().lock();

            if (classAdder != null)
            {
                // manageClasses() has been recursively re-entered: just add table objects for the requested classes and return.
                classAdder.addClassTables(classNames, clr);
                return;
            }

            new ClassAdder(classNames, null).execute(clr);
        }
        finally
        {
            schemaLock.writeLock().unlock();
        }
    }

    /**
     * Utility to remove all classes that we are managing.
     * @param clr The ClassLoaderResolver
     */
    public void unmanageAllClasses(ClassLoaderResolver clr)
    {
        DeleteTablesSchemaTransaction deleteTablesTxn = new DeleteTablesSchemaTransaction(this, Connection.TRANSACTION_READ_COMMITTED, storeDataMgr);
        boolean success = true;
        try
        {
            deleteTablesTxn.execute(clr);
        }
        catch (NucleusException ne)
        {
            success = false;
            throw ne;
        }
        finally
        {
            if (success)
            {
                clearSchemaData();
            }
        }
    }

    /**
     * Accessor for the writer for DDL (if set).
     * @return DDL writer
     */
    public Writer getDdlWriter()
    {
        return ddlWriter;
    }

    /**
     * Accessor for whether we should generate complete DDL when in that mode.
     * Otherwise will generate "upgrade DDL".
     * @return Generate complete DDL ?
     */
    public boolean getCompleteDDL()
    {
        return completeDDL;
    }

    /**
     * When we are in SchemaTool DDL mode, return if the supplied statement is already present.
     * This is used to eliminate duplicate statements from a bidirectional relation.
     * @param stmt The statement
     * @return Whether we have that statement already
     */
    public boolean hasWrittenDdlStatement(String stmt)
    {
        return writtenDdlStatements != null && writtenDdlStatements.contains(stmt);
    }

    /**
     * When we are in SchemaTool DDL mode, add a new DDL statement.
     * @param stmt The statement
     */
    public void addWrittenDdlStatement(String stmt)
    {
        if (writtenDdlStatements != null)
        {
            writtenDdlStatements.add(stmt);
        }
    }

    /**
     * Utility to validate the specified table.
     * This is useful where we have made an update to the columns in a table and want to apply the updates to the datastore.
     * @param table The table to validate
     * @param clr The ClassLoaderResolver
     */
    public void validateTable(final TableImpl table, ClassLoaderResolver clr)
    {
        ValidateTableSchemaTransaction validateTblTxn = new ValidateTableSchemaTransaction(this, 
            Connection.TRANSACTION_READ_COMMITTED, table);
        validateTblTxn.execute(clr);
    }

    // ---------------------------------------------------------------------------------------

    /**
     * Returns the class corresponding to the given object identity.
     * If the identity is an instanceof OID then returns the associated persistable class name.
     * If the identity is a SCOID, return the SCO class.
     * If the identity is a SingleFieldIdentity then returns the associated persistable class name.
     * If the object is an AppID PK, returns the associated PC class (as far as determinable).
     * If the object id is an application id and the user supplies the "ec" argument then
     * a check can be performed in the datastore where necessary.
     * @param id The identity of some object.
     * @param clr ClassLoader resolver
     * @param ec execution context (optional - to allow check inheritance level in datastore)
     * @return For datastore identity, return the class of the corresponding object.
     *      For application identity, return the class of the corresponding object.
     *      Otherwise returns null if unable to tie as the identity of a particular class.
     */
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr, ExecutionContext ec)
    {
        if (id instanceof SCOID)
        {
            // Object is a SCOID
            return ((SCOID)id).getSCOClass();
        }

        // Generate a list of metadata for the roots of inheritance tree(s) that this identity can represent
        // Really ought to be for a single inheritance tree (hence one element in the List) but we allow for
        // a user reusing their PK class in multiple trees
        List<AbstractClassMetaData> rootCmds = new ArrayList<>();
        if (IdentityUtils.isDatastoreIdentity(id))
        {
            // Datastore Identity, so identity is an OID, and the object is of the target class or a subclass
            String className = IdentityUtils.getTargetClassNameForIdentitySimple(id);
            AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
            rootCmds.add(cmd);
            if (cmd.getIdentityType() != IdentityType.DATASTORE)
            {
                throw new NucleusUserException(Localiser.msg("050022", id, cmd.getFullClassName()));
            }
        }
        else if (IdentityUtils.isSingleFieldIdentity(id))
        {
            // Using SingleFieldIdentity so can assume that object is of the target class or a subclass
            String className = IdentityUtils.getTargetClassNameForIdentitySimple(id);
            AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
            rootCmds.add(cmd);
            if (cmd.getIdentityType() != IdentityType.APPLICATION || !cmd.getObjectidClass().equals(id.getClass().getName()))
            {
                throw new NucleusUserException(Localiser.msg("050022", id, cmd.getFullClassName()));
            }
        }
        else
        {
            // Find all of the class with a PK class of this type
            Collection<AbstractClassMetaData> pkCmds = getMetaDataManager().getClassMetaDataWithApplicationId(id.getClass().getName());
            if (pkCmds != null && pkCmds.size() > 0)
            {
                Iterator<AbstractClassMetaData> iter = pkCmds.iterator();
                while (iter.hasNext())
                {
                    AbstractClassMetaData pkCmd = iter.next();

                    AbstractClassMetaData cmdToSwap = null;
                    boolean toAdd = true;

                    Iterator<AbstractClassMetaData> rootCmdIterator = rootCmds.iterator();
                    while (rootCmdIterator.hasNext())
                    {
                        AbstractClassMetaData rootCmd = rootCmdIterator.next();
                        if (rootCmd.isDescendantOf(pkCmd))
                        {
                            // This cmd is a parent of an existing, so swap them
                            cmdToSwap = rootCmd;
                            toAdd = false;
                            break;
                        }
                        else if (pkCmd.isDescendantOf(rootCmd))
                        {
                            toAdd = false;
                        }
                    }

                    if (cmdToSwap != null)
                    {
                        rootCmds.remove(cmdToSwap);
                        rootCmds.add(pkCmd);
                    }
                    else if (toAdd)
                    {
                        rootCmds.add(pkCmd);
                    }
                }
            }

            if (rootCmds.size() == 0)
            {
                return null;
            }
        }

        AbstractClassMetaData rootCmd = rootCmds.get(0);
        if (ec != null)
        {
            // Perform a check on the exact object inheritance level with this key (uses SQL query)
            if (rootCmds.size() == 1)
            {
                Collection<String> subclasses = getSubClassesForClass(rootCmd.getFullClassName(), true, clr);
                if (!rootCmd.isImplementationOfPersistentDefinition())
                {
                    // Not persistent interface implementation so check if any subclasses
                    if (subclasses == null || subclasses.isEmpty())
                    {
                        // No subclasses, so we assume that this is root class only
                        // NB there could be other supertypes sharing this table, but the id is set to this class name
                        // so we assume it can't be a supertype
                        return rootCmd.getFullClassName();
                        // This commented out code simply restricts if other classes are using the table
                        /*DatastoreClass primaryTable = getDatastoreClass(rootCmd.getFullClassName(), clr);
                        String[] managedClassesInTable = primaryTable.getManagedClasses();
                        if (managedClassesInTable.length == 1 && managedClassesInTable[0].equals(rootCmd.getFullClassName()))
                        {
                            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                            {
                                NucleusLogger.PERSISTENCE.debug("Sole candidate for id is " +
                                    rootCmd.getFullClassName() + " and has no subclasses, so returning without checking datastore");
                            }
                            return rootCmd.getFullClassName();
                        }*/
                    }
                }

                // Check how many concrete classes we have in this tree, in case only one
                int numConcrete = 0;
                String concreteClassName = null;
                Class rootCls = clr.classForName(rootCmd.getFullClassName());
                if (!Modifier.isAbstract(rootCls.getModifiers()))
                {
                    concreteClassName = rootCmd.getFullClassName();
                    numConcrete++;
                }
                for (String subclassName : subclasses)
                {
                    Class subcls = clr.classForName(subclassName);
                    if (!Modifier.isAbstract(subcls.getModifiers()))
                    {
                        if (concreteClassName == null)
                        {
                            concreteClassName = subclassName;
                        }
                        numConcrete++;
                    }
                }
                if (numConcrete == 1)
                {
                    // Single possible concrete class, so return it
                    return concreteClassName;
                }

                // Simple candidate query of this class and subclasses
                if (rootCmd.hasDiscriminatorStrategy())
                {
                    // Query using discriminator
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug("Performing query using discriminator on " +
                            rootCmd.getFullClassName() + " and its subclasses to find the class of " + id);
                    }
                    return RDBMSStoreHelper.getClassNameForIdUsingDiscriminator(this, ec, id, rootCmd);
                }

                // Query using UNION
                if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                {
                    NucleusLogger.PERSISTENCE.debug("Performing query using UNION on " +
                            rootCmd.getFullClassName() + " and its subclasses to find the class of " + id);
                }
                return RDBMSStoreHelper.getClassNameForIdUsingUnion(this, ec, id, rootCmds);
            }

            // Multiple possible roots so use UNION statement
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                StringBuilder str = new StringBuilder();
                Iterator<AbstractClassMetaData> rootCmdIter = rootCmds.iterator();
                while (rootCmdIter.hasNext())
                {
                    AbstractClassMetaData cmd = rootCmdIter.next();
                    str.append(cmd.getFullClassName());
                    if (rootCmdIter.hasNext())
                    {
                        str.append(",");
                    }
                }
                NucleusLogger.PERSISTENCE.debug("Performing query using UNION on " + str.toString() + " and their subclasses to find the class of " + id);
            }
            return RDBMSStoreHelper.getClassNameForIdUsingUnion(this, ec, id, rootCmds);
        }

        // Check not possible so just return the first root
        if (rootCmds.size() > 1)
        {
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug("Id \""+id+"\" has been determined to be the id of class "+
                        rootCmd.getFullClassName() + " : this is the first of " + rootCmds.size() + " possible, but unable to determine further");
            }
            return rootCmd.getFullClassName();
        }

        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug("Id \""+id+"\" has been determined to be the id of class "+
                    rootCmd.getFullClassName() + " : unable to determine if actually of a subclass");
        }
        return rootCmd.getFullClassName();
    }

    public FieldManager getFieldManagerForResultProcessing(ObjectProvider op, ResultSet rs, StatementClassMapping resultMappings)
    {
        return new ResultSetGetter(this, op, rs, resultMappings);
    }

    public FieldManager getFieldManagerForResultProcessing(ExecutionContext ec, ResultSet rs, StatementClassMapping resultMappings, AbstractClassMetaData cmd)
    {
        return new ResultSetGetter(this, ec, rs, resultMappings, cmd);
    }

    /**
     * Method to return a FieldManager for populating information in statements.
     * @param sm The ObjectProvider for the object.
     * @param ps The Prepared Statement to set values on.
     * @param stmtMappings the index of parameters/mappings
     * @return The FieldManager to use
     */
    public FieldManager getFieldManagerForStatementGeneration(ObjectProvider sm, PreparedStatement ps, StatementClassMapping stmtMappings)
    {
        return new ParameterSetter(sm, ps, stmtMappings);
    }

    /**
     * Method to return the value from the results for the mapping at the specified position.
     * @param rs The results
     * @param mapping The mapping
     * @param position The position in the results
     * @return The value at that position
     * @throws NucleusDataStoreException if an error occurs accessing the results
     */
    public Object getResultValueAtPosition(ResultSet rs, JavaTypeMapping mapping, int position)
    {
        try
        {
            return rs.getObject(position);
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException(sqle.getMessage(), sqle);
        }
    }

    /**
     * Accessor for the next value from the specified generator.
     * This implementation caters for datastore-specific generators and provides synchronisation on the connection to the datastore.
     * @param generator The generator
     * @param ec execution context
     * @return The next value.
     */
    protected Object getStrategyValueForGenerator(ValueGenerator generator, final ExecutionContext ec)
    {
        Object oid = null;
        synchronized (generator)
        {
            // Get the next value for this generator for this ExecutionContext
            // Note : this is synchronised since we dont want to risk handing out this generator
            // while its connectionProvider is set to that of a different ExecutionContext
            // It maybe would be good to change ValueGenerator to have a next taking the connectionProvider
            if (generator instanceof AbstractDatastoreGenerator)
            {
                ConnectionPreference connPref = ((AbstractDatastoreGenerator)generator).getConnectionPreference();
                final boolean newConnection;
                if (connPref == ConnectionPreference.NONE)
                {
                    // No preference from the generator so use NEW unless overridden by the persistence property
                    if (getStringProperty(PropertyNames.PROPERTY_VALUEGEN_TXN_ATTRIBUTE).equalsIgnoreCase("UsePM") || // TODO Deprecated
                        getStringProperty(PropertyNames.PROPERTY_VALUEGEN_TXN_ATTRIBUTE).equalsIgnoreCase("EXISTING"))
                    {
                        newConnection = false;
                    }
                    else
                    {
                        newConnection = true;
                    }
                }
                else
                {
                    newConnection = connPref == ConnectionPreference.NEW;
                }

                // RDBMS-based generator so set the connection provider
                final RDBMSStoreManager thisStoreMgr = this;
                ValueGenerationConnectionProvider connProvider = new ValueGenerationConnectionProvider()
                {
                    ManagedConnection mconn;
                    public ManagedConnection retrieveConnection()
                    {
                        if (newConnection)
                        {
                            mconn = thisStoreMgr.getConnection(TransactionUtils.getTransactionIsolationLevelForName(getStringProperty(PropertyNames.PROPERTY_VALUEGEN_TXN_ISOLATION)));
                        }
                        else
                        {
                            mconn = thisStoreMgr.getConnection(ec);
                        }
                        return mconn;
                    }

                    public void releaseConnection()
                    {
                        try
                        {
                            mconn.release();
                            mconn = null;
                        }
                        catch (NucleusException e)
                        {
                            String msg = Localiser.msg("050025", e);
                            NucleusLogger.VALUEGENERATION.error(msg);
                            throw new NucleusDataStoreException(msg, e);
                        }
                    }
                };
                ((AbstractDatastoreGenerator)generator).setConnectionProvider(connProvider);
            }
            oid = generator.next();
        }
        return oid;
    }

    /**
     * Method to return the properties to pass to the generator for the specified field.
     * @param cmd MetaData for the class
     * @param absoluteFieldNumber Number of the field (-1 = datastore identity)
     * @param ec execution context
     * @param seqmd Any sequence metadata
     * @param tablegenmd Any table generator metadata
     * @return The properties to use for this field
     */
    protected Properties getPropertiesForGenerator(AbstractClassMetaData cmd, int absoluteFieldNumber,
            ExecutionContext ec, SequenceMetaData seqmd, TableGeneratorMetaData tablegenmd)
    {
        AbstractMemberMetaData mmd = null;
        IdentityStrategy strategy = null;
        String sequence = null;
        Map<String, String> extensions = null;
        if (absoluteFieldNumber >= 0)
        {
            // real field
            mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);
            strategy = mmd.getValueStrategy();
            sequence = mmd.getSequence();
            extensions = mmd.getExtensions();
        }
        else
        {
            // datastore-identity surrogate field
            // always use the root IdentityMetaData since the root class defines the identity
            IdentityMetaData idmd = cmd.getBaseIdentityMetaData();
            strategy = idmd.getValueStrategy();
            sequence = idmd.getSequence();
            extensions = idmd.getExtensions();
        }

        // Get base table with the required field
        DatastoreClass tbl = getDatastoreClass(cmd.getBaseAbstractClassMetaData().getFullClassName(), ec.getClassLoaderResolver());
        if (tbl == null)
        {
            tbl = getTableForStrategy(cmd,absoluteFieldNumber,ec.getClassLoaderResolver());
        }
        JavaTypeMapping m = null;
        if (mmd != null)
        {
            m = tbl.getMemberMapping(mmd);
            if (m == null)
            {
                // Field not mapped in root table so use passed-in table
                tbl = getTableForStrategy(cmd,absoluteFieldNumber,ec.getClassLoaderResolver());
                m = tbl.getMemberMapping(mmd);
            }
        }
        else
        {
            m = tbl.getIdMapping();
        }
        StringBuilder columnsName = new StringBuilder();
        for (int i = 0; i < m.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                columnsName.append(",");
            }
            columnsName.append(m.getDatastoreMapping(i).getColumn().getIdentifier().toString());
        }

        Properties properties = new Properties();
        properties.setProperty(ValueGenerator.PROPERTY_CLASS_NAME, cmd.getFullClassName());
        properties.put(ValueGenerator.PROPERTY_ROOT_CLASS_NAME, cmd.getBaseAbstractClassMetaData().getFullClassName());
        if (mmd != null)
        {
            properties.setProperty(ValueGenerator.PROPERTY_FIELD_NAME, mmd.getFullFieldName());
        }

        if (cmd.getCatalog() != null)
        {
            properties.setProperty(ValueGenerator.PROPERTY_CATALOG_NAME, cmd.getCatalog());
        }
        else if (!StringUtils.isWhitespace(catalogName))
        {
            properties.setProperty(ValueGenerator.PROPERTY_CATALOG_NAME, catalogName);
        }

        if (cmd.getSchema() != null)
        {
            properties.setProperty(ValueGenerator.PROPERTY_SCHEMA_NAME, cmd.getSchema());
        }
        else if (!StringUtils.isWhitespace(schemaName))
        {
            properties.setProperty(ValueGenerator.PROPERTY_SCHEMA_NAME, schemaName);
        }

        properties.setProperty(ValueGenerator.PROPERTY_TABLE_NAME, tbl.getIdentifier().toString());
        properties.setProperty(ValueGenerator.PROPERTY_COLUMN_NAME, columnsName.toString());

        if (sequence != null)
        {
            properties.setProperty(ValueGenerator.PROPERTY_SEQUENCE_NAME, sequence);
        }

        // Add any extension properties
        if (extensions != null && extensions.size() > 0)
        {
            properties.putAll(extensions);
        }

        if (strategy.equals(IdentityStrategy.NATIVE))
        {
            String realStrategyName = getStrategyForNative(cmd, absoluteFieldNumber);
            strategy = IdentityStrategy.getIdentityStrategy(realStrategyName);
        }

        if (strategy == IdentityStrategy.INCREMENT && tablegenmd != null)
        {
            // User has specified a TableGenerator (JPA)
            properties.put(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE, "" + tablegenmd.getInitialValue());
            properties.put(ValueGenerator.PROPERTY_KEY_CACHE_SIZE, "" + tablegenmd.getAllocationSize());
            if (tablegenmd.getTableName() != null)
            {
                properties.put(ValueGenerator.PROPERTY_SEQUENCETABLE_TABLE, tablegenmd.getTableName());
            }
            if (tablegenmd.getCatalogName() != null)
            {
                properties.put(ValueGenerator.PROPERTY_SEQUENCETABLE_CATALOG, tablegenmd.getCatalogName());
            }
            if (tablegenmd.getSchemaName() != null)
            {
                properties.put(ValueGenerator.PROPERTY_SEQUENCETABLE_SCHEMA, tablegenmd.getSchemaName());
            }
            if (tablegenmd.getPKColumnName() != null)
            {
                properties.put(ValueGenerator.PROPERTY_SEQUENCETABLE_NAME_COLUMN, tablegenmd.getPKColumnName());
            }
            if (tablegenmd.getPKColumnName() != null)
            {
                properties.put(ValueGenerator.PROPERTY_SEQUENCETABLE_NEXTVAL_COLUMN, tablegenmd.getValueColumnName());
            }
            if (tablegenmd.getPKColumnValue() != null)
            {
                properties.put(ValueGenerator.PROPERTY_SEQUENCE_NAME, tablegenmd.getPKColumnValue());
            }

            // Using JPA generator so don't enable initial value detection
            properties.remove(ValueGenerator.PROPERTY_TABLE_NAME);
            properties.remove(ValueGenerator.PROPERTY_COLUMN_NAME);
        }
        else if (strategy == IdentityStrategy.INCREMENT && tablegenmd == null)
        {
            if (!properties.containsKey(ValueGenerator.PROPERTY_KEY_CACHE_SIZE))
            {
                // Use default allocation size
                int allocSize = getIntProperty(PropertyNames.PROPERTY_VALUEGEN_INCREMENT_ALLOCSIZE);
                properties.put(ValueGenerator.PROPERTY_KEY_CACHE_SIZE, "" + allocSize);
            }
        }
        else if (strategy == IdentityStrategy.SEQUENCE && seqmd != null)
        {
            // User has specified a SequenceGenerator (JDO/JPA)
            if (StringUtils.isWhitespace(sequence))
            {
                // Apply default to sequence name, as name of sequence metadata
                if (seqmd.getName() != null)
                {
                    properties.put(ValueGenerator.PROPERTY_SEQUENCE_NAME, seqmd.getName());
                }
            }
            if (seqmd.getDatastoreSequence() != null)
            {
                if (seqmd.getInitialValue() >= 0)
                {
                    properties.put(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE, "" + seqmd.getInitialValue());
                }
                if (seqmd.getAllocationSize() > 0)
                {
                    properties.put(ValueGenerator.PROPERTY_KEY_CACHE_SIZE, "" + seqmd.getAllocationSize());
                }
                else
                {
                    // Use default allocation size
                    int allocSize = getIntProperty(PropertyNames.PROPERTY_VALUEGEN_SEQUENCE_ALLOCSIZE);
                    properties.put(ValueGenerator.PROPERTY_KEY_CACHE_SIZE, "" + allocSize);
                }
                properties.put(ValueGenerator.PROPERTY_SEQUENCE_NAME, "" + seqmd.getDatastoreSequence());

                // Add on any extensions specified on the sequence
                Map<String, String> seqExtensions = seqmd.getExtensions();
                if (seqExtensions != null && seqExtensions.size() > 0)
                {
                    properties.putAll(seqExtensions);
                }
            }
            else
            {
                // JDO Factory-based sequence generation
                // TODO Support this
            }
        }
        return properties;
    }

    private DatastoreClass getTableForStrategy(AbstractClassMetaData cmd, int fieldNumber, ClassLoaderResolver clr)
    {
        DatastoreClass t = getDatastoreClass(cmd.getFullClassName(), clr);
        if (t == null && cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
        {
            throw new NucleusUserException(Localiser.msg("032013", cmd.getFullClassName()));
        }

        if (fieldNumber>=0)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            t = t.getBaseDatastoreClassWithMember(mmd);
        }
        else if (t!=null)
        {
            // Go up to overall superclass to find id for that class.
            boolean has_superclass = true;
            while (has_superclass)
            {
                DatastoreClass supert = t.getSuperDatastoreClass();
                if (supert != null)
                {
                    t = supert;
                }
                else
                {
                    has_superclass = false;
                }
            }
        }
        return t;
    }

    /**
     * Method defining which value-strategy to use when the user specifies "native".
     * @return Should be overridden by all store managers that have other behaviour.
     */
    public String getStrategyForNative(AbstractClassMetaData cmd, int absFieldNumber)
    {
        // TODO If the user has generated the schema and the column for this field is an autoincrement column then select IDENTITY
        if (getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_LEGACY_NATIVE_VALUE_STRATEGY))
        {
            // Use legacy process for deciding which strategy to use
            String sequence = null;
            if (absFieldNumber >= 0)
            {
                // real field
                sequence = cmd.getMetaDataForManagedMemberAtAbsolutePosition(absFieldNumber).getSequence();
            }
            else
            {
                // datastore-identity surrogate field
                sequence = cmd.getIdentityMetaData().getSequence();
            }

            if (dba.supportsOption(DatastoreAdapter.SEQUENCES) && sequence != null)
            {
                return "sequence";
            }
            return "table-sequence"; // Maybe ought to use "increment"
        }
        return super.getStrategyForNative(cmd, absFieldNumber);
    }

    /**
     * Accessor for the SQL type info for the specified JDBC type
     * @param jdbcType JDBC type
     * @return (default) SQL type info
     * @throws UnsupportedDataTypeException If the JDBC type is not found
     */
    public SQLTypeInfo getSQLTypeInfoForJDBCType(int jdbcType)
    throws UnsupportedDataTypeException
    {
        return this.getSQLTypeInfoForJDBCType(jdbcType, "DEFAULT");
    }

    /**
     * Accessor for the SQL type info for the specified JDBC type.
     * @param jdbcType JDBC type
     * @param sqlType The SQL type name (if known, otherwise uses the default for this JDBC type).
     * @return SQL type info
     * @throws UnsupportedDataTypeException If the JDBC type is not found
     */
    public SQLTypeInfo getSQLTypeInfoForJDBCType(int jdbcType, String sqlType)
    throws UnsupportedDataTypeException
    {
        RDBMSTypesInfo typesInfo = (RDBMSTypesInfo)schemaHandler.getSchemaData(null, "types", null);
        JDBCTypeInfo jdbcTypeInfo = (JDBCTypeInfo)typesInfo.getChild("" + jdbcType);

        if (jdbcTypeInfo.getNumberOfChildren() == 0)
        {
            // No sql-type for this jdbc-type so unsupported
            throw new UnsupportedDataTypeException(Localiser.msg("051005", dba.getNameForJDBCType(jdbcType)));
        }

        SQLTypeInfo sqlTypeInfo = (SQLTypeInfo) jdbcTypeInfo.getChild(sqlType != null ? sqlType : "DEFAULT");
        if (sqlTypeInfo == null && sqlType != null)
        {
            // Try uppercase form of sql-type
            sqlTypeInfo = (SQLTypeInfo) jdbcTypeInfo.getChild(sqlType.toUpperCase());
            if (sqlTypeInfo == null)
            {
                // Try lowercase form of sql-type
                sqlTypeInfo = (SQLTypeInfo) jdbcTypeInfo.getChild(sqlType.toLowerCase());
                if (sqlTypeInfo == null)
                {
                    // fallback to DEFAULT
                    NucleusLogger.DATASTORE_SCHEMA.debug("Attempt to find JDBC driver 'typeInfo' for jdbc-type=" + dba.getNameForJDBCType(jdbcType) +
                        " but sql-type=" + sqlType + " is not found. Using default sql-type for this jdbc-type.");
                    sqlTypeInfo = (SQLTypeInfo) jdbcTypeInfo.getChild("DEFAULT");
                }
            }
        }
        return sqlTypeInfo;
    }

    /**
     * Returns the column info for a column name. This should be used instead
     * of making direct calls to DatabaseMetaData.getColumns().
     * <p>
     * Where possible, this method loads and caches column info for more than
     * just the table being requested, improving performance by reducing the
     * overall number of calls made to DatabaseMetaData.getColumns() (each of
     * which usually results in one or more database queries).
     * </p>
     * @param table The table/view
     * @param conn JDBC connection to the database.
     * @param column the column
     * @return The ColumnInfo objects describing the column.
     * @throws SQLException Thrown if an error occurs
     */
    public RDBMSColumnInfo getColumnInfoForColumnName(Table table, Connection conn, DatastoreIdentifier column)
    throws SQLException
    {
        return (RDBMSColumnInfo)schemaHandler.getSchemaData(conn, "column", new Object[] {table, column.getName()});
    }

    /**
     * Returns the column info for a database table. This should be used instead
     * of making direct calls to DatabaseMetaData.getColumns().
     * <p>
     * Where possible, this method loads and caches column info for more than
     * just the table being requested, improving performance by reducing the
     * overall number of calls made to DatabaseMetaData.getColumns() (each of
     * which usually results in one or more database queries).
     * </p>
     * @param table The table/view
     * @param conn JDBC connection to the database.
     * @return A list of ColumnInfo objects describing the columns of the table.
     * The list is in the same order as was supplied by getColumns(). If no
     * column info is found for the given table, an empty list is returned.
     * @throws SQLException Thrown if an error occurs
     */
    public List getColumnInfoForTable(Table table, Connection conn)
    throws SQLException
    {
        RDBMSTableInfo tableInfo = (RDBMSTableInfo)schemaHandler.getSchemaData(conn, "columns", new Object[] {table});
        if (tableInfo == null)
        {
            return Collections.EMPTY_LIST;
        }

        List cols = new ArrayList(tableInfo.getNumberOfChildren());
        cols.addAll(tableInfo.getChildren());
        return cols;
    }

    /**
     * Method to invalidate the cached column info for a table.
     * This is called when we have just added columns to the table in the schema
     * has the effect of a reload of the tables information the next time it is needed.
     * @param table The table
     */
    public void invalidateColumnInfoForTable(Table table)
    {
        RDBMSSchemaInfo schemaInfo = (RDBMSSchemaInfo)schemaHandler.getSchemaData(null, "tables", null);
        if (schemaInfo != null && schemaInfo.getNumberOfChildren() > 0)
        {
            schemaInfo.getChildren().remove(table.getIdentifier().getFullyQualifiedName(true));
        }
    }

    /**
     * Convenience accessor of the Table objects managed in this datastore at this point.
     * @param catalog Name of the catalog to restrict the collection by (or null to not restrict)
     * @param schema Name of the schema to restrict the collection by (or null to not restrict)
     * @return Collection of tables
     */
    public Collection<Table> getManagedTables(String catalog, String schema)
    {
        if (storeDataMgr == null)
        {
            return Collections.EMPTY_SET;
        }

        Collection tables = new HashSet();
        for (Iterator<StoreData> i = storeDataMgr.getManagedStoreData().iterator(); i.hasNext();)
        {
            RDBMSStoreData sd = (RDBMSStoreData) i.next();
            if (sd.getTable() != null)
            {
                // Catalog/Schema match if either managed table not set, or input requirements not set
                DatastoreIdentifier identifier = ((Table)sd.getTable()).getIdentifier();
                boolean catalogMatches = true;
                boolean schemaMatches = true;
                if (catalog != null && identifier.getCatalogName() != null &&!catalog.equals(identifier.getCatalogName()))
                {
                    catalogMatches = false;
                }
                if (schema != null && identifier.getSchemaName() != null && !schema.equals(identifier.getSchemaName()))
                {
                    schemaMatches = false;
                }
                if (catalogMatches && schemaMatches)
                {
                    tables.add(sd.getTable());
                }
            }
        }
        return tables;
    }

    /**
     * Resolves an identifier macro. The public fields <var>className</var>, <var>fieldName </var>,
     * and <var>subfieldName </var> of the given macro are taken as inputs, and the public
     * <var>value </var> field is set to the SQL identifier of the corresponding database table or column.
     * @param im The macro to resolve.
     * @param clr The ClassLoaderResolver
     */
    public void resolveIdentifierMacro(MacroString.IdentifierMacro im, ClassLoaderResolver clr)
    {
        DatastoreClass ct = getDatastoreClass(im.className, clr);
        if (im.fieldName == null)
        {
            im.value = ct.getIdentifier().toString();
            return;
        }

        JavaTypeMapping m;
        if (im.fieldName.equals("this")) // TODO This should be candidate alias or something, not hardcoded "this"
        {
            if (!(ct instanceof ClassTable))
            {
                throw new NucleusUserException(Localiser.msg("050034", im.className));
            }

            if (im.subfieldName != null)
            {
                throw new NucleusUserException(Localiser.msg("050035", im.className, im.fieldName, im.subfieldName));
            }
            m = ((Table) ct).getIdMapping();
        }
        else
        {
            AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(im.className, clr);
            AbstractMemberMetaData mmd = cmd.getMetaDataForMember(im.fieldName);
            m = ct.getMemberMapping(mmd);
            Table t = getTable(mmd);
            if (im.subfieldName == null)
            {
                if (t != null)
                {
                    im.value = t.getIdentifier().toString();
                    return;
                }
            }
            else
            {
                if (t instanceof CollectionTable)
                {
                    CollectionTable collTable = (CollectionTable) t;
                    if (im.subfieldName.equals("owner"))
                    {
                        m = collTable.getOwnerMapping();
                    }
                    else if (im.subfieldName.equals("element"))
                    {
                        m = collTable.getElementMapping();
                    }
                    else if (im.subfieldName.equals("index"))
                    {
                        m = collTable.getOrderMapping();
                    }
                    else
                    {
                        throw new NucleusUserException(Localiser.msg("050036", im.subfieldName, im));
                    }
                }
                else if (t instanceof MapTable)
                {
                    MapTable mt = (MapTable) t;
                    if (im.subfieldName.equals("owner"))
                    {
                        m = mt.getOwnerMapping();
                    }
                    else if (im.subfieldName.equals("key"))
                    {
                        m = mt.getKeyMapping();
                    }
                    else if (im.subfieldName.equals("value"))
                    {
                        m = mt.getValueMapping();
                    }
                    else
                    {
                        throw new NucleusUserException(Localiser.msg("050037", im.subfieldName, im));
                    }
                }
                else
                {
                    throw new NucleusUserException(Localiser.msg("050035", im.className, im.fieldName, im.subfieldName));
                }
            }
        }
        im.value = m.getDatastoreMapping(0).getColumn().getIdentifier().toString();
    }

    /**
     * Method to output particular information owned by this datastore.
     * Supports "DATASTORE" and "SCHEMA" categories.
     * @param category Category of information
     * @param ps PrintStream
     * @throws Exception Thrown if an error occurs in the output process
     */
    public void printInformation(String category, PrintStream ps)
    throws Exception
    {
        DatastoreAdapter dba = getDatastoreAdapter();

        super.printInformation(category, ps);

        if (category.equalsIgnoreCase("DATASTORE"))
        {
            ps.println(dba.toString());
            ps.println();
            ps.println("Database TypeInfo");

            RDBMSTypesInfo typesInfo = (RDBMSTypesInfo)schemaHandler.getSchemaData(null, "types", null);
            if (typesInfo != null)
            {
                Iterator iter = typesInfo.getChildren().keySet().iterator();
                while (iter.hasNext())
                {
                    String jdbcTypeStr = (String)iter.next();
                    short jdbcTypeNumber = 0;
                    try
                    {
                        jdbcTypeNumber = Short.parseShort(jdbcTypeStr);
                    }
                    catch (NumberFormatException nfe) { }
                    JDBCTypeInfo jdbcType = (JDBCTypeInfo)typesInfo.getChild(jdbcTypeStr);
                    Collection sqlTypeNames = jdbcType.getChildren().keySet();

                    // SQL type names for JDBC type
                    String typeStr = "JDBC Type=" + dba.getNameForJDBCType(jdbcTypeNumber) +
                        " sqlTypes=" + StringUtils.collectionToString(sqlTypeNames);
                    ps.println(typeStr);

                    // Default SQL type details
                    SQLTypeInfo sqlType = (SQLTypeInfo)jdbcType.getChild("DEFAULT");
                    ps.println(sqlType);
                }
            }
            ps.println("");

            // Print out the keywords info
            ps.println("Database Keywords");

            Iterator reservedWordsIter = dba.iteratorReservedWords();
            while (reservedWordsIter.hasNext())
            {
                Object words = reservedWordsIter.next();
                ps.println(words);
            }
            ps.println("");
        }
        else if (category.equalsIgnoreCase("SCHEMA"))
        {
            ps.println(dba.toString());
            ps.println();
            ps.println("TABLES");

            ManagedConnection mc = getConnection(-1);
            try
            {
                Connection conn = (Connection)mc.getConnection();
                RDBMSSchemaInfo schemaInfo = (RDBMSSchemaInfo)schemaHandler.getSchemaData(conn, "tables", new Object[] {this.catalogName, this.schemaName});
                if (schemaInfo != null)
                {
                    Iterator tableIter = schemaInfo.getChildren().values().iterator();
                    while (tableIter.hasNext())
                    {
                        // Print out the table information
                        RDBMSTableInfo tableInfo = (RDBMSTableInfo)tableIter.next();
                        ps.println(tableInfo);

                        Iterator columnIter = tableInfo.getChildren().iterator();
                        while (columnIter.hasNext())
                        {
                            // Print out the column information
                            RDBMSColumnInfo colInfo = (RDBMSColumnInfo)columnIter.next();
                            ps.println(colInfo);
                        }
                    }
                }
            }
            finally
            {
                if (mc != null)
                {
                    mc.release();
                }
            }
            ps.println("");
        }
    }

    /**
     * Called by (container) Mapping objects to request the creation of a join table.
     * If the specified field doesn't require a join table then this returns null.
     * If the join table already exists, then this returns it.
     * @param ownerTable The table that owns this member.
     * @param mmd The metadata describing the field/property.
     * @param clr The ClassLoaderResolver
     * @return The table (SetTable/ListTable/MapTable/ArrayTable)
     */
    public Table newJoinTable(Table ownerTable, AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        if (mmd.getJoinMetaData() == null)
        {
            AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
            if (relatedMmds != null && relatedMmds[0].getJoinMetaData() != null)
            {
                // Join specified at other end of a bidirectional relation so create a join table
            }
            else
            {
                Class element_class;
                if (mmd.hasCollection())
                {
                    element_class = clr.classForName(mmd.getCollection().getElementType());
                }
                else if (mmd.hasMap())
                {
                    MapMetaData mapmd = (MapMetaData)mmd.getContainer();
                    if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getMappedBy() != null)
                    {
                        // value stored in the key table
                        element_class = clr.classForName(mapmd.getKeyType());
                    }
                    else if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getMappedBy() != null)
                    {
                        // key stored in the value table
                        element_class = clr.classForName(mapmd.getValueType());
                    }
                    else
                    {
                        // No information given for what is stored in what, so throw it back to the user to fix the input :-)
                        throw new NucleusUserException(Localiser.msg("050050", mmd.getFullFieldName()));
                    }
                }
                else if (mmd.hasArray())
                {
                    element_class = clr.classForName(mmd.getTypeName()).getComponentType();
                }
                else
                {
                    // N-1 using join table ?
                    // what is this? should not happen
                    return null;
                }

                // Check that the element class has MetaData
                if (getMetaDataManager().getMetaDataForClass(element_class, clr) != null)
                {
                    // FK relationship, so no join table
                    return null;
                }
                else if (ClassUtils.isReferenceType(element_class))
                {
                    // reference type using FK relationship so no join table
                    return null;
                }

                // Trap all non-PC elements that haven't had a join table specified but need one
                throw new NucleusUserException(Localiser.msg("050049", mmd.getFullFieldName(), mmd.toString()));
            }
        }

        // Check if the join table already exists
        Table joinTable = getTable(mmd);
        if (joinTable != null)
        {
            return joinTable;
        }

        // Create a new join table for the container
        if (classAdder == null)
        {
            throw new IllegalStateException(Localiser.msg("050016"));
        }

        if (mmd.getType().isArray())
        {
            // Use Array table for array types
            return classAdder.addJoinTableForContainer(ownerTable, mmd, clr, ClassAdder.JOIN_TABLE_ARRAY);
        }
        else if (Map.class.isAssignableFrom(mmd.getType()))
        {
            // Use Map join table for supported map types
            return classAdder.addJoinTableForContainer(ownerTable, mmd, clr, ClassAdder.JOIN_TABLE_MAP);
        }
        else if (Collection.class.isAssignableFrom(mmd.getType()))
        {
            // Use Collection join table for collection/set types
            return classAdder.addJoinTableForContainer(ownerTable, mmd, clr, ClassAdder.JOIN_TABLE_COLLECTION);
        }
        else
        {
            // N-1 uni join
            return classAdder.addJoinTableForContainer(ownerTable, mmd, clr, ClassAdder.JOIN_TABLE_PERSISTABLE);
        }
    }

    public void registerTableInitialized(Table table)
    {
        if (classAdder != null)
        {
            classAdder.tablesRecentlyInitialized.add(table);
        }
    }

    /**
     * A schema transaction that adds a set of classes to the RDBMSManager, making them usable for persistence.
     * <p>
     * This class embodies the work necessary to activate a persistent class and ready it for storage management. 
     * It is the primary mutator of a RDBMSManager.
     * </p>
     * <p>
     * Adding classes is an involved process that includes the creation and/or validation in the database of tables, views, and table constraints, 
     * and their corresponding Java objects maintained by the RDBMSManager. Since it's a management transaction, the entire process is subject to 
     * retry on SQLExceptions. It is responsible for ensuring that the procedure either adds <i>all </i> of the requested classes successfully, 
     * or adds none of them and preserves the previous state of the RDBMSManager exactly as it was.
     * </p>
     */
    private class ClassAdder extends AbstractSchemaTransaction
    {
        /** join table for Collection. **/
        public static final int JOIN_TABLE_COLLECTION = 1;
        /** join table for Map. **/
        public static final int JOIN_TABLE_MAP = 2;
        /** join table for Array. **/
        public static final int JOIN_TABLE_ARRAY = 3;
        /** join table for persistable. **/
        public static final int JOIN_TABLE_PERSISTABLE = 4;

        /** Optional writer to dump the DDL for any classes being added. */
        private Writer ddlWriter = null;

        /** Whether to check if table/view exists */
        private final boolean checkExistTablesOrViews;

        /** tracks the SchemaData currrently being added - used to rollback the AutoStart added classes **/
        private Set<RDBMSStoreData> schemaDataAdded = new HashSet();

        private final String[] classNames;

        private List<Table> tablesRecentlyInitialized = new ArrayList();

        /**
         * Constructs a new class adder transaction that will add the given classes to the RDBMSManager.
         * @param classNames Names of the (initial) class(es) to be added.
         * @param writer Optional writer for DDL when we want the DDL outputting to file instead of creating the tables
         */
        private ClassAdder(String[] classNames, Writer writer)
        {
            super(RDBMSStoreManager.this, dba.getTransactionIsolationForSchemaCreation());
            this.ddlWriter = writer;
            this.classNames = classNames;

            checkExistTablesOrViews = RDBMSStoreManager.this.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CHECK_EXISTS_TABLES_VIEWS);
        }

        /**
         * Method to give a string version of this object.
         * @return The String version of this object.
         */
        public String toString()
        {
            return Localiser.msg("050038", catalogName, schemaName);
        }

        /**
         * Method to perform the class addition.
         * @param clr the ClassLoaderResolver
         * @throws SQLException Thrown if an error occurs in execution.
         */
        protected void run(ClassLoaderResolver clr)
        throws SQLException
        {
            if (classNames == null || classNames.length == 0)
            {
                return;
            }

            try
            {
                schemaLock.writeLock().lock();

                classAdder = this;
                try
                {
                    /*
                     * Adds a new table object (ie ClassTable or ClassView) for every class in the given list that 
                     * 1) requires an extent and 2) does not yet have an extent (ie table) initialized in the store manager.
                     * After all of the table objects, including any other tables they might reference, have been added, 
                     * each table is initialized and validated in the database.
                     * If any error occurs along the way, any table(s) that were created are dropped and the state of the 
                     * RDBMSManager is rolled back to the point at which this method was called.
                     */
                    storeDataMgr.begin();
                    boolean completed = false;

                    List tablesCreated = null;
                    List tableConstraintsCreated = null;
                    List viewsCreated = null;

                    try
                    {
                        List autoCreateErrors = new ArrayList();

                        // Add SchemaData entries and tables/views for the requested classes - not yet initialized
                        addClassTables(classNames, clr);

                        // Initialise all tables/views for the classes
                        List<Table>[] toValidate = initializeClassTables(classNames, clr);

                        if (!performingDeleteSchemaForClasses)
                        {
                            // Not performing delete of schema for classes so create in datastore if required
                            if (toValidate[0] != null && toValidate[0].size() > 0)
                            {
                                // Validate the tables
                                List[] result = performTablesValidation(toValidate[0], clr);
                                tablesCreated = result[0];
                                tableConstraintsCreated = result[1];
                                autoCreateErrors = result[2];
                            }

                            if (toValidate[1] != null && toValidate[1].size() > 0)
                            {
                                // Validate the views
                                List[] result = performViewsValidation(toValidate[1]);
                                viewsCreated = result[0];
                                autoCreateErrors.addAll(result[1]);
                            }

                            // Process all errors from the above
                            if (autoCreateErrors.size() > 0)
                            {
                                // Verify the list of errors, log the errors and raise NucleusDataStoreException when fail on error is enabled.
                                Iterator errorsIter = autoCreateErrors.iterator();
                                while (errorsIter.hasNext())
                                {
                                    Throwable exc = (Throwable)errorsIter.next();
                                    if (rdbmsMgr.getSchemaHandler().isAutoCreateWarnOnError())
                                    {
                                        NucleusLogger.DATASTORE.warn(Localiser.msg("050044", exc));
                                    }
                                    else
                                    {
                                        NucleusLogger.DATASTORE.error(Localiser.msg("050044", exc));
                                    }
                                }
                                if (!rdbmsMgr.getSchemaHandler().isAutoCreateWarnOnError())
                                {
                                    throw new NucleusDataStoreException(Localiser.msg("050043"), (Throwable[])autoCreateErrors.toArray(new Throwable[autoCreateErrors.size()]));
                                }
                            }
                        }

                        completed = true;
                    }
                    catch (SQLException sqle)
                    {
                        String msg = Localiser.msg("050044", sqle);
                        NucleusLogger.DATASTORE_SCHEMA.error(msg);
                        throw new NucleusDataStoreException(msg, sqle);
                    }
                    catch (Exception e)
                    {
                        if (NucleusException.class.isAssignableFrom(e.getClass()))
                        {
                            throw (NucleusException)e;
                        }
                        NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("050044", e));
                        throw new NucleusException(e.toString(), e).setFatal();
                    }
                    finally
                    {
                        // If something went wrong, roll things back to the way they were before we started.
                        // This may not restore the database 100% of the time (if DDL statements are not transactional) 
                        // but it will always put the RDBMSManager's internal structures back the way they were.
                        if (!completed)
                        {
                            storeDataMgr.rollback();
                            rollbackSchemaCreation(viewsCreated,tableConstraintsCreated,tablesCreated);
                        }
                        else
                        {
                            storeDataMgr.commit();
                        }
                        schemaDataAdded.clear();
                    }
                }
                finally
                {
                    classAdder = null;
                }
            }
            finally
            {
                schemaLock.writeLock().unlock();
            }
        }

        private int addClassTablesRecursionCounter = 0;

        /**
         * Adds a new table object (ie ClassTable or ClassView) for every class in the given list. These classes
         * <ol>
         * <li>require a table</li>
         * <li>do not yet have a table initialized in the store manager.</li>
         * </ol>
         * <p>
         * This doesn't initialize or validate the tables, it just adds the table objects to the RDBMSManager's internal data structures.
         * </p>
         * @param classNames Names of class(es) whose tables are to be added.
         * @param clr the ClassLoaderResolver
         */
        public void addClassTables(String[] classNames, ClassLoaderResolver clr)
        {
            addClassTablesRecursionCounter += 1;
            try
            {
                Iterator iter = getMetaDataManager().getReferencedClasses(classNames, clr).iterator();
                AutoStartMechanism starter = rdbmsMgr.getNucleusContext().getAutoStartMechanism();
                try
                {
                    if (starter != null && !starter.isOpen())
                    {
                        starter.open();
                    }

                    // Pass through the classes and create necessary tables
                    while (iter.hasNext())
                    {
                        addClassTable((ClassMetaData) iter.next(), clr);
                    }

                    // For data where the table wasn't defined, make a second pass.
                    // This is necessary where a subclass uses "superclass-table" and the superclass' table
                    // hadn't been defined at the point of adding this class
                    Iterator<RDBMSStoreData> addedIter = new HashSet(this.schemaDataAdded).iterator();
                    while (addedIter.hasNext())
                    {
                        RDBMSStoreData data = addedIter.next();
                        if (data.getTable() == null && data.isFCO())
                        {
                            AbstractClassMetaData cmd = (AbstractClassMetaData) data.getMetaData();
                            InheritanceMetaData imd = cmd.getInheritanceMetaData();
                            if (imd.getStrategy() == InheritanceStrategy.SUPERCLASS_TABLE)
                            {
                                AbstractClassMetaData[] managingCmds = getClassesManagingTableForClass(cmd, clr);
                                DatastoreClass superTable = null;
                                if (managingCmds != null && managingCmds.length == 1)
                                {
                                    RDBMSStoreData superData = (RDBMSStoreData) storeDataMgr.get(managingCmds[0].getFullClassName());

                                    // Assert that managing class is in the set of storeDataByClass
                                    if (superData == null)
                                    {
                                        this.addClassTables(new String[]{managingCmds[0].getFullClassName()}, clr);
                                        superData = (RDBMSStoreData) storeDataMgr.get(managingCmds[0].getFullClassName());
                                    }
                                    if (superData == null)
                                    {
                                        String msg = Localiser.msg("050013", cmd.getFullClassName());
                                        NucleusLogger.PERSISTENCE.error(msg);
                                        throw new NucleusUserException(msg);
                                    }
                                    superTable = (DatastoreClass) superData.getTable();
                                    data.setDatastoreContainerObject(superTable);
                                }
                            }
                        }
                    }
                }
                finally
                {
                    if (starter != null && starter.isOpen() && addClassTablesRecursionCounter <= 1)
                    {
                        starter.close();
                    }
                }
            }
            finally
            {
                addClassTablesRecursionCounter -= 1;
            }
        }

        /**
         * Method to add a new table object (ie ClassTable or ClassView).
         * Doesn't initialize or validate the tables, just adding the table objects to the internal data structures.
         * @param cmd the ClassMetaData
         * @param clr the ClassLoaderResolver
         */
        private void addClassTable(ClassMetaData cmd, ClassLoaderResolver clr)
        {
            // Only add tables for "PERSISTENCE_CAPABLE" classes
            if (cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_CAPABLE)
            {
                return;
            }
            else if (cmd.getIdentityType() == IdentityType.NONDURABLE)
            {
                if (cmd.hasExtension(METADATA_NONDURABLE_REQUIRES_TABLE) && cmd.getValueForExtension(METADATA_NONDURABLE_REQUIRES_TABLE) != null && 
                    cmd.getValueForExtension(METADATA_NONDURABLE_REQUIRES_TABLE).equalsIgnoreCase("false"))
                {
                    return;
                }
            }

            if (!storeDataMgr.managesClass(cmd.getFullClassName()))
            {
                // For application-identity classes with user-defined identities we check for use of the 
                // objectid-class in different inheritance trees. We prevent this to avoid problems later on.
                // The builtin objectid-classes are allowed to be duplicated.
                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    if (!cmd.usesSingleFieldIdentityClass())
                    {
                        // Check whether this class has the same base persistable class as the others using the PK. 
                        // If not, then throw an error
                        String baseClassWithMetaData = cmd.getBaseAbstractClassMetaData().getFullClassName();
                        Collection<AbstractClassMetaData> pkCmds = getMetaDataManager().getClassMetaDataWithApplicationId(cmd.getObjectidClass());
                        if (pkCmds != null && pkCmds.size() > 0)
                        {
                            // We already have at least 1 class using the same app id PK class
                            // so check if it is has the same persistable root class.
                            boolean in_same_tree = false;
                            String sample_class_in_other_tree = null;

                            Iterator<AbstractClassMetaData> iter = pkCmds.iterator();
                            while (iter.hasNext())
                            {
                                AbstractClassMetaData pkCmd = iter.next();
                                String otherClassBaseClass = pkCmd.getBaseAbstractClassMetaData().getFullClassName();
                                if (otherClassBaseClass.equals(baseClassWithMetaData))
                                {
                                    in_same_tree = true;
                                    break;
                                }
                                sample_class_in_other_tree = pkCmd.getFullClassName();
                            }

                            if (!in_same_tree)
                            {
                                String error_msg = Localiser.msg("050021", cmd.getFullClassName(), cmd.getObjectidClass(), sample_class_in_other_tree);
                                NucleusLogger.DATASTORE.error(error_msg);
                                throw new NucleusUserException(error_msg);
                            }
                        }
                    }
                }

                if (cmd.isEmbeddedOnly())
                {
                    // Nothing to do. Only persisted as SCO.
                    NucleusLogger.DATASTORE.debug(Localiser.msg("032012", cmd.getFullClassName()));
                }
                else
                {
                    InheritanceMetaData imd = cmd.getInheritanceMetaData();
                    RDBMSStoreData sdNew = null;
                    if (imd.getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
                    {
                        // Table mapped into the table(s) of subclass(es)
                        // Just add the SchemaData entry with no table - managed by subclass
                        sdNew = new RDBMSStoreData(cmd, null, false);
                        registerStoreData(sdNew);
                    }
                    else if (imd.getStrategy() == InheritanceStrategy.COMPLETE_TABLE && cmd.isAbstract())
                    {
                        // Abstract class with "complete-table" so gets no table
                        sdNew = new RDBMSStoreData(cmd, null, false);
                        registerStoreData(sdNew);
                    }
                    else if (imd.getStrategy() == InheritanceStrategy.NEW_TABLE || imd.getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
                    {
                        // Table managed by this class
                        // Generate an identifier for the table required
                        DatastoreIdentifier tableName = null;
                        RDBMSStoreData tmpData = (RDBMSStoreData) storeDataMgr.get(cmd.getFullClassName());
                        if (tmpData != null && tmpData.getDatastoreIdentifier() != null)
                        {
                            tableName = tmpData.getDatastoreIdentifier();
                        }
                        else
                        {
                            tableName = rdbmsMgr.getIdentifierFactory().newTableIdentifier(cmd);
                        }

                        // Check that the required table isn't already in use
                        StoreData[] existingStoreData = getStoreDataForDatastoreContainerObject(tableName);
                        if (existingStoreData != null)
                        {
                            String existingClass = null;
                            for (int j=0;j<existingStoreData.length;j++)
                            {
                                if (!existingStoreData[j].getName().equals(cmd.getFullClassName()))
                                {
                                    existingClass = existingStoreData[j].getName();
                                    break;
                                }
                            }
                            if (existingClass != null)
                            {
                                // Give a warning that will create a new instance of the table (mapped to the same datastore object)
                                NucleusLogger.DATASTORE.warn(Localiser.msg("050015", cmd.getFullClassName(), tableName.getName(), existingClass));
                            }
                        }

                        // Create the table to use for this class
                        DatastoreClass t = null;
                        boolean hasViewDef = false;
                        if (dba.getVendorID() != null)
                        {
                            hasViewDef = cmd.hasExtension(MetaData.EXTENSION_CLASS_VIEW_DEFINITION + '-' + dba.getVendorID());
                        }
                        if (!hasViewDef)
                        {
                            hasViewDef = cmd.hasExtension(MetaData.EXTENSION_CLASS_VIEW_DEFINITION);
                        }
                        if (hasViewDef)
                        {
                            t = new ClassView(tableName, RDBMSStoreManager.this, cmd);
                        }
                        else
                        {
                            t = new ClassTable(tableName, RDBMSStoreManager.this, cmd);
                        }

                        sdNew = new RDBMSStoreData(cmd, t, true);
                        rdbmsMgr.registerStoreData(sdNew);

                        // must be initialized after registering, to avoid StackOverflowError
                        ((Table)t).preInitialize(clr);
                    }
                    else if (imd.getStrategy() == InheritanceStrategy.SUPERCLASS_TABLE)
                    {
                        // Table mapped into table of superclass
                        // Find the superclass - should have been created first
                        AbstractClassMetaData[] managingCmds = getClassesManagingTableForClass(cmd, clr);
                        Table superTable = null;
                        if (managingCmds != null && managingCmds.length == 1)
                        {
                            RDBMSStoreData superData = (RDBMSStoreData) storeDataMgr.get(managingCmds[0].getFullClassName());
                            if (superData != null)
                            {
                                // Specify the table if it already exists
                                superTable = (Table) superData.getTable();
                            }
                            sdNew = new RDBMSStoreData(cmd, superTable, false);
                            rdbmsMgr.registerStoreData(sdNew);
                        }
                        else
                        {
                            String msg = Localiser.msg("050013", cmd.getFullClassName());
                            NucleusLogger.PERSISTENCE.error(msg);
                            throw new NucleusUserException(msg);
                        }
                    }
                    schemaDataAdded.add(sdNew);
                }
            }
        }

        /**
         * Initialisation of tables. Updates the internal representation of the table to match what is 
         * required for the class(es). Each time a table object is initialized, it may cause other associated 
         * table objects to be added (via callbacks to addClasses()) so the loop is repeated until no more 
         * initialisation is needed.
         * @param classNames String array of class names
         * @param clr the ClassLoaderResolver
         * @return an array of List where index == 0 is list of the tables created, index == 1 is list of the views created
         */
        private List<Table>[] initializeClassTables(String[] classNames, ClassLoaderResolver clr)
        {
            List<Table> tablesToValidate = new ArrayList();
            List<Table> viewsToValidate = new ArrayList();

            tablesRecentlyInitialized.clear();
            int numTablesInitializedInit = 0;
            int numStoreDataInit = 0;
            RDBMSStoreData[] rdbmsStoreData = storeDataMgr.getManagedStoreData().toArray(new RDBMSStoreData[storeDataMgr.size()]);
            do
            {
                numStoreDataInit = rdbmsStoreData.length;
                numTablesInitializedInit = tablesRecentlyInitialized.size();
                for (int i=0; i<rdbmsStoreData.length; i++)
                {
                    RDBMSStoreData currentStoreData = rdbmsStoreData[i];
                    if (currentStoreData.hasTable())
                    {
                        // Class has a table so we have work to do
                        Table t = (Table) currentStoreData.getTable();
                        if (t instanceof DatastoreClass)
                        {
                            ((RDBMSPersistenceHandler)rdbmsMgr.getPersistenceHandler()).removeRequestsForTable((DatastoreClass)t);
                        }

                        if (!t.isInitialized())
                        {
                            // Initialize this table. This may trigger the initialisation of any supertables as required
                            t.initialize(clr);
                        }

                        if (!currentStoreData.isTableOwner() && !((ClassTable)t).managesClass(currentStoreData.getName()))
                        {
                            // Table maybe already exist, but this class is stored there so is possibly adding columns
                            ((ClassTable)t).manageClass((ClassMetaData)currentStoreData.getMetaData(), clr);
                            if (!tablesToValidate.contains(t))
                            {
                                tablesToValidate.add(t);
                            }
                        }
                    }
                }

                rdbmsStoreData = storeDataMgr.getManagedStoreData().toArray(new RDBMSStoreData[storeDataMgr.size()]);
            }
            while ((tablesRecentlyInitialized.size() > numTablesInitializedInit) || (rdbmsStoreData.length > numStoreDataInit));

            // Post initialisation of tables
            for (int j=0; j<tablesRecentlyInitialized.size(); j++)
            {
                tablesRecentlyInitialized.get(j).postInitialize(clr);
            }

            for (Table t : tablesRecentlyInitialized)
            {
                if (t instanceof ViewImpl)
                {
                    viewsToValidate.add(t);
                }
                else
                {
                    if (!tablesToValidate.contains(t))
                    {
                        tablesToValidate.add(t);
                    }
                }
            }
            return new List[] { tablesToValidate, viewsToValidate };
        }

        /**
         * Validate tables.
         * @param tablesToValidate list of TableImpl to validate
         * @param clr the ClassLoaderResolver
         * @return an array of List where index == 0 has a list of the tables created, index == 1 has a list of the contraints created, index == 2 has a list of the auto creation errors 
         * @throws SQLException When an error occurs in validation
         */
        private List[] performTablesValidation(List<Table> tablesToValidate, ClassLoaderResolver clr) 
        throws SQLException
        {
            List autoCreateErrors = new ArrayList();
            List<Table> tableConstraintsCreated = new ArrayList();
            List<Table> tablesCreated = new ArrayList();

            if (ddlWriter != null) // TODO Why is this only done with the DDL option enabled?
            {
                // Remove any existence of the same actual table more than once so we dont duplicate its
                // DDL for creation. Note that this will allow more than once instance of tables with the same
                // name (identifier) since when you have multiple inheritance trees each inheritance tree
                // will have its own ClassTable, and you want both of these to pass through to schema generation.
                List<Table> tmpTablesToValidate = new ArrayList();
                for (Table tbl : tablesToValidate)
                {
                    // This just checks the identifier name - see hashCode of Table
                    if (!tmpTablesToValidate.contains(tbl))
                    {
                        tmpTablesToValidate.add(tbl);
                    }
                }
                tablesToValidate = tmpTablesToValidate;
            }

            // Table existence and validation.
            // a). Check for existence of the table
            // b). If autocreate, create the table if necessary
            // c). If validate, validate the table
            Iterator i = tablesToValidate.iterator();
            while (i.hasNext())
            {
                TableImpl t = (TableImpl) i.next();

                boolean columnsValidated = false;
                boolean columnsInitialised = false;
                if (checkExistTablesOrViews)
                {
                    if (ddlWriter != null)
                    {
                        try
                        {
                            if (t instanceof ClassTable)
                            {
                                ddlWriter.write("-- Table " + t.toString() + " for classes " + StringUtils.objectArrayToString(((ClassTable)t).getManagedClasses()) + "\n");
                            }
                            else if (t instanceof JoinTable)
                            {
                                ddlWriter.write("-- Table " + t.toString() + " for join relationship\n");
                            }
                        }
                        catch (IOException ioe)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("error writing DDL into file for table " + t, ioe);
                        }
                    }

                    if (!tablesCreated.contains(t) && t.exists(getCurrentConnection(), rdbmsMgr.getSchemaHandler().isAutoCreateTables()))
                    {
                        // Table has been created so add to our list so we don't process it multiple times
                        // Any subsequent instance of this table in the list will have the columns checked only
                        tablesCreated.add(t);
                        columnsValidated = true;
                    }
                    else
                    {
                        // Table wasn't just created, so do any autocreate of columns necessary
                        if (t.isInitializedModified() || rdbmsMgr.getSchemaHandler().isAutoCreateColumns())
                        {
                            // Check for existence of the required columns and add where required
                            t.validateColumns(getCurrentConnection(), false, rdbmsMgr.getSchemaHandler().isAutoCreateColumns(), autoCreateErrors);
                            columnsValidated = true;
                        }
                    }
                }

                if (rdbmsMgr.getSchemaHandler().isValidateTables() && !columnsValidated) // Table not just created and validation requested
                {
                    // Check down to the column structure where required
                    t.validate(getCurrentConnection(), rdbmsMgr.getSchemaHandler().isValidateColumns(), false, autoCreateErrors);
                    columnsInitialised = rdbmsMgr.getSchemaHandler().isValidateColumns();
                }

                if (!columnsInitialised)
                {
                    // Allow initialisation of the column information TODO Arguably we should always do this
                    String initInfo = getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_INIT_COLUMN_INFO);
                    if (initInfo.equalsIgnoreCase("PK"))
                    {
                        // Initialise the PK columns only
                        t.initializeColumnInfoForPrimaryKeyColumns(getCurrentConnection());
                    }
                    else if (initInfo.equalsIgnoreCase("ALL"))
                    {
                        // Initialise all columns
                        t.initializeColumnInfoFromDatastore(getCurrentConnection());
                    }
                }

                // Discard any cached column info used to validate the table
                invalidateColumnInfoForTable(t);
            }

            // Table constraint existence and validation
            // a). Check for existence of the constraint
            // b). If autocreate, create the constraint if necessary
            // c). If validate, validate the constraint
            // Constraint processing is done as a separate step from table processing
            // since the constraints are dependent on tables being available
            i = tablesToValidate.iterator();
            while (i.hasNext())
            {
                TableImpl t = (TableImpl) i.next();
                if (rdbmsMgr.getSchemaHandler().isValidateConstraints() || rdbmsMgr.getSchemaHandler().isAutoCreateConstraints())
                {
                    if (ddlWriter != null)
                    {
                        try
                        {
                            if (t instanceof ClassTable)
                            {
                                ddlWriter.write("-- Constraints for table " + t.toString() + " for class(es) " + StringUtils.objectArrayToString(((ClassTable)t).getManagedClasses()) + "\n");
                            }
                            else
                            {
                                ddlWriter.write("-- Constraints for table " + t.toString() + "\n");
                            }
                        }
                        catch (IOException ioe)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("error writing DDL into file for table " + t, ioe);
                        }
                    }

                    // TODO : split this method into checkExistsConstraints and validateConstraints
                    // TODO : if duplicated entries on the list, we need to validate before.
                    if (tablesCreated.contains(t) && !hasDuplicateTablesFromList(tablesToValidate))
                    {
                        if (t.createConstraints(getCurrentConnection(), autoCreateErrors, clr))
                        {
                            tableConstraintsCreated.add(t);
                        }
                    }
                    else if (t.validateConstraints(getCurrentConnection(), rdbmsMgr.getSchemaHandler().isAutoCreateConstraints(), autoCreateErrors, clr))
                    {
                        tableConstraintsCreated.add(t);
                    }
                    if (ddlWriter != null)
                    {
                        try
                        {
                            ddlWriter.write("\n");
                        }
                        catch (IOException ioe)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("error writing DDL into file for table " + t, ioe);
                        }
                    }
                }
            }
            return new List[] { tablesCreated, tableConstraintsCreated, autoCreateErrors };
        }

        /**
         * Check if duplicated tables are in the list.
         * @param newTables the list of DatastoreContainerObject
         * @return true if duplicated tables are in the list
         */
        private boolean hasDuplicateTablesFromList(List<Table> newTables)
        {
            Map map = new HashMap();
            for (int i=0; i<newTables.size(); i++)
            {
                Table t1 = newTables.get(i);
                if (map.containsKey(t1.getIdentifier().getName()))
                {
                    return true;
                }
                map.put(t1.getIdentifier().getName(), t1);
            }
            return false;
        }

        /**
         * Validate the supplied views.
         * @param viewsToValidate list of ViewImpl to validate
         * @return an array of List where index == 0 has a list of the views created, index == 1 has a list of the auto creation errors 
         * @throws SQLException
         */
        private List[] performViewsValidation(List<Table> viewsToValidate) throws SQLException
        {
            // View existence and validation.
            // a). Check for existence of the view
            // b). If autocreate, create the view if necessary
            // c). If validate, validate the view
            List<Table> viewsCreated = new ArrayList();
            List autoCreateErrors = new ArrayList();
            Iterator i = viewsToValidate.iterator();
            while (i.hasNext())
            {
                ViewImpl v = (ViewImpl) i.next();
                if (checkExistTablesOrViews)
                {
                    if (v.exists(getCurrentConnection(), rdbmsMgr.getSchemaHandler().isAutoCreateTables()))
                    {
                        viewsCreated.add(v);
                    }
                }
                if (rdbmsMgr.getSchemaHandler().isValidateTables())
                {
                    v.validate(getCurrentConnection(), true, false, autoCreateErrors);
                }

                // Discard any cached column info used to validate the view
                invalidateColumnInfoForTable(v);
            }
            return new List[] { viewsCreated, autoCreateErrors };
        }

        /**
         * Rollback / Compensate schema creation by dropping tables, views, constraints and
         * deleting entries in the auto start mechanism.
         * @param viewsCreated the views created that must be dropped
         * @param tableConstraintsCreated the constraints created that must be dropped
         * @param tablesCreated the tables created that must be dropped
         */
        private void rollbackSchemaCreation(List<Table> viewsCreated, List<Table> tableConstraintsCreated, List<Table> tablesCreated)
        {
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("050040"));
            }

            // Tables, table constraints, and views get removed in the reverse order from which they were created.
            try
            {
                if (viewsCreated != null)
                {
                    ListIterator li = viewsCreated.listIterator(viewsCreated.size());
                    while (li.hasPrevious())
                    {
                        ((ViewImpl) li.previous()).drop(getCurrentConnection());
                    }
                }
                if( tableConstraintsCreated != null)
                {
                    ListIterator li = tableConstraintsCreated.listIterator(tableConstraintsCreated.size());
                    while (li.hasPrevious())
                    {
                        ((TableImpl) li.previous()).dropConstraints(getCurrentConnection());
                    }
                }
                if (tablesCreated != null)
                {
                    ListIterator li = tablesCreated.listIterator(tablesCreated.size());
                    while (li.hasPrevious())
                    {
                        ((TableImpl) li.previous()).drop(getCurrentConnection());
                    }
                }
            }
            catch (Exception e)
            {
                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("050041", e));
            }

            // AutoStarter - Remove all classes from the supported list that were added in this pass.
            AutoStartMechanism starter = rdbmsMgr.getNucleusContext().getAutoStartMechanism();
            if (starter != null)
            {
                try
                {
                    if (!starter.isOpen())
                    {
                        starter.open();
                    }
                    Iterator<RDBMSStoreData> schema_added_iter = schemaDataAdded.iterator();
                    while (schema_added_iter.hasNext())
                    {
                        RDBMSStoreData sd = schema_added_iter.next();
                        starter.deleteClass(sd.getName());
                    }                            
                }
                finally
                {
                    if (starter.isOpen())
                    {
                        starter.close();
                    }
                }
            }
        }

        /**
         * Called by Mapping objects in the midst of RDBMSManager.addClasses()
         * to request the creation of a join table to hold a containers' contents.
         * @param ownerTable Table of the owner of this member
         * @param mmd The member metadata for this member
         * @param type The type of the join table
         */
        private Table addJoinTableForContainer(Table ownerTable, AbstractMemberMetaData mmd, ClassLoaderResolver clr, int type)
        {
            DatastoreIdentifier tableName = null;
            RDBMSStoreData sd = (RDBMSStoreData) storeDataMgr.get(mmd);
            if (sd != null && sd.getDatastoreIdentifier() != null)
            {
                tableName = sd.getDatastoreIdentifier();
            }
            else
            {
                tableName = identifierFactory.newTableIdentifier(mmd);
            }

            Table join = null;
            if (type == JOIN_TABLE_COLLECTION)
            {
                join = new CollectionTable(ownerTable, tableName, mmd, RDBMSStoreManager.this);
            }
            else if (type == JOIN_TABLE_MAP)
            {
                join = new MapTable(ownerTable, tableName, mmd, RDBMSStoreManager.this);
            }
            else if (type == JOIN_TABLE_ARRAY)
            {
                join = new ArrayTable(ownerTable, tableName, mmd, RDBMSStoreManager.this);
            }
            else if (type == JOIN_TABLE_PERSISTABLE)
            {
                join = new PersistableJoinTable(ownerTable, tableName, mmd, RDBMSStoreManager.this);
            }

            AutoStartMechanism starter = rdbmsMgr.getNucleusContext().getAutoStartMechanism();
            try
            {
                if (starter != null && !starter.isOpen())
                {
                    starter.open();
                }

                RDBMSStoreData data = new RDBMSStoreData(mmd, join);
                schemaDataAdded.add(data);
                rdbmsMgr.registerStoreData(data);
            }
            finally
            {
                if (starter != null && starter.isOpen())
                {
                    starter.close();
                }
            }

            return join;
        }
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_APPLICATION_COMPOSITE_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_NONDURABLE_ID);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_SECONDARY_TABLE);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_COLLECTION);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_MAP);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_ARRAY);
        set.add(StoreManager.OPTION_ORM_FOREIGN_KEYS);
        set.add(StoreManager.OPTION_ORM_SERIALISED_PC);
        set.add(StoreManager.OPTION_ORM_SERIALISED_COLLECTION_ELEMENT);
        set.add(StoreManager.OPTION_ORM_SERIALISED_ARRAY_ELEMENT);
        set.add(StoreManager.OPTION_ORM_SERIALISED_MAP_KEY);
        set.add(StoreManager.OPTION_ORM_SERIALISED_MAP_VALUE);

        // Add isolation levels for this database adapter
        if (dba.supportsOption(DatastoreAdapter.TX_ISOLATION_READ_COMMITTED))
        {
            set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        }
        if (dba.supportsOption(DatastoreAdapter.TX_ISOLATION_READ_UNCOMMITTED))
        {
            set.add(StoreManager.OPTION_TXN_ISOLATION_READ_UNCOMMITTED);
        }
        if (dba.supportsOption(DatastoreAdapter.TX_ISOLATION_REPEATABLE_READ))
        {
            set.add(StoreManager.OPTION_TXN_ISOLATION_REPEATABLE_READ);
        }
        if (dba.supportsOption(DatastoreAdapter.TX_ISOLATION_SERIALIZABLE))
        {
            set.add(StoreManager.OPTION_TXN_ISOLATION_SERIALIZABLE);
        }

        // Query Cancel and Datastore Timeout is supported on JDOQL for RDBMS (unless turned off by user)
        set.add(StoreManager.OPTION_QUERY_CANCEL);
        set.add(StoreManager.OPTION_DATASTORE_TIMEOUT);
        if (dba.supportsOption(DatastoreAdapter.DATETIME_STORES_MILLISECS))
        {
            set.add(StoreManager.OPTION_DATASTORE_TIME_STORES_MILLISECS);
            // TODO What about nanosecs
        }

        // JDOQL Bitwise are only supported if supported by the datastore
        if (dba.supportsOption(DatastoreAdapter.OPERATOR_BITWISE_AND))
        {
            set.add(StoreManager.OPTION_QUERY_JDOQL_BITWISE_OPS);
        }
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_INSERT);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_UPDATE);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_DELETE);
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_UPDATE);
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_DELETE);

        return set;
    }

    /**
     * Accessor for whether this mapping requires values inserting on an INSERT.
     * @param datastoreMapping The datastore mapping
     * @return Whether values are to be inserted into this mapping on an INSERT
     */
    public boolean insertValuesOnInsert(DatastoreMapping datastoreMapping)
    {
        return ((AbstractDatastoreMapping)datastoreMapping).insertValuesOnInsert();
    }

    /**
     * Convenience method to return if the datastore supports batching and the user wants batching.
     * @return If batching of statements is permissible
     */
    public boolean allowsBatching()
    {
        return dba.supportsOption(DatastoreAdapter.STATEMENT_BATCHING) && getIntProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STATEMENT_BATCH_LIMIT) != 0;
    }

    public boolean usesBackedSCOWrappers()
    {
        return true;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#useBackedSCOWrapperForMember(org.datanucleus.metadata.AbstractMemberMetaData, org.datanucleus.store.ExecutionContext)
     */
    @Override
    public boolean useBackedSCOWrapperForMember(AbstractMemberMetaData mmd, ExecutionContext ec)
    {
        if ((mmd.hasCollection() || mmd.hasMap()) && mmd.hasExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME))
        {
            // The only case where we don't use backed wrappers is for a Collection/Map that is using a converter for the whole field (so storing as a single column)
            return false;
        }

        return true;
    }

    /**
     * Accessor for the Calendar to be used in handling all timezone issues with the datastore.
     * Utilises the "serverTimeZoneID" in providing this Calendar used in time/date conversions.
     * @return The calendar to use for dateTimezone issues.
     */
    public Calendar getCalendarForDateTimezone()
    {
        if (dateTimezoneCalendar == null)
        {
            TimeZone tz;
            String serverTimeZoneID = getStringProperty(PropertyNames.PROPERTY_SERVER_TIMEZONE_ID);
            if (serverTimeZoneID != null)
            {
                tz = TimeZone.getTimeZone(serverTimeZoneID);
            }
            else
            {
                tz = TimeZone.getDefault();
            }
            dateTimezoneCalendar = new GregorianCalendar(tz);
        }
        // This returns a clone because Oracle JDBC driver was taking the Calendar and modifying it
        // in calls. Hence passing a clone gets around that. May be best to just return it direct here
        // and then in Oracle usage we pass in a clone to its JDBC driver
        return (Calendar) dateTimezoneCalendar.clone();
    }

    // ---------------------------------------SchemaTool------------------------------------------------

    public void createDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.createDatabase(catalogName, schemaName, props, null);
    }

    public void deleteDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.deleteDatabase(catalogName, schemaName, props, null);
    }

    public void createSchemaForClasses(Set<String> inputClassNames, Properties props)
    {
        Set<String> classNames = cleanInputClassNames(nucleusContext, inputClassNames);

        String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
        String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
        boolean completeDdl = completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true");
        String autoStartProp = props != null ? props.getProperty("autoStartTable") : null;
        boolean autoStart = autoStartProp != null && autoStartProp.equalsIgnoreCase("true");

        if (classNames.size() > 0)
        {
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            FileWriter ddlFileWriter = null;
            try
            {
                if (ddlFilename != null)
                {
                    // Open the DDL file for writing
                    File ddlFile = StringUtils.getFileForFilename(ddlFilename);
                    if (ddlFile.exists())
                    {
                        // Delete existing file
                        ddlFile.delete();
                    }
                    if (ddlFile.getParentFile() != null && !ddlFile.getParentFile().exists())
                    {
                        // Make sure the directory exists
                        ddlFile.getParentFile().mkdirs();
                    }
                    ddlFile.createNewFile();
                    ddlFileWriter = new FileWriter(ddlFile);

                    SimpleDateFormat fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                    ddlFileWriter.write("-- ----------------------------------------------------------------\n");
                    ddlFileWriter.write("-- DataNucleus SchemaTool " + 
                        "(ran at " + fmt.format(new java.util.Date()) + ")\n");
                    ddlFileWriter.write("-- ----------------------------------------------------------------\n");
                    if (completeDdl)
                    {
                        ddlFileWriter.write("-- Complete schema required for the following classes:-\n");
                    }
                    else  
                    {
                        ddlFileWriter.write("-- Schema diff for " + getConnectionURL() + " and the following classes:-\n");
                    }
                    Iterator classNameIter = classNames.iterator();
                    while (classNameIter.hasNext())
                    {
                        ddlFileWriter.write("--     " + classNameIter.next() + "\n");
                    }
                    ddlFileWriter.write("--\n");
                }

                try
                {
                    if (ddlFileWriter != null)
                    {
                        this.ddlWriter = ddlFileWriter;
                        this.completeDDL = completeDdl;
                        this.writtenDdlStatements = new HashSet();
                    }

                    // Tables/constraints DDL
                    String[] classNamesArr = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames.toArray(new String[classNames.size()]));
                    if (classNamesArr.length > 0)
                    {
                        new ClassAdder(classNamesArr, ddlFileWriter).execute(clr);
                    }

                    if (autoStart)
                    {
                        // Generate the SchemaTable auto-starter table
                        if (ddlFileWriter != null)
                        {
                            try
                            {
                                ddlFileWriter.write("\n");
                                ddlFileWriter.write("-- ----------------------------------------------------------------\n");
                                ddlFileWriter.write("-- Table for SchemaTable auto-starter\n");
                            }
                            catch (IOException ioe)
                            {
                            }
                        }
                        new SchemaAutoStarter(this, clr);
                    }

                    if (ddlFileWriter != null)
                    {
                        this.ddlWriter = null;
                        this.completeDDL = false;
                        this.writtenDdlStatements.clear();
                        this.writtenDdlStatements = null;
                    }

                    // Sequences, ValueGenerator table
                    if (ddlFileWriter != null)
                    {
                        ddlFileWriter.write("\n");
                        ddlFileWriter.write("-- ----------------------------------------------------------------\n");
                        ddlFileWriter.write("-- Sequences and SequenceTables\n");
                    }
                    createSchemaSequences(classNames, clr, ddlFileWriter);
                }
                finally
                {
                    if (ddlFileWriter != null)
                    {
                        ddlFileWriter.close();
                    }
                }
            }
            catch (IOException ioe)
            {
                NucleusLogger.DATASTORE_SCHEMA.error("Exception thrown writing DDL file", ioe);
                // Error in writing DDL file
            }
        }
        else
        {
            String msg = Localiser.msg("014039");
            NucleusLogger.DATASTORE_SCHEMA.error(msg);
            System.out.println(msg);

            throw new NucleusException(msg);
        }
    }

    protected void createSchemaSequences(Set<String> classNames, ClassLoaderResolver clr, FileWriter ddlWriter)
    {
        // Check for datastore-based value-generator usage
        if (classNames != null && classNames.size() > 0)
        {
            Set<String> seqTablesGenerated = new HashSet();
            Set<String> sequencesGenerated = new HashSet();
            Iterator<String> classNameIter = classNames.iterator();
            while (classNameIter.hasNext())
            {
                String className = classNameIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd.getIdentityMetaData() != null && cmd.getIdentityMetaData().getValueStrategy() != null)
                {
                    if (cmd.getIdentityMetaData().getValueStrategy() == IdentityStrategy.INCREMENT)
                    {
                        addSequenceTableForMetaData(cmd.getIdentityMetaData(), clr, seqTablesGenerated);
                    }
                    else if (cmd.getIdentityMetaData().getValueStrategy() == IdentityStrategy.SEQUENCE)
                    {
                        String seqName = cmd.getIdentityMetaData().getSequence();
                        if (StringUtils.isWhitespace(seqName))
                        {
                            seqName = cmd.getIdentityMetaData().getValueGeneratorName();
                        }
                        if (!StringUtils.isWhitespace(seqName))
                        {
                            addSequenceForMetaData(cmd.getIdentityMetaData(), seqName, clr, sequencesGenerated, ddlWriter);
                        }
                    }
                }

                AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
                for (int j=0;j<mmds.length;j++)
                {
                    IdentityStrategy str = mmds[j].getValueStrategy();
                    if (str == IdentityStrategy.INCREMENT)
                    {
                        addSequenceTableForMetaData(mmds[j], clr, seqTablesGenerated);
                    }
                    else if (str == IdentityStrategy.SEQUENCE)
                    {
                        String seqName = mmds[j].getSequence();
                        if (StringUtils.isWhitespace(seqName))
                        {
                            seqName = mmds[j].getValueGeneratorName();
                        }
                        if (!StringUtils.isWhitespace(seqName))
                        {
                            addSequenceForMetaData(mmds[j], seqName, clr, sequencesGenerated, ddlWriter);
                        }
                    }
                }
            }
        }
    }

    protected void addSequenceTableForMetaData(MetaData md, ClassLoaderResolver clr, Set<String> seqTablesGenerated)
    {
        String catName = null;
        String schName = null;
        String tableName = TableGenerator.DEFAULT_TABLE_NAME;
        String seqColName = TableGenerator.DEFAULT_SEQUENCE_COLUMN_NAME;
        String nextValColName = TableGenerator.DEFAULT_NEXTVALUE_COLUMN_NAME;
        if (md.hasExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_CATALOG))
        {
            catName = md.getValueForExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_CATALOG);
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_SCHEMA))
        {
            schName = md.getValueForExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_SCHEMA);
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_TABLE))
        {
            tableName = md.getValueForExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_TABLE);
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_NAME_COLUMN))
        {
            seqColName = md.getValueForExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_NAME_COLUMN);
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_NEXTVAL_COLUMN))
        {
            nextValColName = md.getValueForExtension(ValueGenerator.PROPERTY_SEQUENCETABLE_NEXTVAL_COLUMN);
        }

        if (!seqTablesGenerated.contains(tableName))
        {
            ManagedConnection mconn = getConnection(TransactionIsolation.NONE);
            Connection conn = (Connection) mconn.getConnection();
            try
            {
                DatastoreIdentifier tableIdentifier = identifierFactory.newTableIdentifier(tableName);
                if (catName != null)
                {
                    tableIdentifier.setCatalogName(catName);
                }
                if (schName != null)
                {
                    tableIdentifier.setSchemaName(schName);
                }
                SequenceTable seqTable = new SequenceTable(tableIdentifier, this, seqColName, nextValColName);
                seqTable.initialize(clr);
                seqTable.exists(conn, true);
            }
            catch (Exception e)
            {
            }
            finally
            {
                mconn.release();
            }
            seqTablesGenerated.add(tableName);
        }
    }

    protected void addSequenceForMetaData(MetaData md, String seq,  ClassLoaderResolver clr, Set<String> sequencesGenerated, FileWriter ddlWriter)
    {
        String seqName = seq;
        Integer min = null;
        Integer max = null;
        Integer start = null;
        Integer increment = null;
        Integer cacheSize = null;

        SequenceMetaData seqmd = getMetaDataManager().getMetaDataForSequence(clr, seq);
        if (seqmd != null)
        {
            seqName = seqmd.getDatastoreSequence();
            if (seqmd.getAllocationSize() > 0)
            {
                increment = Integer.valueOf(seqmd.getAllocationSize());
            }
            if (seqmd.getInitialValue() >= 0)
            {
                start = Integer.valueOf(seqmd.getInitialValue());
            }
            md = seqmd;
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_KEY_MIN_VALUE))
        {
            min = Integer.valueOf(md.getValueForExtension(ValueGenerator.PROPERTY_KEY_MIN_VALUE));
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_KEY_MAX_VALUE))
        {
            max = Integer.valueOf(md.getValueForExtension(ValueGenerator.PROPERTY_KEY_MAX_VALUE));
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_KEY_CACHE_SIZE))
        {
            increment = Integer.valueOf(md.getValueForExtension(ValueGenerator.PROPERTY_KEY_CACHE_SIZE));
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE))
        {
            start = Integer.valueOf(md.getValueForExtension(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE));
        }
        if (md.hasExtension(ValueGenerator.PROPERTY_KEY_DATABASE_CACHE_SIZE))
        {
            cacheSize = Integer.valueOf(md.getValueForExtension(ValueGenerator.PROPERTY_KEY_DATABASE_CACHE_SIZE));
        }
        if (!sequencesGenerated.contains(seqName))
        {
            String stmt = getDatastoreAdapter().getSequenceCreateStmt(seqName, min, max, start, increment, cacheSize);
            if (ddlWriter != null)
            {
                try
                {
                    ddlWriter.write(stmt + ";\n");
                }
                catch (IOException ioe)
                {
                }
            }
            else
            {
                PreparedStatement ps = null;
                ManagedConnection mconn = getConnection(TransactionIsolation.NONE);
                try
                {
                    ps = sqlController.getStatementForUpdate(mconn, stmt, false);
                    sqlController.executeStatementUpdate(null, mconn, stmt, ps, true);
                }
                catch (SQLException e)
                {
                }
                finally
                {
                    try
                    {
                        if (ps != null)
                        {
                            sqlController.closeStatement(mconn, ps);
                        }
                    }
                    catch (SQLException e)
                    {
                    }
                    mconn.release();
                }
            }
            sequencesGenerated.add(seqName);
        }
    }

    boolean performingDeleteSchemaForClasses = false;

    public void deleteSchemaForClasses(Set<String> inputClassNames, Properties props)
    {
        Set<String> classNames = cleanInputClassNames(nucleusContext, inputClassNames);

        if (classNames.size() > 0)
        {
            // Delete the tables
            String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
            String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
            boolean completeDdl = completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true");
            String autoStartProp = props != null ? props.getProperty("autoStartTable") : null;
            boolean autoStart = autoStartProp != null && autoStartProp.equalsIgnoreCase("true");

            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            FileWriter ddlFileWriter = null;
            try
            {
                performingDeleteSchemaForClasses = true;
                if (ddlFilename != null)
                {
                    // Open the DDL file for writing
                    File ddlFile = StringUtils.getFileForFilename(ddlFilename);
                    if (ddlFile.exists())
                    {
                        // Delete existing file
                        ddlFile.delete();
                    }
                    if (ddlFile.getParentFile() != null && !ddlFile.getParentFile().exists())
                    {
                        // Make sure the directory exists
                        ddlFile.getParentFile().mkdirs();
                    }
                    ddlFile.createNewFile();
                    ddlFileWriter = new FileWriter(ddlFile);

                    SimpleDateFormat fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                    ddlFileWriter.write("------------------------------------------------------------------\n");
                    ddlFileWriter.write("-- DataNucleus SchemaTool " + 
                        "(ran at " + fmt.format(new java.util.Date()) + ")\n");
                    ddlFileWriter.write("------------------------------------------------------------------\n");
                    ddlFileWriter.write("-- Delete schema required for the following classes:-\n");
                    Iterator classNameIter = classNames.iterator();
                    while (classNameIter.hasNext())
                    {
                        ddlFileWriter.write("--     " + classNameIter.next() + "\n");
                    }
                    ddlFileWriter.write("--\n");
                }

                try
                {
                    if (ddlFileWriter != null)
                    {
                        this.ddlWriter = ddlFileWriter;
                        this.completeDDL = completeDdl;
                        this.writtenDdlStatements = new HashSet();
                    }

                    // Generate the tables/constraints for these classes (so we know the tables to delete)
                    // TODO This will add CREATE to the DDL, need to be able to omit this
                    String[] classNameArray = classNames.toArray(new String[classNames.size()]);
                    manageClasses(clr, classNameArray); // Add them to mgr first

                    // Delete the tables of the required classes
                    DeleteTablesSchemaTransaction deleteTablesTxn = new DeleteTablesSchemaTransaction(this, Connection.TRANSACTION_READ_COMMITTED, storeDataMgr);
                    deleteTablesTxn.setWriter(ddlWriter);
                    boolean success = true;
                    try
                    {
                        deleteTablesTxn.execute(clr);
                    }
                    catch (NucleusException ne)
                    {
                        success = false;
                        throw ne;
                    }
                    finally
                    {
                        if (success)
                        {
                            clearSchemaData();
                        }
                    }

                    if (autoStart)
                    {
                        // TODO Delete the SchemaTable auto-starter table
                    }

                    // TODO Delete sequences and sequenceTables
                }
                finally
                {
                    performingDeleteSchemaForClasses = false;
                    if (ddlFileWriter != null)
                    {
                        this.ddlWriter = null;
                        this.completeDDL = false;
                        this.writtenDdlStatements.clear();
                        this.writtenDdlStatements = null;

                        ddlFileWriter.close();
                    }
                }
            }
            catch (IOException ioe)
            {
                // Error in writing DDL file
                // TODO Handle this
            }
        }
        else
        {
            String msg = Localiser.msg("014039");
            NucleusLogger.DATASTORE_SCHEMA.error(msg);
            System.out.println(msg);

            throw new NucleusException(msg);
        }
    }

    public void validateSchemaForClasses(Set<String> inputClassNames, Properties props)
    {
        Set<String> classNames = cleanInputClassNames(nucleusContext, inputClassNames);
        if (classNames != null && classNames.size() > 0)
        {
            // Validate the tables/constraints
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);

            String[] classNameArray = classNames.toArray(new String[classNames.size()]);
            manageClasses(clr, classNameArray); // Validates since we have the flags set
        }
        else
        {
            String msg = Localiser.msg("014039");
            NucleusLogger.DATASTORE_SCHEMA.error(msg);
            System.out.println(msg);

            throw new NucleusException(msg);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaScriptAwareStoreManager#executeScript(java.lang.String)
     */
    public void executeScript(String script)
    {
        script = StringUtils.replaceAll(script, "\n", " ");
        script = StringUtils.replaceAll(script, "\t", " ");
        ManagedConnection mc = getConnection(-1);
        try
        {
            // Execute the script on this datastore
            // Note that we simply split the script at line delimiter (";")
            Connection conn = (Connection) mc.getConnection();
            Statement stmt = conn.createStatement();
            try
            {
                StringTokenizer tokeniser = new StringTokenizer(script, ";");
                while (tokeniser.hasMoreTokens())
                {
                    String token = tokeniser.nextToken().trim();
                    if (!StringUtils.isWhitespace(token))
                    {
                        NucleusLogger.DATASTORE_NATIVE.debug("Executing script statement : " + token);
                        stmt.execute(token + ";");
                    }
                }
            }
            finally
            {
                stmt.close();
            }
        }
        catch (SQLException e)
        {
            NucleusLogger.DATASTORE_NATIVE.error("Exception executing user script", e);
            throw new NucleusUserException("Exception executing user script. See nested exception for details", e);
        }
        finally
        {
            mc.release();
        }
    }

    /**
     * Method to generate a set of class names using the input list.
     * If no input class names are provided then uses the list of classes known to have metadata.
     * @param ctx NucleusContext
     * @param inputClassNames Class names to start from
     * @return The set of class names
     */
    protected static Set<String> cleanInputClassNames(NucleusContext ctx, Set<String> inputClassNames) 
    {
        Set<String> classNames = new TreeSet<>();
        if (inputClassNames == null || inputClassNames.size() == 0)
        {
            // Use all "known" persistable classes
            Collection classesWithMetadata = ctx.getMetaDataManager().getClassesWithMetaData();
            classNames.addAll(classesWithMetadata);
        }
        else
        {
            // Use all input classes
            classNames.addAll(inputClassNames);
        }
        return classNames;
    }
}