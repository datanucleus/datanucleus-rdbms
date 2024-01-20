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
package org.datanucleus.store.rdbms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlanForClass;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.fieldmanager.DynamicSchemaFieldManager;
import org.datanucleus.store.rdbms.request.DeleteRequest;
import org.datanucleus.store.rdbms.request.FetchRequest;
import org.datanucleus.store.rdbms.request.InsertRequest;
import org.datanucleus.store.rdbms.request.LocateBulkRequest;
import org.datanucleus.store.rdbms.request.LocateRequest;
import org.datanucleus.store.rdbms.request.Request;
import org.datanucleus.store.rdbms.request.RequestIdentifier;
import org.datanucleus.store.rdbms.request.RequestType;
import org.datanucleus.store.rdbms.request.UpdateRequest;
import org.datanucleus.store.rdbms.table.ClassView;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.SecondaryDatastoreClass;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ConcurrentReferenceHashMap;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.ConcurrentReferenceHashMap.ReferenceType;

/**
 * Handler for persistence for RDBMS datastores.
 */
public class RDBMSPersistenceHandler extends AbstractPersistenceHandler
{
    /** The cache of database requests. Access is synchronized on the map object itself. */
    private Map<RequestIdentifier, Request> requestsByID = new ConcurrentReferenceHashMap<>(1, ReferenceType.STRONG, ReferenceType.SOFT);

    /** Cache of nullable PK fields */
    private static final Map<Class<?>, int[]> nullablePkFields = new ConcurrentHashMap<>();

    /** 
     * Member extension for when using optimistic locking.
     * When setting this extension with value "false" then it means that
     * when updating this member then do not update version of object.
     * So if ONLY updating such members no version update is made.
     * On the other hand - if just one other member (without this setting)
     * is updated then version is updated as normally.
     */
    public static final String EXTENSION_MEMBER_VERSION_UPDATE = "rdbms-version-update";

    /** Cache of version-update=false field - that is fields where version should not be updated when using optimistic locking */
    private static final Map<Class<? extends Object>, Set<Integer>> noVersionUpdateFieldsMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     * @param storeMgr StoreManager
     */
    public RDBMSPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /**
     * Method to close the handler and release any resources.
     */
    public void close()
    {
        requestsByID.clear();
        requestsByID = null;
    }

    private DatastoreClass getDatastoreClass(String className, ClassLoaderResolver clr)
    {
        return ((RDBMSStoreManager)storeMgr).getDatastoreClass(className, clr);
    }

    protected BitSet getNullPkFields(DNStateManager<?> sm)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        int[] nullablePkFieldsForPC = nullablePkFields.computeIfAbsent(sm.getObject().getClass(), pcClass ->
        {
            final int[] pkMemberPositions = cmd.getPKMemberPositions();
            if (pkMemberPositions == null)
            {
                return new int[0];
            }
            return Arrays.stream(pkMemberPositions)
                    .sorted()
                    .filter(no ->
                    {
                        final AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(no);
                        if (mmd != null)
                        {
                            final ColumnMetaData[] columnMetaData = mmd.getColumnMetaData();
                            if (columnMetaData != null)
                            {
                                for (ColumnMetaData columnMetaDatum : columnMetaData)
                                {
                                    if (columnMetaDatum.isAllowsNull())
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    })
                    .toArray();
        });

        BitSet concreteNullPkFields = new BitSet();

        for (int nullablePkField : nullablePkFieldsForPC)
        {
            final String typeConverterName = cmd.getMetaDataForManagedMemberAtAbsolutePosition(nullablePkField)
                    .getTypeConverterName();
            final TypeConverter typeConverter = sm.getExecutionContext().getNucleusContext()
                    .getTypeManager().getTypeConverterForName(typeConverterName);
            final Object val = sm.provideField(nullablePkField);
            if ((typeConverter==null ? val : typeConverter.toDatastoreType(val)) == null)
            {
                concreteNullPkFields.set(nullablePkField);
            }
        }
        return concreteNullPkFields;
    }

    // ------------------------------ Insert ----------------------------------

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#insertObjects(org.datanucleus.state.DNStateManager[])
     */
    @Override
    public void insertObjects(DNStateManager... sms)
    {
        // TODO Implement this in such a way as it process all objects of a type in one call (after checking hasRelations)
        super.insertObjects(sms);
    }

    /**
     * Inserts a persistent object into the database.
     * The insert can take place in several steps, one insert per table that it is stored in.
     * e.g When persisting an object that uses "new-table" inheritance for each level of the inheritance tree then will get an INSERT into each table. 
     * When persisting an object that uses "complete-table" inheritance then will get a single INSERT into its table.
     * @param sm StateManager for the object to be inserted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    @Override
    public void insertObject(DNStateManager sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        // Check if we need to do any updates to the schema before inserting this object
        checkForSchemaUpdatesForFieldsOfObject(sm, sm.getLoadedFieldNumbers());

        ExecutionContext ec = sm.getExecutionContext();
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        String className = sm.getClassMetaData().getFullClassName();
        DatastoreClass dc = getDatastoreClass(className, clr);
        if (dc == null)
        {
            if (sm.getClassMetaData().getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
            {
                throw new NucleusUserException(Localiser.msg("032013", className));
            }
            throw new NucleusException(Localiser.msg("032014", className, sm.getClassMetaData().getInheritanceMetaData().getStrategy())).setFatal();
        }

        if (ec.getStatistics() != null)
        {
            ec.getStatistics().incrementInsertCount();
        }

        insertObjectInTable(dc, sm, clr);
    }

    /**
     * Convenience method to handle the insert into the various tables that this object is persisted into.
     * @param table The table to process
     * @param sm StateManager for the object being inserted
     * @param clr ClassLoader resolver
     */
    private void insertObjectInTable(DatastoreClass table, DNStateManager sm, ClassLoaderResolver clr)
    {
        if (table instanceof ClassView)
        {
            throw new NucleusUserException("Cannot perform InsertRequest on RDBMS view " + table);
        }

        DatastoreClass supertable = table.getSuperDatastoreClass();
        if (supertable != null)
        {
            // Process the superclass table first
            insertObjectInTable(supertable, sm, clr);
        }

        // Do the actual insert of this table
        getInsertRequest(table, sm.getClassMetaData(), clr).execute(sm);

        // Process any secondary tables
        Collection<? extends SecondaryDatastoreClass> secondaryTables = table.getSecondaryDatastoreClasses();
        if (secondaryTables != null)
        {
            for (SecondaryDatastoreClass secTable : secondaryTables)
            {
                // Process the secondary table
                insertObjectInTable(secTable, sm, clr);
            }
        }
    }

    /**
     * Returns a request object that will insert a row in the given table. 
     * The store manager will cache the request object for re-use by subsequent requests to the same table.
     * @param table The table into which to insert.
     * @param cmd ClassMetaData of the object of the request
     * @param clr ClassLoader resolver
     * @return An insertion request object.
     */
    private Request getInsertRequest(DatastoreClass table, AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        RequestIdentifier reqID = new RequestIdentifier(table, null, RequestType.INSERT, cmd.getFullClassName());
        Request req = requestsByID.get(reqID);
        if (req == null)
        {
            req = new InsertRequest(table, cmd, clr);
            requestsByID.put(reqID, req);
        }
        return req;
    }

    // ------------------------------ Fetch ----------------------------------

    /**
     * Fetches (fields of) a persistent object from the database.
     * This does a single SELECT on the candidate of the class in question. 
     * Will join to inherited tables as appropriate to get values persisted into other tables. 
     * Can also join to the tables of related objects (1-1, N-1) as necessary to retrieve those objects.
     * @param sm StateManager of the object to be fetched.
     * @param memberNumbers The numbers of the members to be fetched.
     * @throws NucleusObjectNotFoundException if the object doesn't exist
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    @Override
    public void fetchObject(DNStateManager sm, int memberNumbers[])
    {
        ExecutionContext ec = sm.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();

        // Extract metadata of members to fetch/store
        AbstractMemberMetaData[] mmdsToFetch = null;
        AbstractMemberMetaData[] mmdsToStore = null;
        if (memberNumbers != null && memberNumbers.length > 0)
        {
            int[] memberNumbersToFetch = memberNumbers;
            int[] memberNumbersToStore = null;
            AbstractClassMetaData cmd = sm.getClassMetaData();

            // Option to add additional members that are currently unloaded but not requested to this fetch
            boolean addUnloadedBasic = storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_FETCH_UNLOADED_BASIC_AUTO);
            boolean addUnloadedFK = storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_FETCH_UNLOADED_FK_AUTO);
            if (!sm.getLifecycleState().isDeleted() && (addUnloadedBasic || addUnloadedFK))
            {
                // Check if this fetch selects anything (because we don't want to add anything unnecessarily
                boolean fetchPerformsSelect = false;
                for (int i=0;i<memberNumbers.length;i++)
                {
                    AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberNumbers[i]);
                    RelationType relationType = mmd.getRelationType(clr);
                    if (relationType == RelationType.NONE || RelationType.isRelationSingleValued(relationType))
                    {
                        fetchPerformsSelect = true;
                        break;
                    }
                }

                if (fetchPerformsSelect)
                {
                    // Definitely does a SELECT, so try to identify any additional non-relation or 1-1, N-1
                    // members that aren't loaded that could be fetched right now in this call.
                    List<Integer> memberNumberList = new ArrayList<>();
                    for (int i=0;i<memberNumbers.length;i++)
                    {
                        memberNumberList.add(memberNumbers[i]);
                    }
                    List<Integer> memberNumberListStore = new ArrayList<>();

                    // Check if we could retrieve any other unloaded fields in this call
                    boolean[] loadedFlags = sm.getLoadedFields();
                    for (int i=0;i<loadedFlags.length;i++)
                    {
                        boolean requested = false;
                        for (int j=0;j<memberNumbers.length;j++)
                        {
                            if (memberNumbers[j] == i)
                            {
                                requested = true;
                                break;
                            }
                        }
                        if (!requested && !loadedFlags[i])
                        {
                            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(i);
                            RelationType relType = mmd.getRelationType(clr);
                            if (relType == RelationType.NONE)
                            {
                                if (addUnloadedBasic)
                                {
                                    // Basic field is unloaded and not selected, so select it
                                    memberNumberList.add(i);
                                }
                            }
                            else if (relType == RelationType.ONE_TO_ONE_UNI || (relType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() == null)) // TODO N-1 with FK
                            {
                                if (!mmd.getType().isInterface()) // Ignore reference fields for now
                                {
                                    if (addUnloadedFK)
                                    {
                                        // 1-1 with FK at this side so can select/store the FK
                                        memberNumberListStore.add(i);
                                    }
                                }
                            }
                        }
                    }

                    memberNumbersToFetch = memberNumberList.stream().mapToInt(i -> i).toArray();
//                    memberNumbersToFetch = new int[memberNumberList.size()];
//                    int i=0;
//                    for (Integer memberNumber : memberNumberList)
//                    {
//                        memberNumbersToFetch[i++] = memberNumber;
//                    }

                    if (!memberNumberListStore.isEmpty())
                    {
                        memberNumbersToStore = memberNumberListStore.stream().mapToInt(i -> i).toArray();
//                        i = 0;
//                        memberNumbersToStore = new int[memberNumberListStore.size()];
//                        for (Integer memberNumber : memberNumberListStore)
//                        {
//                            memberNumbersToStore[i++] = memberNumber;
//                        }
                    }
                }
            }

            // Convert the member numbers for this class into their member metadata
            mmdsToFetch = new AbstractMemberMetaData[memberNumbersToFetch.length];
            for (int i=0;i<mmdsToFetch.length;i++)
            {
                mmdsToFetch[i] = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberNumbersToFetch[i]);
            }

            if (memberNumbersToStore != null)
            {
                mmdsToStore = new AbstractMemberMetaData[memberNumbersToStore.length];
                for (int i=0;i<mmdsToStore.length;i++)
                {
                    mmdsToStore[i] = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberNumbersToStore[i]);
                }
            }
        }

        if (sm.isEmbedded())
        {
            StringBuilder str = new StringBuilder();
            if (mmdsToFetch != null)
            {
                for (int i=0;i<mmdsToFetch.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(',');
                    }
                    str.append(mmdsToFetch[i].getName());
                }
            }
            NucleusLogger.PERSISTENCE.info("Request to load fields \"" + str.toString() + "\" of class " + sm.getClassMetaData().getFullClassName() + " but object is embedded, so ignored");
        }
        else
        {
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementFetchCount();
            }

            DatastoreClass table = getDatastoreClass(sm.getClassMetaData().getFullClassName(), clr);
            Request req = getFetchRequest(table, sm.getFetchPlanForClass(), clr, sm, mmdsToFetch, mmdsToStore);
            req.execute(sm);
        }
    }

    /**
     * Returns a request object that will fetch a row from the given table. 
     * The store manager will cache the request object for re-use by subsequent requests to the same table.
     * @param table The table from which to fetch.
     * @param fpClass FetchPlan for class
     * @param clr ClassLoader resolver
     * @param sm StateManager for the object being updated
     * @param mmdsFetch MetaData for the members to be fetched.
     * @param mmdsStore MetaData for the members to store the values for later processing
     * @return A fetch request object.
     */
    private Request getFetchRequest(DatastoreClass table, FetchPlanForClass fpClass, ClassLoaderResolver clr, DNStateManager<?> sm,
            AbstractMemberMetaData[] mmdsFetch, AbstractMemberMetaData[] mmdsStore)
    {
        // TODO Add fpClass to RequestIdentifier
        final BitSet nullPkFields = getNullPkFields(sm);
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        RequestIdentifier reqID = new RequestIdentifier(table, mmdsFetch, mmdsStore, RequestType.FETCH, cmd.getFullClassName(), nullPkFields);
        Request req = requestsByID.get(reqID);
        if (req == null)
        {
            req = new FetchRequest(table, fpClass, clr, cmd, mmdsFetch, mmdsStore, nullPkFields);
            requestsByID.put(reqID, req);
        }
        return req;
    }

    // ------------------------------ Update ----------------------------------

    /**
     * Updates a persistent object in the database.
     * The update can take place in several steps, one update per table that it is stored in (depending on which fields are updated).
     * e.g When updating an object that uses "new-table" inheritance for each level of the inheritance tree then will get an UPDATE into each table.
     * When updating an object that uses "complete-table" inheritance then will get a single UPDATE into its table.
     * @param sm StateManager for the object to be updated.
     * @param fieldNumbers The numbers of the fields to be updated.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    @Override
    public void updateObject(DNStateManager sm, int fieldNumbers[])
    {
        // Check if we need to do any updates to the schema before updating this object
        checkForSchemaUpdatesForFieldsOfObject(sm, fieldNumbers);

        AbstractMemberMetaData[] mmds = null;
        if (fieldNumbers != null && fieldNumbers.length > 0)
        {
            // Convert the field numbers for this class into their metadata for the table
            ExecutionContext ec = sm.getExecutionContext();
            mmds = new AbstractMemberMetaData[fieldNumbers.length];
            for (int i=0;i<mmds.length;i++)
            {
                mmds[i] = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementUpdateCount();
            }

            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            DatastoreClass dc = getDatastoreClass(sm.getObject().getClass().getName(), clr);
            final boolean noVersionUpdate = onlyNoVersionUpdateFieldsDirty(sm, fieldNumbers);
            updateObjectInTable(dc, sm, clr, mmds, noVersionUpdate);
        }
    }

    /**
     * Convenience method to handle the update into the various tables that this object is persisted into.
     *
     * @param table           The table to process
     * @param sm              StateManager for the object being updated
     * @param clr             ClassLoader resolver
     * @param mmds            MetaData for the fields being updated
     * @param noVersionUpdate
     */
    private void updateObjectInTable(DatastoreClass table, DNStateManager sm, ClassLoaderResolver clr, AbstractMemberMetaData[] mmds, boolean noVersionUpdate)
    {
        if (table instanceof ClassView)
        {
            throw new NucleusUserException("Cannot perform UpdateRequest on RDBMS view " + table);
        }

        DatastoreClass supertable = table.getSuperDatastoreClass();
        if (supertable != null)
        {
            // Process the superclass table first
            updateObjectInTable(supertable, sm, clr, mmds, noVersionUpdate);
        }

        // Do the actual update of this table
        final Request updateRequest = getUpdateRequest(table, mmds, sm, clr, noVersionUpdate);
        if (!(updateRequest instanceof UpdateRequest) || ((UpdateRequest) updateRequest).willUpdate())
        {
            // Check if read-only so update not permitted
            // Only check if update request really will update anything
            assertReadOnlyForUpdateOfObject(sm);
        }
        updateRequest.execute(sm);

        // Update any secondary tables
        Collection<? extends SecondaryDatastoreClass> secondaryTables = table.getSecondaryDatastoreClasses();
        if (secondaryTables != null)
        {
            for (SecondaryDatastoreClass secTable : secondaryTables)
            {
                // Process the secondary table
                updateObjectInTable(secTable, sm, clr, mmds, noVersionUpdate);
            }
        }
    }

    /**
     * Checks if ONLY version-update=false fields are updated.
     * If this is the case we do not want to update anything - not even version number.
     * Return true from this method if you do not want to update version of object
     * based on fieldNumbers being updated.
     *
     * @param sm StateManager for the object to be updated.
     * @param fieldNumbers The numbers of the fields to be updated.
     * @return true if no version update is wanted for this objects and supplied updated fieldsNumbers.
     */
    protected boolean onlyNoVersionUpdateFieldsDirty(DNStateManager sm, int[] fieldNumbers)
    {
        Set<Integer> noVersionUpdateFields = getNoVersionUpdateFields(sm); // get all fields with version-update=false
        if (noVersionUpdateFields.isEmpty())
            return false; // only relevant for types having version-update=false fields
        boolean hadDirtyField = false;
        for (int i=0; i< fieldNumbers.length; i++)
        {
            int fieldNo = fieldNumbers[i];
            hadDirtyField = true;
            if (!noVersionUpdateFields.contains(fieldNo))
                return false;
        }
        return hadDirtyField; // has dirty fields && all dirty fields are version-update=false marked
    }

    private static Set<Integer> getNoVersionUpdateFields(DNStateManager sm) {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        Set<Integer> lockGroupNoneFields = noVersionUpdateFieldsMap.computeIfAbsent(sm.getObject().getClass(), clazz ->
        {
            return Arrays.stream(cmd.getAllMemberPositions())
                    .filter(fieldNo -> {
                        final String versionLock = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNo)
                                .getValueForExtension(EXTENSION_MEMBER_VERSION_UPDATE);
                        return "false".equals(versionLock);
                    })
                    .boxed()
                    .collect(Collectors.toSet());
        });
        return lockGroupNoneFields;
    }

    /**
     * Returns a request object that will update a row in the given table.
     * The store manager will cache the request object for re-use by subsequent requests to the same table.
     *
     * @param table           The table in which to update.
     * @param mmds            The metadata corresponding to the columns to be updated. MetaData whose columns exist in supertables will be ignored.
     * @param sm              StateManager for the object being updated
     * @param clr             ClassLoader resolver
     * @param noVersionUpdate Should we ignore updating version when using optimistic lock?
     * @return An update request object.
     */
    private Request getUpdateRequest(DatastoreClass table, AbstractMemberMetaData[] mmds, DNStateManager<?> sm, ClassLoaderResolver clr, boolean noVersionUpdate)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        BitSet nullPkFields = getNullPkFields(sm);
        RequestIdentifier reqID = new RequestIdentifier(table, mmds, null, RequestType.UPDATE, cmd.getFullClassName(), nullPkFields, noVersionUpdate);
        Request req = requestsByID.get(reqID);
        if (req == null)
        {
            req = new UpdateRequest(table, mmds, cmd, clr, nullPkFields, noVersionUpdate);
            requestsByID.put(reqID, req);
        }
        return req;
    }

    // ------------------------------ Delete ----------------------------------

    /**
     * Deletes a persistent object from the database.
     * The delete can take place in several steps, one delete per table that it is stored in.
     * e.g When deleting an object that uses "new-table" inheritance for each level of the inheritance tree then will get an DELETE for each table.
     * When deleting an object that uses "complete-table" inheritance then will get a single DELETE for its table.
     * @param sm StateManager for the object to be deleted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    @Override
    public void deleteObject(DNStateManager sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        if (ec.getStatistics() != null)
        {
            ec.getStatistics().incrementDeleteCount();
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        DatastoreClass dc = getDatastoreClass(sm.getClassMetaData().getFullClassName(), clr);
        deleteObjectFromTable(dc, sm, clr);
    }

    /**
     * Convenience method to handle the delete from the various tables that this object is persisted into.
     * @param table The table to process
     * @param sm StateManager for the object being deleted
     * @param clr ClassLoader resolver
     */
    private void deleteObjectFromTable(DatastoreClass table, DNStateManager sm, ClassLoaderResolver clr)
    {
        if (table instanceof ClassView)
        {
            throw new NucleusUserException("Cannot perform DeleteRequest on RDBMS view " + table);
        }

        // Delete any secondary tables
        Collection<? extends SecondaryDatastoreClass> secondaryTables = table.getSecondaryDatastoreClasses();
        if (secondaryTables != null)
        {
            for (SecondaryDatastoreClass secTable : secondaryTables)
            {
                // Process the secondary table
                deleteObjectFromTable(secTable, sm, clr);
            }
        }

        // Do the actual delete of this table
        getDeleteRequest(table, sm, clr).execute(sm);

        DatastoreClass supertable = table.getSuperDatastoreClass();
        if (supertable != null)
        {
            // Process the superclass table last
            deleteObjectFromTable(supertable, sm, clr);
        }
    }

    /**
     * Returns a request object that will delete a row from the given table.
     * The store manager will cache the request object for re-use by subsequent requests to the same table.
     * @param table The table from which to delete.
     * @param sm StateManager for the object being updated
     * @param clr ClassLoader resolver
     * @return A deletion request object.
     */
    private Request getDeleteRequest(DatastoreClass table, DNStateManager<?> sm, ClassLoaderResolver clr)
    {
        final AbstractClassMetaData acmd = sm.getClassMetaData();
        BitSet nullPkFields = getNullPkFields(sm);
        RequestIdentifier reqID = new RequestIdentifier(table, null, null, RequestType.DELETE, acmd.getFullClassName(), nullPkFields);
        Request req = requestsByID.get(reqID);
        if (req == null)
        {
            req = new DeleteRequest(table, acmd, clr, nullPkFields);
            requestsByID.put(reqID, req);
        }
        return req;
    }

    // ------------------------------ Locate ----------------------------------

    @Override
    public void locateObjects(DNStateManager[] sms)
    {
        if (sms == null || sms.length == 0)
        {
            return;
        }

        ClassLoaderResolver clr = sms[0].getExecutionContext().getClassLoaderResolver();
        Map<DatastoreClass, List<DNStateManager>> smsByTable = new HashMap<>();
        for (int i=0;i<sms.length;i++)
        {
            AbstractClassMetaData cmd = sms[i].getClassMetaData();
            DatastoreClass table = getDatastoreClass(cmd.getFullClassName(), clr);
            table = table.getBaseDatastoreClass(); // Use root table in hierarchy
            List<DNStateManager> smList = smsByTable.get(table);
            if (smList == null)
            {
                smList = new ArrayList<>();
            }
            smList.add(sms[i]);
            smsByTable.put(table, smList);
        }

        // Submit a LocateBulkRequest for each table required, with each request returning however many are in that table
        for (Map.Entry<DatastoreClass, List<DNStateManager>> entry : smsByTable.entrySet())
        {
            DatastoreClass table = entry.getKey();
            List<DNStateManager> tableSMs = entry.getValue();
            AbstractClassMetaData cmd = tableSMs.get(0).getClassMetaData();

            // TODO This just uses the base table. Could change to use the most-derived table
            // which would permit us to join to supertables and load more fields during this process
            LocateBulkRequest req = new LocateBulkRequest(table, cmd, clr);
            req.execute(tableSMs.toArray(new DNStateManager[tableSMs.size()]));
        }
    }

    /**
     * Locates this object in the datastore.
     * @param sm StateManager for the object to be found
     * @throws NucleusObjectNotFoundException if the object doesnt exist
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    @Override
    public void locateObject(DNStateManager sm)
    {
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        DatastoreClass table = getDatastoreClass(sm.getObject().getClass().getName(), clr);
        getLocateRequest(table, sm, clr).execute(sm);
    }

    /**
     * Returns a request object that will locate a row from the given table.
     * The store manager will cache the request object for re-use by subsequent requests to the same table.
     * @param table The table from which to locate.
     * @param sm StateManager for the object being updated
     * @param clr ClassLoader resolver
     * @return A locate request object.
     */
    private Request getLocateRequest(DatastoreClass table, DNStateManager<?> sm, ClassLoaderResolver clr)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        BitSet nullPkFields = getNullPkFields(sm);
        RequestIdentifier reqID = new RequestIdentifier(table, null, null, RequestType.LOCATE, cmd.getFullClassName(), nullPkFields);
        Request req = requestsByID.get(reqID);
        if (req == null)
        {
            req = new LocateRequest(table, cmd, clr, nullPkFields);
            requestsByID.put(reqID, req);
        }
        return req;
    }

    // ------------------------------ Find ----------------------------------

    /**
     * Method to return a persistable object with the specified id. Optional operation for StoreManagers.
     * Should return a (at least) hollow persistable object if the store manager supports the operation.
     * If the StoreManager is managing the in-memory object instantiation (as part of co-managing the object 
     * lifecycle in general), then the StoreManager has to create the object during this call (if it is not 
     * already created). Most relational databases leave the in-memory object instantion to Core, but some 
     * object databases may manage the in-memory object instantion, effectively preventing Core of doing this.
     * <p>
     * StoreManager implementations may simply return null, indicating that they leave the object instantiate to us. 
     * Other implementations may instantiate the object in question (whether the implementation may trust that the object is not already instantiated 
     * has still to be determined). If an implementation believes that an object with the given ID should exist, but in fact does not exist, 
     * then the implementation should throw a RuntimeException. It should not silently return null in this case.
     * </p>
     * @param ec execution context
     * @param id the id of the object in question.
     * @return a persistable object with a valid object state (for example: hollow) or null, 
     *     indicating that the implementation leaves the instantiation work to us.
     */
    @Override
    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }

    // ------------------------------ Convenience ----------------------------------

    /**
     * Convenience method to remove all requests since the schema has changed.
     */
    public void removeAllRequests()
    {
        synchronized (requestsByID)
        {
            requestsByID.clear();
        }
    }

    /**
     * Convenience method to remove all requests that use a particular table since the structure
     * of the table has changed potentially leading to missing columns in the cached version.
     * @param table The table
     */
    public void removeRequestsForTable(DatastoreClass table)
    {
        synchronized(requestsByID)
        {
            // Synchronise on the "requestsById" set since while it is "synchronised itself, all iterators needs this sync
            Set<RequestIdentifier> keySet = new HashSet<>(requestsByID.keySet());
            for (RequestIdentifier reqId : keySet)
            {
                if (reqId.getTable() == table)
                {
                    requestsByID.remove(reqId);
                }
            }
        }
    }

    /**
     * Check if we need to update the schema before performing an insert/update.
     * This is typically of use where the user has an interface field and some new implementation
     * is trying to be persisted to that field, so we need to update the schema.
     * @param sm StateManager for the object
     * @param fieldNumbers The fields to check for required schema updates
     */
    private void checkForSchemaUpdatesForFieldsOfObject(DNStateManager sm, int[] fieldNumbers)
    {
        if (fieldNumbers == null || fieldNumbers.length == 0)
        {
            // Nothing to do for a class with no fields/attributes
            return;
        }

        if (storeMgr.getBooleanObjectProperty(RDBMSPropertyNames.PROPERTY_RDBMS_DYNAMIC_SCHEMA_UPDATES).booleanValue())
        {
            DynamicSchemaFieldManager dynamicSchemaFM = new DynamicSchemaFieldManager((RDBMSStoreManager)storeMgr, sm);
            sm.provideFields(fieldNumbers, dynamicSchemaFM);
            if (dynamicSchemaFM.hasPerformedSchemaUpdates())
            {
                requestsByID.clear();
            }
        }
    }
}