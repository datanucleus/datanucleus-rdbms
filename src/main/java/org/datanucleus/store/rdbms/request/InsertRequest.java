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
2003 Erik Bengtson - the fields to fetch are better managed for application
                     identity. Their selection were moved from execute
                     method to the constructor
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - conversion to use Logger
2004 Erik Bengtson - changed to use mapping consumer
2004 Andy Jefferson - added discriminator support
2005 Andy Jefferson - fixed 1-1 bidir order of insertion
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.request;

import static java.util.stream.Collectors.toList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NotYetFlushedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.fieldmanager.ParameterSetter;
import org.datanucleus.store.rdbms.table.AbstractClassTable;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.SecondaryTable;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Class to provide a means of insertion of records to RDBMS. 
 * Extends basic request class implementing the execute method to do a JDBC insert operation.
 * The SQL will be of the form
 * <pre>
 * INSERT INTO TBL_NAME (col1, col2, col3, ...) VALUES (?, ?, ?, ...)
 * </pre>
 * <p>
 * When inserting an object with inheritance this will involve 1 InsertRequest for each table involved. 
 * So if we have a class B that extends class A and they both use "new-table" inheritance strategy, we will have 2 InsertRequests, one for table A, and one for table B.
 * </p>
 * <p>
 * When the InsertRequest starts to populate its statement and it has a PC field, this calls PersistableMapping.setObject(). 
 * This then checks if the other PC object is yet persistent and, if not, will persist it before processing this objects INSERT. 
 * This forms the key to "persistence-by-reachability".
 * </p>
 */
public class InsertRequest extends Request
{
    private static final int IDPARAMNUMBER = 1;

    /** callback mappings will have their postInsert method called after the update */
    private final List<MappingCallbacks> mappingCallbacks;

    private final List<JavaTypeMapping> postSetMappings;

    /** Numbers of fields in the INSERT statement (excluding PK). */
    private final int[] insertFieldNumbers;

    /** Numbers of Primary key fields. */
    private final int[] pkFieldNumbers;

    /** Numbers of fields that are reachable yet have no datastore column in this table. Used for reachability. */
    private final int[] reachableFieldNumbers;

    /** Numbers of fields that are relations that may be detached when persisting but not bidir so cant attach yet. */
    private final int[] relationFieldNumbers;

    /** SQL statement for the INSERT. */
    private final String insertStmt;

    /** Whether the class has an identity (auto-increment, serial etc) column */
    private boolean hasIdentityColumn = false;

    /** one StatementExpressionIndex for each field **/
    private StatementMappingIndex[] stmtMappings;

    /** StatementExpressionIndex for fields to be "retrieved" **/
    private StatementMappingIndex[] retrievedStmtMappings;

    /** StatementExpressionIndex for version **/
    private StatementMappingIndex versionStmtMapping;

    /** StatementExpressionIndex for discriminator. **/
    private StatementMappingIndex discriminatorStmtMapping;

    /** StatementExpressionIndex for multi-tenancy. **/
    private StatementMappingIndex multitenancyStmtMapping;

    /** StatementExpressionIndex for soft-delete. **/
    private StatementMappingIndex softDeleteStmtMapping;

    /** StatementExpressionIndex for create-user. **/
    private StatementMappingIndex createUserStmtMapping;

    /** StatementExpressionIndex for create-timestamp. **/
    private StatementMappingIndex createTimestampStmtMapping;

    /** StatementExpressionIndex for update-user. **/
    private StatementMappingIndex updateUserStmtMapping;

    /** StatementExpressionIndex for update-timestamp. **/
    private StatementMappingIndex updateTimestampStmtMapping;

    /** StatementExpressionIndex for external FKs */
    private StatementMappingIndex[] externalFKStmtMappings;

    /** StatementExpressionIndex for external FK discriminators (shared FKs) */
    private StatementMappingIndex[] externalFKDiscrimStmtMappings;

    /** StatementExpressionIndex for external indices */
    private StatementMappingIndex[] externalOrderStmtMappings;

    /** Whether to batch the INSERT SQL. */
    private boolean batch = false;

    /**
     * Constructor, taking the table. Uses the structure of the datastore table to build a basic query.
     * @param table The Class Table representing the datastore table to insert.
     * @param cmd ClassMetaData for the object being persisted
     * @param clr ClassLoader resolver
     */
    public InsertRequest(DatastoreClass table, AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        super(table);

        InsertMappingConsumer consumer = new InsertMappingConsumer(clr, cmd, IDPARAMNUMBER);
        table.provideSurrogateMapping(SurrogateColumnType.DATASTORE_ID, consumer);
        table.provideNonPrimaryKeyMappings(consumer);
        table.providePrimaryKeyMappings(consumer);
        table.provideSurrogateMapping(SurrogateColumnType.VERSION, consumer);
        boolean discriminatorColumnAlreadyMapped = cmd.getDiscriminatorColumnName()!=null && consumer.assignedColumns.containsKey(cmd.getDiscriminatorColumnName().toUpperCase());
        if (!discriminatorColumnAlreadyMapped) {
            table.provideSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, consumer);
        }
        table.provideSurrogateMapping(SurrogateColumnType.MULTITENANCY, consumer);
        table.provideSurrogateMapping(SurrogateColumnType.SOFTDELETE, consumer);
        table.provideSurrogateMapping(SurrogateColumnType.CREATE_USER, consumer);
        table.provideSurrogateMapping(SurrogateColumnType.CREATE_TIMESTAMP, consumer);
        table.provideSurrogateMapping(SurrogateColumnType.UPDATE_USER, consumer);
        table.provideSurrogateMapping(SurrogateColumnType.UPDATE_TIMESTAMP, consumer);
        table.provideExternalMappings(consumer, MappingType.EXTERNAL_FK);
        table.provideExternalMappings(consumer, MappingType.EXTERNAL_FK_DISCRIMINATOR);
        table.provideExternalMappings(consumer, MappingType.EXTERNAL_INDEX);
        table.provideUnmappedColumns(consumer);

        mappingCallbacks = consumer.getMappingCallbacks();
        postSetMappings = consumer.getPostSetMappings();
        stmtMappings = consumer.getStatementMappings();
        versionStmtMapping = consumer.getVersionStatementMapping();
        discriminatorStmtMapping = consumer.getDiscriminatorStatementMapping();
        multitenancyStmtMapping = consumer.getMultitenancyStatementMapping();
        softDeleteStmtMapping = consumer.getSoftDeleteStatementMapping();
        createUserStmtMapping = consumer.getCreateUserStatementMapping();
        createTimestampStmtMapping = consumer.getCreateTimestampStatementMapping();
        updateUserStmtMapping = consumer.getUpdateUserStatementMapping();
        updateTimestampStmtMapping = consumer.getUpdateTimestampStatementMapping();

        externalFKStmtMappings = consumer.getExternalFKStatementMapping();
        externalFKDiscrimStmtMappings = consumer.getExternalFKDiscrimStatementMapping();
        externalOrderStmtMappings = consumer.getExternalOrderStatementMapping();
        pkFieldNumbers = consumer.getPrimaryKeyFieldNumbers();
        if (table.getIdentityType() == IdentityType.APPLICATION && pkFieldNumbers.length < 1 && !hasIdentityColumn)
        {
	        throw new NucleusException(Localiser.msg("052200", cmd.getFullClassName())).setFatal();
        }
        insertFieldNumbers = consumer.getInsertFieldNumbers();
        retrievedStmtMappings = consumer.getReachableStatementMappings();
        reachableFieldNumbers = consumer.getReachableFieldNumbers();
        relationFieldNumbers = consumer.getRelationFieldNumbers();

        insertStmt = consumer.getInsertStmt();

        // TODO Need to also check on whether there is inheritance with multiple tables
        if (!hasIdentityColumn && !cmd.hasRelations(clr) &&  externalFKStmtMappings == null)
        {
            // No identity, no persistence-by-reachability and no external FKs so should be safe to batch this
            batch = true;
        }
    }

    /**
     * Method performing the insertion of the record from the datastore. 
     * Takes the constructed insert query and populates with the specific record information.
     * @param sm StateManager for the record to be inserted
     */
    public void execute(DNStateManager sm)
    {
        ExecutionContext ec = sm.getExecutionContext();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are inserting
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("052207", sm.getObjectAsPrintable(), table));
        }

        try
        {
            VersionMetaData vermd = table.getVersionMetaData();
            RDBMSStoreManager storeMgr = table.getStoreManager();
            if (vermd != null && vermd.getMemberName() != null)
            {
                // Version field - Update the version in the object
                AbstractMemberMetaData verfmd = ((AbstractClassMetaData)vermd.getParent()).getMetaDataForMember(vermd.getMemberName());
                Object currentVersion = sm.getVersion();
                if (currentVersion instanceof Number)
                {
                    // Cater for Integer based versions
                    currentVersion = Long.valueOf(((Number)currentVersion).longValue());
                }

                Object nextOptimisticVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                if (verfmd.getType() == Integer.class || verfmd.getType() == int.class)
                {
                    // Cater for Integer based versions
                    nextOptimisticVersion = Integer.valueOf(((Number)nextOptimisticVersion).intValue());
                }
                sm.replaceField(verfmd.getAbsoluteFieldNumber(), nextOptimisticVersion);
            }

            // Set the state to "inserting" (may already be at this state if multiple inheritance level INSERT)
            sm.setInserting();

            SQLController sqlControl = storeMgr.getSQLController();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                List<String> pkColumnNames = new ArrayList<>();
                if (table.getIdentityType() == IdentityType.DATASTORE)
                {
                    JavaTypeMapping mapping = table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, true);
                    ColumnMapping[] columnMappings = mapping.getColumnMappings();
                    pkColumnNames = Stream.of(columnMappings).map(cm -> cm.getColumn().getIdentifier().getName()).collect(toList());
                }
                else if (table.getIdentityType() == IdentityType.APPLICATION)
                {
                    List<Column> pkColumns = ((AbstractClassTable)table).getPrimaryKey().getColumns();
                    if (!pkColumns.isEmpty())
                    {
                        pkColumnNames = pkColumns.stream().map(cm -> cm.getIdentifier().getName()).collect(toList());
                    }
                }

                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, insertStmt, batch,
                    hasIdentityColumn && storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.GET_GENERATED_KEYS_STATEMENT),
                    pkColumnNames);

                try
                {
                    StatementClassMapping mappingDefinition = new StatementClassMapping();
                    for (int i=0;i<stmtMappings.length;i++)
                    {
                        if (stmtMappings[i] != null)
                        {
                            mappingDefinition.addMappingForMember(i, stmtMappings[i]);
                        }
                    }

                    // Provide the primary key field(s)
                    if (table.getIdentityType() == IdentityType.DATASTORE)
                    {
                        if (!table.isObjectIdDatastoreAttributed() || !table.isBaseDatastoreClass())
                        {
                            int[] paramNumber = {IDPARAMNUMBER};
                            table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false).setObject(ec, ps, paramNumber, sm.getInternalObjectId());
                        }
                    }
                    else if (table.getIdentityType() == IdentityType.APPLICATION)
                    {
                        if (pkFieldNumbers != null && pkFieldNumbers.length > 0)
                        {
                            sm.provideFields(pkFieldNumbers, new ParameterSetter(sm, ps, mappingDefinition));
                        }
                    }

                    // Provide all non-key fields needed for the insert - provides "persistence-by-reachability" for these fields
                    if (insertFieldNumbers.length > 0)
                    {
                        AbstractClassMetaData cmd = sm.getClassMetaData();
                        for (int i=0;i<insertFieldNumbers.length;i++)
                        {
                            if (insertFieldNumbers[i] == cmd.getCreateTimestampMemberPosition())
                            {
                                // Member "create-timestamp"
                                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(insertFieldNumbers[i]);
                                if (mmd.isCreateTimestamp())
                                {
                                    if (mmd.getType().isAssignableFrom(java.time.Instant.class))
                                    {
                                        sm.replaceField(insertFieldNumbers[i], ec.getTransaction().getIsActive() ? 
                                                java.time.Instant.ofEpochMilli(ec.getTransaction().getBeginTime()) : java.time.Instant.now());
                                    }
                                    else
                                    {
                                        sm.replaceField(insertFieldNumbers[i], ec.getTransaction().getIsActive() ? 
                                                new Timestamp(ec.getTransaction().getBeginTime()) : new Timestamp(System.currentTimeMillis()));
                                    }
                                }
                            }
                            else if (insertFieldNumbers[i] == cmd.getCreateUserMemberPosition())
                            {
                                // Member "create-user"
                                sm.replaceField(cmd.getCreateUserMemberPosition(), ec.getCurrentUser());
                            }
                            else if (insertFieldNumbers[i] == cmd.getUpdateTimestampMemberPosition())
                            {
                                // Member "update-timestamp"
                                sm.replaceField(insertFieldNumbers[i], null);
                            }
                            else if (insertFieldNumbers[i] == cmd.getUpdateUserMemberPosition())
                            {
                                // Member "update-user"
                                sm.replaceField(insertFieldNumbers[i], null);
                            }
                        }

                        int numberOfFieldsToProvide = 0;
                        int numMembers = cmd.getMemberCount();
                        for (int i = 0; i < insertFieldNumbers.length; i++)
                        {
                            if (insertFieldNumbers[i] < numMembers)
                            {
                                numberOfFieldsToProvide++;
                            }
                        }

                        int j = 0;
                        int[] fieldNums = new int[numberOfFieldsToProvide];
                        for (int i = 0; i < insertFieldNumbers.length; i++)
                        {
                            if (insertFieldNumbers[i] < numMembers)
                            {
                                fieldNums[j++] = insertFieldNumbers[i];
                            }
                        }
                        sm.provideFields(fieldNums, new ParameterSetter(sm, ps, mappingDefinition));
                    }

                    JavaTypeMapping versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
                    if (versionMapping != null)
                    {
                        // Surrogate version - set the new version for the object
                        Object currentVersion = sm.getVersion();
                        Object nextOptimisticVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                        for (int k=0;k<versionStmtMapping.getNumberOfParameterOccurrences();k++)
                        {
                            versionMapping.setObject(ec, ps, versionStmtMapping.getParameterPositionsForOccurrence(k), nextOptimisticVersion);
                        }
                        sm.setTransactionalVersion(nextOptimisticVersion);
                    }
                    else if (vermd != null && vermd.getMemberName() != null)
                    {
                        // Version field - set the new version for the object
                        Object currentVersion = sm.getVersion();
                        Object nextOptimisticVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                        sm.setTransactionalVersion(nextOptimisticVersion);
                    }

                    if (multitenancyStmtMapping != null)
                    {
                        // Multitenancy mapping
                        String tenantId = ec.getTenantId();
                        if (tenantId == null)
                        {
                            NucleusLogger.PERSISTENCE.warn("Insert of object with multitenancy column but tenantId not set! Suggest that you set it.");
                        }
                        table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false).setObject(ec, ps, multitenancyStmtMapping.getParameterPositionsForOccurrence(0),
                            tenantId);
                    }

                    if (softDeleteStmtMapping != null)
                    {
                        // Soft-Delete mapping
                        table.getSurrogateMapping(SurrogateColumnType.SOFTDELETE, false).setObject(ec, ps, softDeleteStmtMapping.getParameterPositionsForOccurrence(0), Boolean.FALSE);
                    }

                    if (createUserStmtMapping != null)
                    {
                        // Set SURROGATE create-user
                        table.getSurrogateMapping(SurrogateColumnType.CREATE_USER, false).setObject(ec, ps, createUserStmtMapping.getParameterPositionsForOccurrence(0), 
                            ec.getCurrentUser());
                    }
                    if (createTimestampStmtMapping != null)
                    {
                        // Set SURROGATE create-timestamp
                        table.getSurrogateMapping(SurrogateColumnType.CREATE_TIMESTAMP, false).setObject(ec, ps, createTimestampStmtMapping.getParameterPositionsForOccurrence(0), 
                            new Timestamp(ec.getTransaction().getIsActive() ? ec.getTransaction().getBeginTime() : System.currentTimeMillis()));
                    }

                    if (updateUserStmtMapping != null)
                    {
                        // Set SURROGATE update-user
                        // TODO Do we need to specify this on INSERT? can they be nullable?
                        table.getSurrogateMapping(SurrogateColumnType.UPDATE_USER, false).setObject(ec, ps, updateUserStmtMapping.getParameterPositionsForOccurrence(0), "");
                    }
                    if (updateTimestampStmtMapping != null)
                    {
                        // Set SURROGATE update-timestamp
                        // TODO Do we need to specify this on INSERT? can they be nullable?
                        table.getSurrogateMapping(SurrogateColumnType.UPDATE_TIMESTAMP, false).setObject(ec, ps, updateTimestampStmtMapping.getParameterPositionsForOccurrence(0), null);
                    }

                    JavaTypeMapping discrimMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
                    if (discrimMapping != null && discriminatorStmtMapping!=null)
                    {
                        // Discriminator mapping
                        Object discVal = sm.getClassMetaData().getDiscriminatorValue();
                        for (int k=0;k<discriminatorStmtMapping.getNumberOfParameterOccurrences();k++)
                        {
                            discrimMapping.setObject(ec, ps, discriminatorStmtMapping.getParameterPositionsForOccurrence(k), discVal);
                        }
                    }

                    // External FK columns (optional)
                    if (externalFKStmtMappings != null)
                    {
                        for (int i=0;i<externalFKStmtMappings.length;i++)
                        {
                            Object fkValue = sm.getAssociatedValue(externalFKStmtMappings[i].getMapping());
                            if (fkValue != null)
                            {
                                // Need to provide the owner field number so PCMapping can work out if it is inserted yet
                                AbstractMemberMetaData ownerFmd = table.getMetaDataForExternalMapping(externalFKStmtMappings[i].getMapping(), MappingType.EXTERNAL_FK);
                                for (int k=0;k<externalFKStmtMappings[i].getNumberOfParameterOccurrences();k++)
                                {
                                    externalFKStmtMappings[i].getMapping().setObject(ec, ps,
                                        externalFKStmtMappings[i].getParameterPositionsForOccurrence(k), fkValue, null, ownerFmd.getAbsoluteFieldNumber());
                                }
                            }
                            else
                            {
                                // TODO What if the column is not nullable?
                                // We're inserting a null so don't need the owner field
                                for (int k=0;k<externalFKStmtMappings[i].getNumberOfParameterOccurrences();k++)
                                {
                                    externalFKStmtMappings[i].getMapping().setObject(ec, ps, externalFKStmtMappings[i].getParameterPositionsForOccurrence(k), null);
                                }
                            }
                        }
                    }

                    // External FK discriminator columns (optional)
                    if (externalFKDiscrimStmtMappings != null)
                    {
                        for (int i=0;i<externalFKDiscrimStmtMappings.length;i++)
                        {
                            Object discrimValue = sm.getAssociatedValue(externalFKDiscrimStmtMappings[i].getMapping());
                            for (int k=0;k<externalFKDiscrimStmtMappings[i].getNumberOfParameterOccurrences();k++)
                            {
                                externalFKDiscrimStmtMappings[i].getMapping().setObject(ec, ps, externalFKDiscrimStmtMappings[i].getParameterPositionsForOccurrence(k), discrimValue);
                            }
                        }
                    }

                    // External order columns (optional)
                    if (externalOrderStmtMappings != null)
                    {
                        for (int i=0;i<externalOrderStmtMappings.length;i++)
                        {
                            Object orderValue = sm.getAssociatedValue(externalOrderStmtMappings[i].getMapping());
                            if (orderValue == null)
                            {
                                // No order value so use -1
                                orderValue = Integer.valueOf(-1);
                            }
                            for (int k=0;k<externalOrderStmtMappings[i].getNumberOfParameterOccurrences();k++)
                            {
                                externalOrderStmtMappings[i].getMapping().setObject(ec, ps, externalOrderStmtMappings[i].getParameterPositionsForOccurrence(k), orderValue);
                            }
                        }
                    }

                    sqlControl.executeStatementUpdate(ec, mconn, insertStmt, ps, !batch);

                    if (hasIdentityColumn)
                    {
                        // Identity column was set in the datastore using auto-increment/identity/serial etc
                        Object newId = getInsertedIdentityValue(ec, sqlControl, sm, mconn, ps);
                        sm.setPostStoreNewObjectId(newId);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("052206", sm.getObjectAsPrintable(), 
                                IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId())));
                        }
                    }

                    // Execute any mapping actions on the insert of the fields (e.g Oracle CLOBs/BLOBs)
                    if (postSetMappings != null)
                    {
                        for (JavaTypeMapping m : postSetMappings)
                        {
                            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                            {
                                NucleusLogger.PERSISTENCE.debug(Localiser.msg("052222", sm.getObjectAsPrintable(), m.getMemberMetaData().getFullFieldName()));
                            }
                            m.performSetPostProcessing(sm);
                        }
                    }

                    // Update the insert status for this table via the StoreManager
                    storeMgr.setObjectIsInsertedToLevel(sm, table);

                    // Make sure all relation fields (1-1, N-1 with FK) we processed in the INSERT are attached.
                    // This is necessary because with a bidir relation and the other end attached we can just
                    // do the INSERT above first and THEN attach the other end here
                    // (if we did it the other way around we would get a NotYetFlushedException thrown above).
                    for (int i=0;i<relationFieldNumbers.length;i++)
                    {
                        Object value = sm.provideField(relationFieldNumbers[i]);
                        if (value != null && ec.getApiAdapter().isDetached(value))
                        {
                            Object valueAttached = ec.persistObjectInternal(value, null, PersistableObjectType.PC);
                            sm.replaceField(relationFieldNumbers[i], valueAttached);
                        }
                    }

                    // Perform reachability on all fields that have no datastore column (1-1 bi non-owner, N-1 bi join)
                    if (reachableFieldNumbers.length > 0)
                    {
                        int numberOfReachableFields = 0;
                        for (int i = 0; i < reachableFieldNumbers.length; i++)
                        {
                            if (reachableFieldNumbers[i] < sm.getClassMetaData().getMemberCount())
                            {
                                numberOfReachableFields++;
                            }
                        }
                        int[] fieldNums = new int[numberOfReachableFields];
                        int j = 0;
                        for (int i = 0; i < reachableFieldNumbers.length; i++)
                        {
                            if (reachableFieldNumbers[i] < sm.getClassMetaData().getMemberCount())
                            {
                                fieldNums[j++] = reachableFieldNumbers[i];
                            }
                        }

                        mappingDefinition = new StatementClassMapping();
                        for (int i=0;i<retrievedStmtMappings.length;i++)
                        {
                            if (retrievedStmtMappings[i] != null)
                            {
                                mappingDefinition.addMappingForMember(i, retrievedStmtMappings[i]);
                            }
                        }
                        NucleusLogger.PERSISTENCE.debug("Performing reachability on fields " + StringUtils.intArrayToString(fieldNums));
                        sm.provideFields(fieldNums, new ParameterSetter(sm, ps, mappingDefinition));
                    }
                }
                finally
                {
                    sqlControl.closeStatement(mconn, ps);
                }
            }
            finally
            {
                mconn.release();
            }
        }
        catch (SQLException e)
        {
            String msg = Localiser.msg("052208", sm.getObjectAsPrintable(), insertStmt, e.getMessage());
            NucleusLogger.DATASTORE_PERSIST.warn(msg);
            List<Exception> exceptions = new ArrayList<>();
            exceptions.add(e);
            while ((e = e.getNextException()) != null)
            {
                exceptions.add(e);
            }
            throw new NucleusDataStoreException(msg, exceptions.toArray(new Throwable[exceptions.size()]));
        }

        // Execute any mapping actions now that we have inserted the element (things like inserting any association parent-child).
        if (mappingCallbacks != null)
        {
            for (MappingCallbacks m : mappingCallbacks)
            {
                try
                {
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("052209", IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId()), 
                            ((JavaTypeMapping)m).getMemberMetaData().getFullFieldName()));
                    }
                    m.postInsert(sm);
                }
                catch (NotYetFlushedException e)
                {
                    sm.updateFieldAfterInsert(e.getPersistable(), ((JavaTypeMapping) m).getMemberMetaData().getAbsoluteFieldNumber());
                }
            }
        }
    }

    /**
     * Method to obtain the identity attributed by the datastore when using auto-increment/IDENTITY/SERIAL on a field.
     * @param ec execution context
     * @param sqlControl SQLController
     * @param sm StateManager of the object
     * @param mconn The Connection
     * @param ps PreparedStatement for the INSERT
     * @return The identity
     * @throws SQLException Thrown if an error occurs retrieving the identity
     */
    private Object getInsertedIdentityValue(ExecutionContext ec, SQLController sqlControl, DNStateManager sm, ManagedConnection mconn, PreparedStatement ps)
    throws SQLException
    {
        Object identityValue = null;

        RDBMSStoreManager storeMgr = table.getStoreManager();
        if (storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.GET_GENERATED_KEYS_STATEMENT))
        {
            // Try getGeneratedKeys() method to avoid extra SQL calls (only in more recent JDBC drivers)
            ResultSet rs = null;
            try
            {
                rs = ps.getGeneratedKeys();
                if (rs != null && rs.next())
                {
                    identityValue = rs.getObject(1);
                }
            }
            catch (Throwable e)
            {
                // Not supported maybe (e.g HSQL), or the driver is too old
            }
            finally
            {
                if (rs != null)
                {
                    rs.close();
                }
            }
        }

        if (identityValue == null)
        {
            // Not found, so try the native method for retrieving it
            String columnName = null;
            JavaTypeMapping idMapping = table.getIdMapping(); // TODO Confirm this is ok for datastore AND application id cases. What if the column is not PK?
            if (idMapping != null)
            {
                for (int i=0;i<idMapping.getNumberOfColumnMappings();i++)
                {
                    Column col = idMapping.getColumnMapping(i).getColumn();
                    if (col.isIdentity())
                    {
                        columnName = col.getIdentifier().toString();
                        break;
                    }
                }
            }
            String autoIncStmt = storeMgr.getDatastoreAdapter().getIdentityLastValueStmt(table, columnName);
            PreparedStatement psAutoIncrement = sqlControl.getStatementForQuery(mconn, autoIncStmt);
            ResultSet rs = null;
            try
            {
                rs = sqlControl.executeStatementQuery(ec, mconn, autoIncStmt, psAutoIncrement);
                if (rs.next())
                {
                    identityValue = rs.getObject(1);
                }
            }
            finally
            {
                if (rs != null)
                {
                    rs.close();
                }
                if (psAutoIncrement != null)
                {
                    psAutoIncrement.close();
                }                            
            }
        }

        if (identityValue == null)
        {
            throw new NucleusDataStoreException(Localiser.msg("052205", this.table));
        }

        return identityValue;
    }

    /**
     * Internal class to provide mapping consumption for an INSERT.
     */
    private class InsertMappingConsumer implements MappingConsumer
    {
        /** Numbers of all fields to be inserted. */
        List<Integer> insertFields = new ArrayList<>();

        /** Numbers of all PK fields. */
        List<Integer> pkFields = new ArrayList<>();

        /** Numbers of all reachable fields (with no datastore column). */
        List<Integer> reachableFields = new ArrayList<>();

        /** Numbers of all relations fields (bidir that may already be attached when persisting). */
        List<Integer> relationFields = new ArrayList<>();

        StringBuilder columnNames = new StringBuilder();
        StringBuilder columnValues = new StringBuilder();

        Map<String, Integer> assignedColumns = new HashMap<>();

        /** Mappings that require post-set processing. */
        List<JavaTypeMapping> postSetMappings;

        /** Mappings that require callbacks calling. */
        List<MappingCallbacks> callbackMappings = null;

        boolean initialized = false;

        int paramIndex;

        /** one StatementExpressionIndex for each field **/
        private StatementMappingIndex[] statementMappings;

        /** statement indexes for fields to be "retrieved". */
        private StatementMappingIndex[] retrievedStatementMappings;

        /** StatementExpressionIndex for version **/
        private StatementMappingIndex versionStatementMapping;

        /** StatementExpressionIndex for discriminator **/
        private StatementMappingIndex discriminatorStatementMapping;

        /** StatementExpressionIndex for multi-tenancy. **/
        private StatementMappingIndex multitenancyStatementMapping;

        /** StatementExpressionIndex for SURROGATE soft-delete. **/
        private StatementMappingIndex softDeleteStatementMapping;

        /** StatementExpressionIndex for SURROGATE create-user. **/
        private StatementMappingIndex createUserStatementMapping;
        /** StatementExpressionIndex for SURROGATE create-timestamp. **/
        private StatementMappingIndex createTimestampStatementMapping;
        /** StatementExpressionIndex for SURROGATE update-user. **/
        private StatementMappingIndex updateUserStatementMapping;
        /** StatementExpressionIndex for SURROGATE update-timestamp. **/
        private StatementMappingIndex updateTimestampStatementMapping;

        private StatementMappingIndex[] externalFKStmtExprIndex;

        private StatementMappingIndex[] externalFKDiscrimStmtExprIndex;

        private StatementMappingIndex[] externalOrderStmtExprIndex;

        private final ClassLoaderResolver clr;
        private final AbstractClassMetaData cmd;

        /**
         * Constructor
         * @param clr the ClassLoaderResolver
         * @param cmd ClassMetaData
         * @param initialParamIndex the initial index to use for the JDBC Parameter
         */
        public InsertMappingConsumer(ClassLoaderResolver clr, AbstractClassMetaData cmd, int initialParamIndex)
        {
            this.clr = clr;
            this.cmd = cmd;
            this.paramIndex = initialParamIndex;
        }

        public void preConsumeMapping(int highestFieldNumber)
        {
            if (!initialized)
            {
                statementMappings = new StatementMappingIndex[highestFieldNumber];
                retrievedStatementMappings = new StatementMappingIndex[highestFieldNumber];
                initialized = true;
            }
        }

        /**
         * Consumes a mapping for a member.
         * @param m The mapping.
         * @param mmd MetaData for the member
         */
        public void consumeMapping(JavaTypeMapping m, AbstractMemberMetaData mmd)
        {
            if (!mmd.getAbstractClassMetaData().isSameOrAncestorOf(cmd))
            {
                // Make sure we only accept mappings from the correct part of any inheritance tree
                return;
            }
            if (m.includeInInsertStatement())
            {
                if (m.getNumberOfColumnMappings() == 0 && (m instanceof PersistableMapping || m instanceof ReferenceMapping))
                {
                    // Reachable Fields (that relate to this object but have no column in the table)
                    retrievedStatementMappings[mmd.getAbsoluteFieldNumber()] = new StatementMappingIndex(m);
                    RelationType relationType = mmd.getRelationType(clr);
                    if (relationType == RelationType.ONE_TO_ONE_BI)
                    {
                        if (mmd.getMappedBy() != null)
                        {
                            // 1-1 Non-owner bidirectional field (no datastore columns)
                            reachableFields.add(Integer.valueOf(mmd.getAbsoluteFieldNumber()));
                        }
                    }
                    else if (relationType == RelationType.MANY_TO_ONE_BI)
                    {
                        AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                        if (mmd.getJoinMetaData() != null || relatedMmds[0].getJoinMetaData() != null)
                        {
                            // N-1 bidirectional field using join (no datastore columns)
                            reachableFields.add(Integer.valueOf(mmd.getAbsoluteFieldNumber()));
                        }
                    }
                    // TODO What about 1-N non-owner
                }
                else
                {
                    // Fields to be "inserted" (that have a datastore column)

                    // Check if the field is "insertable"
                	// a). JDO member extension
                    if (mmd.hasExtension(MetaData.EXTENSION_MEMBER_INSERTABLE) && mmd.getValueForExtension(MetaData.EXTENSION_MEMBER_INSERTABLE).equalsIgnoreCase("false"))
                    {
                        return;
                    }

                    // b). JPA column
                    ColumnMetaData[] colmds = mmd.getColumnMetaData();
                    if (colmds != null && colmds.length > 0)
                    {
                        for (int i=0;i<colmds.length;i++)
                        {
                            if (!colmds[i].getInsertable())
                            {
                                // Not to be inserted
                                return;
                            }
                        }
                    }

                    RelationType relationType = mmd.getRelationType(clr);
                    if (relationType == RelationType.ONE_TO_ONE_BI)
                    {
                        if (mmd.getMappedBy() == null)
                        {
                            // 1-1 Owner bidirectional field using FK (in this table)
                        }
                    }
                    else if (relationType == RelationType.MANY_TO_ONE_BI)
                    {
                        AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                        if (mmd.getJoinMetaData() == null && relatedMmds[0].getJoinMetaData() == null)
                        {
                            // N-1 bidirectional field using FK (in this table)
                            relationFields.add(Integer.valueOf(mmd.getAbsoluteFieldNumber()));
                        }
                    }

                    statementMappings[mmd.getAbsoluteFieldNumber()] = new StatementMappingIndex(m);

                    // create the expressions index (columns index)
                    int parametersIndex[] = new int[m.getNumberOfColumnMappings()];
                    for (int j = 0; j < parametersIndex.length; j++)
                    {
                        // check if the column was not already assigned
                        Column c = m.getColumnMapping(j).getColumn();
                        DatastoreIdentifier columnId = c.getIdentifier();
                        boolean columnExists = assignedColumns.containsKey(columnId.toString());
                        if (columnExists)
                        {
                            parametersIndex[j] = assignedColumns.get(c.getIdentifier().toString()).intValue();
                        }

                        // Either we are a field in a secondary table.
                        // Or we are a subclass table.
                        // Or we are not datastore attributed.
                        if (table instanceof SecondaryTable || !table.isBaseDatastoreClass() ||
                            (!table.getStoreManager().isValueGenerationStrategyDatastoreAttributed(cmd, mmd.getAbsoluteFieldNumber()) && !c.isIdentity()))
                        {
                            if (!columnExists)
                            {
                                if (columnNames.length() > 0)
                                {
                                    columnNames.append(',');
                                    columnValues.append(',');
                                }
                                columnNames.append(columnId);
                                columnValues.append(m.getColumnMapping(j).getInsertionInputParameter());
                            }

                            if (m.getColumnMapping(j).insertValuesOnInsert())
                            {
                                // only add fields to be replaced by the real values only if the param value has ?
                                Integer abs_field_num = Integer.valueOf(mmd.getAbsoluteFieldNumber());
                                if (mmd.isPrimaryKey())
                                {
                                    if (!pkFields.contains(abs_field_num))
                                    {
                                        pkFields.add(abs_field_num);
                                    }
                                }
                                else if (!insertFields.contains(abs_field_num))
                                {
                                    insertFields.add(abs_field_num);
                                }

                                if (columnExists)
                                {
                                    parametersIndex[j] = assignedColumns.get(c.getIdentifier().toString()).intValue();
                                }
                                else
                                {
                                    parametersIndex[j] = paramIndex++;
                                }
                            }

                            if (!columnExists)
                            {
                                assignedColumns.put(c.getIdentifier().toString(), parametersIndex[j]);
                            }
                        }
                        else
                        {
                            hasIdentityColumn = true;
                        }
                    }

                    statementMappings[mmd.getAbsoluteFieldNumber()].addParameterOccurrence(parametersIndex);
                }
            }
            if (m.requiresSetPostProcessing())
            {
                if (postSetMappings == null)
                {
                    postSetMappings = new ArrayList<>();
                }
                postSetMappings.add(m);
            }
            if (m instanceof MappingCallbacks)
            {
                if (callbackMappings == null)
                {
                    callbackMappings = new ArrayList<>();
                }
                callbackMappings.add((MappingCallbacks)m);
            }
        }

        /**
         * Consumes a (surrogate) mapping (i.e not associated to a field).
         * @param m The mapping.
         * @param mappingType the Mapping type
         */
        public void consumeMapping(JavaTypeMapping m, MappingType mappingType)
        {
            if (mappingType == MappingType.VERSION)
            {
                // Surrogate version column
                JavaTypeMapping versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
                if (versionMapping != null)
                {
                    String val = versionMapping.getColumnMapping(0).getUpdateInputParameter();
                    if (columnNames.length() > 0)
                    {
                        columnNames.append(',');
                        columnValues.append(',');
                    }
                    columnNames.append(versionMapping.getColumnMapping(0).getColumn().getIdentifier());
                    columnValues.append(val);

                    versionStatementMapping = new StatementMappingIndex(versionMapping);
                    int[] param = { paramIndex++ };
                    versionStatementMapping.addParameterOccurrence(param);
                }
                else
                {
                    versionStatementMapping = null;
                }
            }
            else if (mappingType == MappingType.DISCRIMINATOR)
            {
                // Surrogate discriminator column
                JavaTypeMapping discrimMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
                if (discrimMapping != null)
                {
                    String val = discrimMapping.getColumnMapping(0).getUpdateInputParameter();

                    if (columnNames.length() > 0)
                    {
                        columnNames.append(',');
                        columnValues.append(',');
                    }
                    columnNames.append(discrimMapping.getColumnMapping(0).getColumn().getIdentifier());
                    columnValues.append(val);
                    discriminatorStatementMapping = new StatementMappingIndex(discrimMapping);
                    int[] param = { paramIndex++ };
                    discriminatorStatementMapping.addParameterOccurrence(param);
                }
                else
                {
                    discriminatorStatementMapping = null;
                }
            }
            else if (mappingType == MappingType.DATASTORE_ID)
            {
                // Surrogate datastore id column
                if (table.getIdentityType() == IdentityType.DATASTORE)
                {
                    if (!table.isObjectIdDatastoreAttributed() || !table.isBaseDatastoreClass())
                    {
                        Iterator iterator = key.getColumns().iterator();
                        if (columnNames.length() > 0)
                        {
                            columnNames.append(',');
                            columnValues.append(',');
                        }
                        columnNames.append(((Column) iterator.next()).getIdentifier().toString());
                        columnValues.append("?");
                        paramIndex++;
                    }
                    else
                    {
                        hasIdentityColumn = true;
                    }
                }
            }
            else if (mappingType == MappingType.EXTERNAL_FK)
            {
                // External FK mapping (1-N uni)
                externalFKStmtExprIndex = processExternalMapping(m, statementMappings, externalFKStmtExprIndex);
            }
            else if (mappingType == MappingType.EXTERNAL_FK_DISCRIMINATOR)
            {
                // External FK discriminator mapping (1-N uni with shared FK)
                externalFKDiscrimStmtExprIndex = processExternalMapping(m, statementMappings, externalFKDiscrimStmtExprIndex);
            }
            else if (mappingType == MappingType.EXTERNAL_INDEX)
            {
                // External FK order mapping (1-N uni List)
                externalOrderStmtExprIndex = processExternalMapping(m, statementMappings, externalOrderStmtExprIndex);
            }
            else if (mappingType == MappingType.MULTITENANCY)
            {
                // Multitenancy column
                JavaTypeMapping multitenancyMapping = table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false);
                String val = multitenancyMapping.getColumnMapping(0).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(multitenancyMapping.getColumnMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                multitenancyStatementMapping = new StatementMappingIndex(multitenancyMapping);
                int[] param = { paramIndex++ };
                multitenancyStatementMapping.addParameterOccurrence(param);
            }
            else if (mappingType == MappingType.SOFTDELETE)
            {
                // SoftDelete column
                JavaTypeMapping softDeleteMapping = table.getSurrogateMapping(SurrogateColumnType.SOFTDELETE, false);
                String val = softDeleteMapping.getColumnMapping(0).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(softDeleteMapping.getColumnMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                softDeleteStatementMapping = new StatementMappingIndex(softDeleteMapping);
                int[] param = { paramIndex++ };
                softDeleteStatementMapping.addParameterOccurrence(param);
            }
            else if (mappingType == MappingType.CREATEUSER)
            {
                // CreateUser column
                JavaTypeMapping createUserMapping = table.getSurrogateMapping(SurrogateColumnType.CREATE_USER, false);
                String val = createUserMapping.getColumnMapping(0).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(createUserMapping.getColumnMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                createUserStatementMapping = new StatementMappingIndex(createUserMapping);
                int[] param = { paramIndex++ };
                createUserStatementMapping.addParameterOccurrence(param);
            }
            else if (mappingType == MappingType.CREATETIMESTAMP)
            {
                // CreateTimestamp column
                JavaTypeMapping createTimestampMapping = table.getSurrogateMapping(SurrogateColumnType.CREATE_TIMESTAMP, false);
                String val = createTimestampMapping.getColumnMapping(0).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(createTimestampMapping.getColumnMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                createTimestampStatementMapping = new StatementMappingIndex(createTimestampMapping);
                int[] param = { paramIndex++ };
                createTimestampStatementMapping.addParameterOccurrence(param);
            }
            else if (mappingType == MappingType.UPDATEUSER)
            {
                // UpdateUser column - we add it to the INSERT in case it is not nullable
                JavaTypeMapping updateUserMapping = table.getSurrogateMapping(SurrogateColumnType.UPDATE_USER, false);
                String val = updateUserMapping.getColumnMapping(0).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(updateUserMapping.getColumnMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                updateUserStatementMapping = new StatementMappingIndex(updateUserMapping);
                int[] param = { paramIndex++ };
                updateUserStatementMapping.addParameterOccurrence(param);
            }
            else if (mappingType == MappingType.UPDATETIMESTAMP)
            {
                // UpdateTimestamp column - we add it to the INSERT in case it is not nullable
                JavaTypeMapping updateTimestampMapping = table.getSurrogateMapping(SurrogateColumnType.UPDATE_TIMESTAMP, false);
                String val = updateTimestampMapping.getColumnMapping(0).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(updateTimestampMapping.getColumnMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                updateTimestampStatementMapping = new StatementMappingIndex(updateTimestampMapping);
                int[] param = { paramIndex++ };
                updateTimestampStatementMapping.addParameterOccurrence(param);
            }
        }

        /**
         * Consumer a column without mapping.
         * @param col The column
         */
        public void consumeUnmappedColumn(Column col)
        {
            if (columnNames.length() > 0)
            {
                columnNames.append(',');
                columnValues.append(',');
            }
            columnNames.append(col.getIdentifier());

            ColumnMetaData colmd = col.getColumnMetaData();
            String value = colmd.getInsertValue();
            if (value != null && value.equalsIgnoreCase("#NULL"))
            {
                value = null;
            }

            if (MetaDataUtils.isJdbcTypeString(colmd.getJdbcType()))
            {
                // String-based so add quoting. Do all RDBMS accept single-quote?
                if (value != null)
                {
                    value = "'" + value + "'";
                }
                else if (!col.isNullable())
                {
                    value = "''";
                }
            }

            columnValues.append(value);
        }

        /**
         * Convenience method to process an external mapping.
         * @param mapping The external mapping
         * @param fieldStmtExprIndex The indices for the fields
         * @param stmtExprIndex The current external mapping indices
         * @return The updated external mapping indices
         */
        private StatementMappingIndex[] processExternalMapping(JavaTypeMapping mapping, 
                StatementMappingIndex[] fieldStmtExprIndex, StatementMappingIndex[] stmtExprIndex)
        {
            // Check that we dont already have this as a field
            for (int i=0;i<fieldStmtExprIndex.length;i++)
            {
                if (fieldStmtExprIndex[i] != null && fieldStmtExprIndex[i].getMapping() == mapping)
                {
                    // Already present so ignore it
                    return stmtExprIndex;
                }
            }

            int pos = 0;
            if (stmtExprIndex == null)
            {
                stmtExprIndex = new StatementMappingIndex[1];
                pos = 0;
            }
            else
            {
                // Check that we dont already have this external order mapping (shared?)
                for (int i=0;i<stmtExprIndex.length;i++)
                {
                    if (stmtExprIndex[i].getMapping() == mapping)
                    {
                        // Shared order mapping so ignore it
                        return stmtExprIndex;
                    }
                }

                StatementMappingIndex[] tmpStmtExprIndex = stmtExprIndex;
                stmtExprIndex = new StatementMappingIndex[tmpStmtExprIndex.length+1];
                for (int i=0;i<tmpStmtExprIndex.length;i++)
                {
                    stmtExprIndex[i] = tmpStmtExprIndex[i];
                }
                pos = tmpStmtExprIndex.length;
            }
            stmtExprIndex[pos] = new StatementMappingIndex(mapping);
            int[] param = new int[mapping.getNumberOfColumnMappings()];
            for (int i=0;i<mapping.getNumberOfColumnMappings();i++)
            {
                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(mapping.getColumnMapping(i).getColumn().getIdentifier());
                columnValues.append(mapping.getColumnMapping(i).getUpdateInputParameter());
                param[i] = paramIndex++;
            }
            stmtExprIndex[pos].addParameterOccurrence(param);
            
            return stmtExprIndex;
        }

        public List<JavaTypeMapping> getPostSetMappings()
        {
            return postSetMappings;
        }

        public List<MappingCallbacks> getMappingCallbacks()
        {
            return callbackMappings;
        }

        /**
         * Accessor for the numbers of the fields to be inserted (excluding PK fields).
         * @return the array of field numbers 
         */
        public int[] getInsertFieldNumbers()
        {
            int[] fieldNumbers = new int[insertFields.size()];
            for (int i = 0; i < insertFields.size(); ++i)
            {
                fieldNumbers[i] = insertFields.get(i).intValue();
            }
            return fieldNumbers;
        }

        /**
         * Accessor for the numbers of the PK fields.
         * @return the array of primary key field numbers 
         */
        public int[] getPrimaryKeyFieldNumbers()
        {
            int[] fieldNumbers = new int[pkFields.size()];
            for (int i = 0; i < pkFields.size(); i++)
            {
                fieldNumbers[i] = pkFields.get(i).intValue();
            }
            return fieldNumbers;
        }

        /**
         * Accessor for the numbers of the reachable fields (not inserted).
         * @return the array of field numbers 
         */
        public int[] getReachableFieldNumbers()
        {
            int[] fieldNumbers = new int[reachableFields.size()];
            for (int i = 0; i < reachableFields.size(); ++i)
            {
                fieldNumbers[i] = reachableFields.get(i).intValue();
            }
            return fieldNumbers;
        }

        /**
         * Accessor for the numbers of the relation fields (not inserted).
         * @return the array of field numbers 
         */
        public int[] getRelationFieldNumbers()
        {
            int[] fieldNumbers = new int[relationFields.size()];
            for (int i = 0; i < relationFields.size(); ++i)
            {
                fieldNumbers[i] = relationFields.get(i).intValue();
            }
            return fieldNumbers;
        }

        /** 
         * Obtain the mappings for fields in the statement
         * @return the array of StatementMappingIndex
         */
        public StatementMappingIndex[] getStatementMappings()
        {
            return statementMappings;
        }

        /** 
         * Obtain the StatementExpressionIndex for the "reachable" fields.
         * @return the array of StatementExpressionIndex indexed by absolute field numbers
         */
        public StatementMappingIndex[] getReachableStatementMappings()
        {
            return retrievedStatementMappings;
        }

        /** 
         * Obtain the mapping for the version in the statement
         * @return the StatementMappingIndex
         */
        public StatementMappingIndex getVersionStatementMapping()
        {
            return versionStatementMapping;
        }

        /** 
         * Obtain the mapping for the discriminator in the statement
         * @return the StatementMappingIndex
         */
        public StatementMappingIndex getDiscriminatorStatementMapping()
        {
            return discriminatorStatementMapping;
        }

        /** 
         * Obtain the mapping for multitenancy in the statement
         * @return the StatementMappingIndex
         */
        public StatementMappingIndex getMultitenancyStatementMapping()
        {
            return multitenancyStatementMapping;
        }

        /** 
         * Obtain the mapping for soft-delete in the statement
         * @return the StatementMappingIndex
         */
        public StatementMappingIndex getSoftDeleteStatementMapping()
        {
            return softDeleteStatementMapping;
        }

        public StatementMappingIndex getCreateUserStatementMapping()
        {
            return createUserStatementMapping;
        }

        public StatementMappingIndex getCreateTimestampStatementMapping()
        {
            return createTimestampStatementMapping;
        }

        public StatementMappingIndex getUpdateUserStatementMapping()
        {
            return updateUserStatementMapping;
        }

        public StatementMappingIndex getUpdateTimestampStatementMapping()
        {
            return updateTimestampStatementMapping;
        }

        /** 
         * Obtain the mapping for any external FKs in the statement
         * @return the StatementMappingIndex
         */
        public StatementMappingIndex[] getExternalFKStatementMapping()
        {
            return externalFKStmtExprIndex;
        }

        /** 
         * Obtain the mapping for any external FK discriminators in the statement.
         * @return the StatementMappingIndex
         */
        public StatementMappingIndex[] getExternalFKDiscrimStatementMapping()
        {
            return externalFKDiscrimStmtExprIndex;
        }

        /** 
         * Obtain the mapping for any external indexes in the statement
         * @return the StatementMappingIndex
         */
        public StatementMappingIndex[] getExternalOrderStatementMapping()
        {
            return externalOrderStmtExprIndex;
        }

        /**
         * Obtain the insert statement
         * @return the SQL statement
         */
        public String getInsertStmt()
        {
            // Construct the statement for the INSERT
            if (columnNames.length() > 0 && columnValues.length() > 0)
            {
                return "INSERT INTO " + table.toString() + " (" + columnNames + ") VALUES (" + columnValues + ")";
            }

            // No columns in the INSERT statement
            return table.getStoreManager().getDatastoreAdapter().getInsertStatementForNoColumns(table);
        }
    }
}