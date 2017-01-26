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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NotYetFlushedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ActivityState;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.SecondaryTable;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Class to provide a means of insertion of records to a data store. Extends basic request class 
 * implementing the execute method to do a JDBC insert operation.
 * <p>
 * When inserting an object with inheritance this will involve 1 InsertRequest
 * for each table involved. So if we have a class B that extends class A and they
 * both use "new-table" inheritance strategy, we will have 2 InsertRequests, one for
 * table A, and one for table B.
 * </p>
 * <p>
 * When the InsertRequest starts to populate its statement and it has a PC field, this calls 
 * PersistableMapping.setObject(). This then checks if the other PC object is yet persistent 
 * and, if not, will persist it before processing this objects INSERT. This forms the key to 
 * "persistence-by-reachability".
 * </p>
 */
public class InsertRequest extends Request
{
    private static final int IDPARAMNUMBER = 1;

    private final MappingCallbacks[] callbacks;

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
        table.provideSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, consumer);
        table.provideSurrogateMapping(SurrogateColumnType.MULTITENANCY, consumer);
        table.provideSurrogateMapping(SurrogateColumnType.SOFTDELETE, consumer);
        table.provideExternalMappings(consumer, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
        table.provideExternalMappings(consumer, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM);
        table.provideExternalMappings(consumer, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
        table.provideUnmappedColumns(consumer);

        callbacks = (MappingCallbacks[])consumer.getMappingCallbacks().toArray(new MappingCallbacks[consumer.getMappingCallbacks().size()]);
        stmtMappings = consumer.getStatementMappings();
        versionStmtMapping = consumer.getVersionStatementMapping();
        discriminatorStmtMapping = consumer.getDiscriminatorStatementMapping();
        multitenancyStmtMapping = consumer.getMultitenancyStatementMapping();
        softDeleteStmtMapping = consumer.getSoftDeleteStatementMapping();

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
     * @param op The ObjectProvider for the record to be inserted
     */
    public void execute(ObjectProvider op)
    {
        ExecutionContext ec = op.getExecutionContext();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are inserting
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("052207", op.getObjectAsPrintable(), table));
        }

        try
        {
            VersionMetaData vermd = table.getVersionMetaData();
            RDBMSStoreManager storeMgr = table.getStoreManager();
            if (vermd != null && vermd.getFieldName() != null)
            {
                // Version field - Update the version in the object
                AbstractMemberMetaData verfmd = ((AbstractClassMetaData)vermd.getParent()).getMetaDataForMember(vermd.getFieldName());
                Object currentVersion = op.getVersion();
                if (currentVersion instanceof Number)
                {
                    // Cater for Integer based versions
                    currentVersion = Long.valueOf(((Number)currentVersion).longValue());
                }

                Object nextOptimisticVersion = ec.getNextVersion(vermd, currentVersion);
                if (verfmd.getType() == Integer.class || verfmd.getType() == int.class)
                {
                    // Cater for Integer based versions TODO Generalise this
                    nextOptimisticVersion = Integer.valueOf(((Long)nextOptimisticVersion).intValue());
                }
                op.replaceField(verfmd.getAbsoluteFieldNumber(), nextOptimisticVersion);
            }

            // Set the state to "inserting" (may already be at this state if multiple inheritance level INSERT)
            op.changeActivityState(ActivityState.INSERTING);

            SQLController sqlControl = storeMgr.getSQLController();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, insertStmt, batch,
                    hasIdentityColumn && storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.GET_GENERATED_KEYS_STATEMENT));

                try
                {
                    StatementClassMapping mappingDefinition = new StatementClassMapping();
                    StatementMappingIndex[] idxs = stmtMappings;
                    for (int i=0;i<idxs.length;i++)
                    {
                        if (idxs[i] != null)
                        {
                            mappingDefinition.addMappingForMember(i, idxs[i]);
                        }
                    }

                    // Provide the primary key field(s)
                    if (table.getIdentityType() == IdentityType.DATASTORE)
                    {
                        if (!table.isObjectIdDatastoreAttributed() || !table.isBaseDatastoreClass())
                        {
                            int[] paramNumber = {IDPARAMNUMBER};
                            table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false).setObject(ec, ps, paramNumber, op.getInternalObjectId());
                        }
                    }
                    else if (table.getIdentityType() == IdentityType.APPLICATION)
                    {
                        op.provideFields(pkFieldNumbers, storeMgr.getFieldManagerForStatementGeneration(op, ps, mappingDefinition));
                    }

                    // Provide all non-key fields needed for the insert.
                    // This provides "persistence-by-reachability" for these fields
                    if (insertFieldNumbers.length > 0)
                    {
                        int numberOfFieldsToProvide = 0;
                        for (int i = 0; i < insertFieldNumbers.length; i++)
                        {
                            if (insertFieldNumbers[i] < op.getClassMetaData().getMemberCount())
                            {
                                AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(insertFieldNumbers[i]);
                                if (mmd.hasExtension("create-timestamp"))
                                {
                                    // TODO Support surrogate create-timestamp
                                    // Set "create-timestamp" to time for the start of this transaction
                                    op.replaceField(insertFieldNumbers[i], new Timestamp(ec.getTransaction().getBeginTime()));
                                }
                                numberOfFieldsToProvide++;
                            }
                        }
                        int j = 0;
                        int[] fieldNums = new int[numberOfFieldsToProvide];
                        for (int i = 0; i < insertFieldNumbers.length; i++)
                        {
                            if (insertFieldNumbers[i] < op.getClassMetaData().getMemberCount())
                            {
                                fieldNums[j++] = insertFieldNumbers[i];
                            }
                        }
                        op.provideFields(fieldNums, storeMgr.getFieldManagerForStatementGeneration(op, ps, mappingDefinition));
                    }

                    JavaTypeMapping versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
                    if (versionMapping != null)
                    {
                        // Surrogate version - set the new version for the object
                        Object currentVersion = op.getVersion();
                        Object nextOptimisticVersion = ec.getNextVersion(vermd, currentVersion);
                        for (int k=0;k<versionStmtMapping.getNumberOfParameterOccurrences();k++)
                        {
                            versionMapping.setObject(ec, ps, versionStmtMapping.getParameterPositionsForOccurrence(k), nextOptimisticVersion);
                        }
                        op.setTransactionalVersion(nextOptimisticVersion);
                    }
                    else if (vermd != null && vermd.getFieldName() != null)
                    {
                        // Version field - set the new version for the object
                        Object currentVersion = op.getVersion();
                        Object nextOptimisticVersion = ec.getNextVersion(vermd, currentVersion);
                        op.setTransactionalVersion(nextOptimisticVersion);
                    }

                    if (multitenancyStmtMapping != null)
                    {
                        // Multitenancy mapping
                        table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false).setObject(ec, ps, multitenancyStmtMapping.getParameterPositionsForOccurrence(0),
                            ec.getNucleusContext().getMultiTenancyId(ec, op.getClassMetaData()));
                    }

                    if (softDeleteStmtMapping != null)
                    {
                        // Soft-Delete mapping
                        table.getSurrogateMapping(SurrogateColumnType.SOFTDELETE, false).setObject(ec, ps, softDeleteStmtMapping.getParameterPositionsForOccurrence(0), Boolean.FALSE);
                    }

                    JavaTypeMapping discrimMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
                    if (discrimMapping != null)
                    {
                        // Discriminator mapping
                        Object discVal = op.getClassMetaData().getDiscriminatorValue();
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
                            Object fkValue = op.getAssociatedValue(externalFKStmtMappings[i].getMapping());
                            if (fkValue != null)
                            {
                                // Need to provide the owner field number so PCMapping can work out if it is inserted yet
                                AbstractMemberMetaData ownerFmd = 
                                    table.getMetaDataForExternalMapping(externalFKStmtMappings[i].getMapping(), 
                                        MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                for (int k=0;k<externalFKStmtMappings[i].getNumberOfParameterOccurrences();k++)
                                {
                                    externalFKStmtMappings[i].getMapping().setObject(ec, ps,
                                        externalFKStmtMappings[i].getParameterPositionsForOccurrence(k), 
                                        fkValue, null, ownerFmd.getAbsoluteFieldNumber());
                                }
                            }
                            else
                            {
                                // TODO What if the column is not nullable?
                                // We're inserting a null so don't need the owner field
                                for (int k=0;k<externalFKStmtMappings[i].getNumberOfParameterOccurrences();k++)
                                {
                                    externalFKStmtMappings[i].getMapping().setObject(ec, ps,
                                        externalFKStmtMappings[i].getParameterPositionsForOccurrence(k), null);
                                }
                            }
                        }
                    }

                    // External FK discriminator columns (optional)
                    if (externalFKDiscrimStmtMappings != null)
                    {
                        for (int i=0;i<externalFKDiscrimStmtMappings.length;i++)
                        {
                            Object discrimValue = op.getAssociatedValue(externalFKDiscrimStmtMappings[i].getMapping());
                            for (int k=0;k<externalFKDiscrimStmtMappings[i].getNumberOfParameterOccurrences();k++)
                            {
                                externalFKDiscrimStmtMappings[i].getMapping().setObject(ec, ps, 
                                    externalFKDiscrimStmtMappings[i].getParameterPositionsForOccurrence(k), discrimValue);
                            }
                        }
                    }

                    // External order columns (optional)
                    if (externalOrderStmtMappings != null)
                    {
                        for (int i=0;i<externalOrderStmtMappings.length;i++)
                        {
                            Object orderValue = op.getAssociatedValue(externalOrderStmtMappings[i].getMapping());
                            if (orderValue == null)
                            {
                                // No order value so use -1
                                orderValue = Integer.valueOf(-1);
                            }
                            for (int k=0;k<externalOrderStmtMappings[i].getNumberOfParameterOccurrences();k++)
                            {
                                externalOrderStmtMappings[i].getMapping().setObject(ec, ps, 
                                    externalOrderStmtMappings[i].getParameterPositionsForOccurrence(k), orderValue);
                            }
                        }
                    }

                    sqlControl.executeStatementUpdate(ec, mconn, insertStmt, ps, !batch);
                    if (hasIdentityColumn)
                    {
                        // Identity was set in the datastore using auto-increment/identity/serial etc
                        Object newId = getInsertedDatastoreIdentity(ec, sqlControl, op, mconn, ps);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("052206",
                                op.getObjectAsPrintable(), newId));
                        }
                        op.setPostStoreNewObjectId(newId);
                    }

                    // Execute any mapping actions on the insert of the fields (e.g Oracle CLOBs/BLOBs)
                    for (int i = 0; i < callbacks.length; ++i)
                    {
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("052222",
                                op.getObjectAsPrintable(),
                                ((JavaTypeMapping)callbacks[i]).getMemberMetaData().getFullFieldName()));
                        }
                        callbacks[i].insertPostProcessing(op);
                    }

                    // Update the insert status for this table via the StoreManager
                    storeMgr.setObjectIsInsertedToLevel(op, table);

                    // Make sure all relation fields (1-1, N-1 with FK) we processed in the INSERT are attached.
                    // This is necessary because with a bidir relation and the other end attached we can just
                    // do the INSERT above first and THEN attach the other end here
                    // (if we did it the other way around we would get a NotYetFlushedException thrown above).
                    for (int i=0;i<relationFieldNumbers.length;i++)
                    {
                        Object value = op.provideField(relationFieldNumbers[i]);
                        if (value != null && ec.getApiAdapter().isDetached(value))
                        {
                            Object valueAttached = ec.persistObjectInternal(value, null, -1, ObjectProvider.PC);
                            op.replaceField(relationFieldNumbers[i], valueAttached);
                        }
                    }

                    // Perform reachability on all fields that have no datastore column (1-1 bi non-owner, N-1 bi join)
                    if (reachableFieldNumbers.length > 0)
                    {
                        int numberOfReachableFields = 0;
                        for (int i = 0; i < reachableFieldNumbers.length; i++)
                        {
                            if (reachableFieldNumbers[i] < op.getClassMetaData().getMemberCount())
                            {
                                numberOfReachableFields++;
                            }
                        }
                        int[] fieldNums = new int[numberOfReachableFields];
                        int j = 0;
                        for (int i = 0; i < reachableFieldNumbers.length; i++)
                        {
                            if (reachableFieldNumbers[i] < op.getClassMetaData().getMemberCount())
                            {
                                fieldNums[j++] = reachableFieldNumbers[i];
                            }
                        }
                        mappingDefinition = new StatementClassMapping();
                        idxs = retrievedStmtMappings;
                        for (int i=0;i<idxs.length;i++)
                        {
                            if (idxs[i] != null)
                            {
                                mappingDefinition.addMappingForMember(i, idxs[i]);
                            }
                        }
                        NucleusLogger.PERSISTENCE.debug("Performing reachability on fields " + StringUtils.intArrayToString(fieldNums));
                        op.provideFields(fieldNums, storeMgr.getFieldManagerForStatementGeneration(op, ps, mappingDefinition));
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
            String msg = Localiser.msg("052208", op.getObjectAsPrintable(), insertStmt, e.getMessage());
            NucleusLogger.DATASTORE_PERSIST.warn(msg);
            List exceptions = new ArrayList();
            exceptions.add(e);
            while ((e = e.getNextException()) != null)
            {
                exceptions.add(e);
            }
            throw new NucleusDataStoreException(msg, (Throwable[])exceptions.toArray(new Throwable[exceptions.size()]));
        }

        // Execute any mapping actions now that we have inserted the element
        // (things like inserting any association parent-child).
        for (int i = 0; i < callbacks.length; ++i)
        {
            try
            {
                if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                {
                    NucleusLogger.PERSISTENCE.debug(Localiser.msg("052209", op.getObjectAsPrintable(),
                        ((JavaTypeMapping)callbacks[i]).getMemberMetaData().getFullFieldName()));
                }
                callbacks[i].postInsert(op);
            }
            catch (NotYetFlushedException e)
            {
                op.updateFieldAfterInsert(e.getPersistable(), 
                    ((JavaTypeMapping) callbacks[i]).getMemberMetaData().getAbsoluteFieldNumber());
            }
        }
    }

    /**
     * Method to obtain the identity attributed by the datastore when using auto-increment/IDENTITY/SERIAL.
     * @param ec execution context
     * @param sqlControl SQLController
     * @param op ObjectProvider of the object
     * @param mconn The Connection
     * @param ps PreparedStatement for the INSERT
     * @return The identity
     * @throws SQLException Thrown if an error occurs retrieving the identity
     */
    private Object getInsertedDatastoreIdentity(ExecutionContext ec, SQLController sqlControl, ObjectProvider op, 
            ManagedConnection mconn, PreparedStatement ps)
    throws SQLException
    {
        Object datastoreId = null;

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
                    datastoreId = rs.getObject(1);
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

        if (datastoreId == null)
        {
            // Not found, so try the native method for retrieving it
            String columnName = null;
            JavaTypeMapping idMapping = table.getIdMapping();
            if (idMapping != null)
            {
                for (int i=0;i<idMapping.getNumberOfDatastoreMappings();i++)
                {
                    Column col = idMapping.getDatastoreMapping(i).getColumn();
                    if (col.isIdentity())
                    {
                        columnName = col.getIdentifier().toString();
                        break;
                    }
                }
            }
            String autoIncStmt = storeMgr.getDatastoreAdapter().getAutoIncrementStmt(table, columnName);
            PreparedStatement psAutoIncrement = sqlControl.getStatementForQuery(mconn, autoIncStmt);
            ResultSet rs = null;
            try
            {
                rs = sqlControl.executeStatementQuery(ec, mconn, autoIncStmt, psAutoIncrement);
                if (rs.next())
                {
                    datastoreId = rs.getObject(1);
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

        if (datastoreId == null)
        {
            throw new NucleusDataStoreException(Localiser.msg("052205", this.table));
        }

        return datastoreId;
    }

    /**
     * Internal class to provide mapping consumption for an INSERT.
     */
    private class InsertMappingConsumer implements MappingConsumer
    {
        /** Numbers of all fields to be inserted. */
        List insertFields = new ArrayList();

        /** Numbers of all PK fields. */
        List pkFields = new ArrayList();

        /** Numbers of all reachable fields (with no datastore column). */
        List reachableFields = new ArrayList();

        /** Numbers of all relations fields (bidir that may already be attached when persisting). */
        List relationFields = new ArrayList();

        StringBuilder columnNames = new StringBuilder();
        StringBuilder columnValues = new StringBuilder();

        Map assignedColumns = new HashMap();

        List mc = new ArrayList();

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

        /** StatementExpressionIndex for soft-delete. **/
        private StatementMappingIndex softDeleteStatementMapping;

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
                if (m.getNumberOfDatastoreMappings() == 0 &&
                    (m instanceof PersistableMapping || m instanceof ReferenceMapping))
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

                    // Check if the field is "insertable" (either using JPA column, or JDO extension)
                    if (mmd.hasExtension(MetaData.EXTENSION_MEMBER_INSERTABLE) && mmd.getValueForExtension(MetaData.EXTENSION_MEMBER_INSERTABLE).equalsIgnoreCase("false"))
                    {
                        return;
                    }

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
                    int parametersIndex[] = new int[m.getNumberOfDatastoreMappings()];
                    for (int j = 0; j < parametersIndex.length; j++)
                    {
                        // check if the column was not already assigned
                        Column c = m.getDatastoreMapping(j).getColumn();
                        DatastoreIdentifier columnId = c.getIdentifier();
                        boolean columnExists = assignedColumns.containsKey(columnId.toString());
                        if (columnExists)
                        {
                            parametersIndex[j] = ((Integer)assignedColumns.get(c.getIdentifier().toString())).intValue();
                        }

                        // Either we are a field in a secondary table.
                        // Or we are a subclass table.
                        // Or we are not datastore attributed.
                        if (table instanceof SecondaryTable || !table.isBaseDatastoreClass() ||
                            (!table.getStoreManager().isStrategyDatastoreAttributed(cmd, mmd.getAbsoluteFieldNumber()) && !c.isIdentity()))
                        {
                            if (!columnExists)
                            {
                                if (columnNames.length() > 0)
                                {
                                    columnNames.append(',');
                                    columnValues.append(',');
                                }
                                columnNames.append(columnId);
                                columnValues.append(((AbstractDatastoreMapping)m.getDatastoreMapping(j)).getInsertionInputParameter());
                            }

                            if (((AbstractDatastoreMapping)m.getDatastoreMapping(j)).insertValuesOnInsert())
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
                                    parametersIndex[j] = ((Integer)assignedColumns.get(c.getIdentifier().toString())).intValue();
                                }
                                else
                                {
                                    parametersIndex[j] = paramIndex++;
                                }
                            }

                            if (!columnExists)
                            {
                                assignedColumns.put(c.getIdentifier().toString(), Integer.valueOf(mmd.getAbsoluteFieldNumber()));
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
            if (m instanceof MappingCallbacks)
            {
                mc.add(m);
            }
        }

        /**
         * Consumes a mapping not associated to a field.
         * @param m The mapping.
         * @param mappingType the Mapping type
         */
        public void consumeMapping(JavaTypeMapping m, int mappingType)
        {
            if (mappingType == MappingConsumer.MAPPING_TYPE_VERSION)
            {
                // Surrogate version column
                JavaTypeMapping versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
                if (versionMapping != null)
                {
                    String val = ((AbstractDatastoreMapping)versionMapping.getDatastoreMapping(0)).getUpdateInputParameter();
                    if (columnNames.length() > 0)
                    {
                        columnNames.append(',');
                        columnValues.append(',');
                    }
                    columnNames.append(versionMapping.getDatastoreMapping(0).getColumn().getIdentifier());
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
            else if (mappingType == MappingConsumer.MAPPING_TYPE_DISCRIMINATOR)
            {
                // Surrogate discriminator column
                JavaTypeMapping discrimMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
                if (discrimMapping != null)
                {
                    String val = ((AbstractDatastoreMapping)discrimMapping.getDatastoreMapping(0)).getUpdateInputParameter();

                    if (columnNames.length() > 0)
                    {
                        columnNames.append(',');
                        columnValues.append(',');
                    }
                    columnNames.append(discrimMapping.getDatastoreMapping(0).getColumn().getIdentifier());
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
            else if (mappingType == MappingConsumer.MAPPING_TYPE_DATASTORE_ID)
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
            else if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_FK)
            {
                // External FK mapping (1-N uni)
                externalFKStmtExprIndex = processExternalMapping(m, statementMappings, externalFKStmtExprIndex);
            }
            else if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM)
            {
                // External FK discriminator mapping (1-N uni with shared FK)
                externalFKDiscrimStmtExprIndex = processExternalMapping(m, statementMappings, externalFKDiscrimStmtExprIndex);
            }
            else if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX)
            {
                // External FK order mapping (1-N uni List)
                externalOrderStmtExprIndex = processExternalMapping(m, statementMappings, externalOrderStmtExprIndex);
            }
            else if (mappingType == MappingConsumer.MAPPING_TYPE_MULTITENANCY)
            {
                // Multitenancy column
                JavaTypeMapping multitenancyMapping = table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false);
                String val = ((AbstractDatastoreMapping)multitenancyMapping.getDatastoreMapping(0)).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(multitenancyMapping.getDatastoreMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                multitenancyStatementMapping = new StatementMappingIndex(multitenancyMapping);
                int[] param = { paramIndex++ };
                multitenancyStatementMapping.addParameterOccurrence(param);
            }
            else if (mappingType == MappingConsumer.MAPPING_TYPE_SOFTDELETE)
            {
                // SoftDelete column
                JavaTypeMapping softDeleteMapping = table.getSurrogateMapping(SurrogateColumnType.SOFTDELETE, false);
                String val = ((AbstractDatastoreMapping)softDeleteMapping.getDatastoreMapping(0)).getUpdateInputParameter();

                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(softDeleteMapping.getDatastoreMapping(0).getColumn().getIdentifier());
                columnValues.append(val);
                softDeleteStatementMapping = new StatementMappingIndex(softDeleteMapping);
                int[] param = { paramIndex++ };
                softDeleteStatementMapping.addParameterOccurrence(param);
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
            int[] param = new int[mapping.getNumberOfDatastoreMappings()];
            for (int i=0;i<mapping.getNumberOfDatastoreMappings();i++)
            {
                if (columnNames.length() > 0)
                {
                    columnNames.append(',');
                    columnValues.append(',');
                }
                columnNames.append(mapping.getDatastoreMapping(i).getColumn().getIdentifier());
                columnValues.append(((AbstractDatastoreMapping)mapping.getDatastoreMapping(i)).getUpdateInputParameter());
                param[i] = paramIndex++;
            }
            stmtExprIndex[pos].addParameterOccurrence(param);
            
            return stmtExprIndex;
        }

        /**
         * @return Returns the mappingCallbacks.
         */
        public List getMappingCallbacks()
        {
            return mc;
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
                fieldNumbers[i] = ((Integer) insertFields.get(i)).intValue();
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
                fieldNumbers[i] = ((Integer) pkFields.get(i)).intValue();
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
                fieldNumbers[i] = ((Integer) reachableFields.get(i)).intValue();
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
                fieldNumbers[i] = ((Integer) relationFields.get(i)).intValue();
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