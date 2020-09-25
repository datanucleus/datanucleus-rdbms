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
2004 Andy Jefferson - conversion to use Logger
2005 Andy Jefferson - added handling for updating FK in related object
2006 Andy Jefferson - changed to extend VersionCheckRequest
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.request;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InterfaceMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.fieldmanager.ParameterSetter;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Class to provide a means of deletion of records from an RDBMS.
 * Extends basic request class implementing the execute method to do a JDBC delete operation.
 * Provides a version check for optimistic handling.
 * The SQL will be of the form
 * <pre>
 * DELETE FROM TBL_NAME WHERE id1=? AND id2=?
 * </pre>
 * or, if "soft delete" is being used for this type, then
 * <pre>
 * UPDATE TBL_NAME SET DELETE_COL=TRUE WHERE id1=? AND id2=?
 * </pre>
 */
public class DeleteRequest extends Request
{
    private final MappingCallbacks[] callbacks;

    /** Statement for deleting the object from the datastore. */
    private final String deleteStmt;

    /** Statement for deleting the object from the datastore (optimistic txns). */
    private final String deleteStmtOptimistic;

    /** Statement for soft-deleting the object from the datastore. */
    private final String softDeleteStmt;

    /** the index for the expression(s) in the delete statement. */
    private StatementMappingDefinition mappingStatementIndex;

    /** StatementExpressionIndex for multitenancy. **/
    private StatementMappingIndex multitenancyStatementMapping;

    /** PK fields to be provided in defining the record to be deleted (app identity cases). */
    private final int[] whereFieldNumbers;

    /** 1-1 bidir non-owner fields that are reachable (but not updated) and have no datastore column. */
    private final AbstractMemberMetaData[] oneToOneNonOwnerFields;

    /** MetaData for the class. */
    protected AbstractClassMetaData cmd = null;

    /** MetaData for the version handling. */
    protected VersionMetaData versionMetaData = null;

    /** Whether we should make checks on optimistic version before updating. */
    protected boolean versionChecks = false;

    /**
     * Constructor, taking the table. Uses the structure of the datastore table to build a basic query.
     * @param table The Class Table representing the datastore table to delete.
     * @param cmd ClassMetaData of objects being deleted
     * @param clr ClassLoader resolver
     */
    public DeleteRequest(DatastoreClass table, AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        super(table);

        this.cmd = cmd;
        versionMetaData = table.getVersionMetaData();
        if (versionMetaData != null && versionMetaData.getVersionStrategy() != VersionStrategy.NONE)
        {
            // Only apply a version check if we have a strategy defined
            versionChecks = true;
        }

        mappingStatementIndex = new StatementMappingDefinition(); // Populated using the subsequent lines
        DeleteMappingConsumer consumer = new DeleteMappingConsumer(clr, cmd);
        table.provideNonPrimaryKeyMappings(consumer); // to compute callbacks

        // WHERE clause - add identity
        consumer.setWhereClauseConsumption();
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            table.providePrimaryKeyMappings(consumer);
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            table.provideSurrogateMapping(SurrogateColumnType.DATASTORE_ID, consumer);
        }
        else
        {
            AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
            table.provideMappingsForMembers(consumer, mmds, false);
        }
        table.provideSurrogateMapping(SurrogateColumnType.MULTITENANCY, consumer);

        // Basic delete statement
        deleteStmt = consumer.getStatement();

        // Add on the optimistic discriminator (if appropriate) to get the delete statement for optimistic txns
        if (versionMetaData != null)
        {
            if (versionMetaData.getFieldName() != null)
            {
                // Version field
                AbstractMemberMetaData[] versionFmds = new AbstractMemberMetaData[1];
                versionFmds[0] = cmd.getMetaDataForMember(versionMetaData.getFieldName());
                table.provideMappingsForMembers(consumer, versionFmds, false);
            }
            else
            {
                // Surrogate version column
                table.provideSurrogateMapping(SurrogateColumnType.VERSION, consumer);
            }
        }

        // Optimistic delete statement
        deleteStmtOptimistic = consumer.getStatement();

        // Soft-delete statement for this delete
        softDeleteStmt = (table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE) != null) ? consumer.getSoftDeleteStatement() : null;

        whereFieldNumbers = consumer.getWhereFieldNumbers();
        callbacks = (MappingCallbacks[])consumer.getMappingCallBacks().toArray(new MappingCallbacks[consumer.getMappingCallBacks().size()]);
        oneToOneNonOwnerFields = consumer.getOneToOneNonOwnerFields();
    }

    /**
     * Method performing the deletion of the record from the datastore.
     * Takes the constructed deletion query and populates with the specific record information.
     * @param op The ObjectProvider for the record to be deleted.
     */
    public void execute(ObjectProvider op)
    {
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are deleting
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("052210", op.getObjectAsPrintable(), table));
        }

        // Process all related fields first
        // a). Delete any dependent objects
        // b). Null any non-dependent objects with FK at other side
        ClassLoaderResolver clr = op.getExecutionContext().getClassLoaderResolver();
        Set relatedObjectsToDelete = null;
        for (int i = 0; i < callbacks.length; ++i)
        {
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("052212", op.getObjectAsPrintable(), ((JavaTypeMapping)callbacks[i]).getMemberMetaData().getFullFieldName()));
            }
            callbacks[i].preDelete(op);

            // Check for any dependent related 1-1 objects where we hold the FK and where the object hasn't been deleted. 
            // This can happen if this DeleteRequest was triggered by delete-orphans and so the related object has to be deleted *after* this object.
            // It's likely we could do this better by using AttachFieldManager and just marking the "orphan" (i.e this object) as deleted 
            // (see AttachFieldManager TODO regarding when not copying)
            JavaTypeMapping mapping = (JavaTypeMapping) callbacks[i];
            AbstractMemberMetaData mmd = mapping.getMemberMetaData();
            RelationType relationType = mmd.getRelationType(clr);
            if (mmd.isDependent() && (relationType == RelationType.ONE_TO_ONE_UNI || (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() == null)))
            {
                try
                {
                    op.isLoaded(mmd.getAbsoluteFieldNumber());
                    Object relatedPc = op.provideField(mmd.getAbsoluteFieldNumber());
                    boolean relatedObjectDeleted = op.getExecutionContext().getApiAdapter().isDeleted(relatedPc);
                    if (!relatedObjectDeleted)
                    {
                        if (relatedObjectsToDelete == null)
                        {
                            relatedObjectsToDelete = new HashSet();
                        }
                        relatedObjectsToDelete.add(relatedPc);
                    }
                }
                catch (Exception e) // Should be XXXObjectNotFoundException but dont want to use JDO class
                {
                }
            }
        }

        // TODO Most of this is handled by PersistableMapping/ReferenceMapping.preDelete so should look to delete this
        // and cater for other cases, in particular persistent interfaces
        if (oneToOneNonOwnerFields != null && oneToOneNonOwnerFields.length > 0)
        {
            for (int i=0;i<oneToOneNonOwnerFields.length;i++)
            {
                AbstractMemberMetaData relatedFmd = oneToOneNonOwnerFields[i];
                updateOneToOneBidirectionalOwnerObjectForField(op, relatedFmd);
            }
        }

        // Choose the statement based on whether optimistic or not
        String stmt = null;
        ExecutionContext ec = op.getExecutionContext();
        RDBMSStoreManager storeMgr = table.getStoreManager();
        boolean optimisticChecks = false;
        if (table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE) != null)
        {
            stmt = softDeleteStmt;
        }
        else
        {
            optimisticChecks = (versionMetaData != null && ec.getTransaction().getOptimistic() && versionChecks);
            if (optimisticChecks)
            {
                stmt = deleteStmtOptimistic;
            }
            else
            {
                stmt = deleteStmt;
            }
        }

        // Process the delete of this object
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();

            try
            {
                // Perform the delete
                boolean batch = true;
                if (optimisticChecks || !ec.getTransaction().isActive())
                {
                    // Turn OFF batching if doing optimistic checks (since we need the result of the delete)
                    // or if using nontransactional writes (since we want it sending to the datastore now)
                    batch = false;
                }

                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, batch);
                try
                {
                    // provide WHERE clause field(s)
                    if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        StatementMappingIndex mapIdx = mappingStatementIndex.getWhereDatastoreId();
                        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
                        {
                            table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false).setObject(ec, ps, mapIdx.getParameterPositionsForOccurrence(i), op.getInternalObjectId());
                        }
                    }
                    else
                    {
                        StatementClassMapping mappingDefinition = new StatementClassMapping();
                        StatementMappingIndex[] idxs = mappingStatementIndex.getWhereFields();
                        for (int i=0;i<idxs.length;i++)
                        {
                            if (idxs[i] != null)
                            {
                                mappingDefinition.addMappingForMember(i, idxs[i]);
                            }
                        }
                        op.provideFields(whereFieldNumbers, new ParameterSetter(op, ps, mappingDefinition));
                    }

                    if (multitenancyStatementMapping != null)
                    {
                        table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false).setObject(ec, ps, 
                            multitenancyStatementMapping.getParameterPositionsForOccurrence(0), ec.getNucleusContext().getMultiTenancyId(ec));
                    }

                    if (optimisticChecks)
                    {
                        // WHERE clause - current version discriminator
                        JavaTypeMapping verMapping = mappingStatementIndex.getWhereVersion().getMapping();
                        Object currentVersion = op.getTransactionalVersion();
                        if (currentVersion == null)
                        {
                            // Somehow the version is not set on this object (not read in ?) so report the bug
                            String msg = Localiser.msg("052202", op.getInternalObjectId(), table);
                            NucleusLogger.PERSISTENCE.error(msg);
                            throw new NucleusException(msg);
                        }
                        StatementMappingIndex mapIdx = mappingStatementIndex.getWhereVersion();
                        for (int i=0;i<mapIdx.getNumberOfParameterOccurrences();i++)
                        {
                            verMapping.setObject(ec, ps, mapIdx.getParameterPositionsForOccurrence(i), currentVersion);
                        }
                    }

                    int[] rcs = sqlControl.executeStatementUpdate(ec, mconn, stmt, ps, !batch);
                    if (optimisticChecks && rcs[0] == 0)
                    {
                        // No object deleted so either object disappeared or failed optimistic version checks
                        throw new NucleusOptimisticException(Localiser.msg("052203", op.getObjectAsPrintable(), op.getInternalObjectId(), "" + op.getTransactionalVersion()), op.getObject());
                    }

                    if (relatedObjectsToDelete != null && !relatedObjectsToDelete.isEmpty())
                    {
                        // Delete any related objects that need deleting after the delete of this object
                        Iterator iter = relatedObjectsToDelete.iterator();
                        while (iter.hasNext())
                        {
                            Object relatedObject = iter.next();
                            ec.deleteObjectInternal(relatedObject);
                        }
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
            String msg = Localiser.msg("052211", op.getObjectAsPrintable(), stmt, e.getMessage());
            NucleusLogger.DATASTORE_PERSIST.warn(msg);
            List exceptions = new ArrayList();
            exceptions.add(e);
            while((e = e.getNextException())!=null)
            {
                exceptions.add(e);
            }
            throw new NucleusDataStoreException(msg, (Throwable[])exceptions.toArray(new Throwable[exceptions.size()]));
        }
    }

    /**
     * Method to update any 1-1 bidir non-owner fields where the foreign-key is stored in the other object.
     * @param op ObjectProvider of this object
     * @param mmd MetaData for field that has related (owner) objects
     */
    private void updateOneToOneBidirectionalOwnerObjectForField(ObjectProvider op, AbstractMemberMetaData mmd)
    {
        if (softDeleteStmt != null)
        {
            // If we are soft deleting the owner then no need to null the linkage BUT we need to check the soft delete status on retrieval of the owner from related
            return;
        }

        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("052217", op.getObjectAsPrintable(), mmd.getFullFieldName()));
        }

        RDBMSStoreManager storeMgr = table.getStoreManager();
        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);

        // Check if we should null here, or leave to the datastore FK handler
        boolean checkFK = true;
        if (ec.getStringProperty(PropertyNames.PROPERTY_DELETION_POLICY).equals("JDO2"))
        {
            // JDO2 doesn't currently (2.0 spec) take note of foreign-key
            checkFK = false;
        }
        if (checkFK)
        {
            for (int i=0;i<relatedMmds.length;i++)
            {
                ForeignKeyMetaData relFkmd = relatedMmds[i].getForeignKeyMetaData();
                if (relFkmd != null && relFkmd.getDeleteAction() != null)
                {
                    // Field has a FK with a delete-action so leave to the datastore to process the delete
                    return;
                }
            }
        }

        // TODO Cater for more than 1 related field
        String fullClassName = ((AbstractClassMetaData)relatedMmds[0].getParent()).getFullClassName();

        //TODO I'm not sure that we need to loop all implementations. will we have the fk set to all tables, if many?
        String[] classes;
        if (((AbstractClassMetaData)relatedMmds[0].getParent()) instanceof InterfaceMetaData)
        {
            classes = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(fullClassName, clr);
        }
        else
        {
            classes = new String[] {fullClassName};
        }
        Set<DatastoreClass> datastoreClasses = new HashSet();
        for (int i=0; i<classes.length; i++)
        {
            // just remove duplicates
            datastoreClasses.add(storeMgr.getDatastoreClass(classes[i], clr));
        }

        Iterator<DatastoreClass> it = datastoreClasses.iterator();
        while (it.hasNext())
        {
            DatastoreClass refTable = it.next();
            JavaTypeMapping refMapping = refTable.getMemberMapping(mmd.getMappedBy());
            if (refMapping.isNullable()) // Only clear the references that can be cleared
            {
                // Create a statement to clear the link from the previous related object
                StringBuilder clearLinkStmt = new StringBuilder("UPDATE " + refTable.toString() + " SET ");
                for (int j=0;j<refMapping.getNumberOfColumnMappings();j++)
                {
                    if (j > 0)
                    {
                        clearLinkStmt.append(",");
                    }
                    clearLinkStmt.append(refMapping.getColumnMapping(j).getColumn().getIdentifier());
                    clearLinkStmt.append("=NULL");
                }
                clearLinkStmt.append(" WHERE ");
                for (int j=0;j<refMapping.getNumberOfColumnMappings();j++)
                {
                    if (j > 0)
                    {
                        clearLinkStmt.append(" AND ");
                    }
                    clearLinkStmt.append(refMapping.getColumnMapping(j).getColumn().getIdentifier());
                    clearLinkStmt.append("=?");
                }

                try
                {
                    ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
                    SQLController sqlControl = storeMgr.getSQLController();
                    try
                    {
                        // Null out the relationship to the object being deleted.
                        PreparedStatement ps = null;
                        try
                        {
                            ps = sqlControl.getStatementForUpdate(mconn, clearLinkStmt.toString(), false);
                            refMapping.setObject(ec, ps, MappingHelper.getMappingIndices(1, refMapping), op.getObject());

                            sqlControl.executeStatementUpdate(ec, mconn, clearLinkStmt.toString(), ps, true);
                        }
                        finally
                        {
                            if (ps != null)
                            {
                                sqlControl.closeStatement(mconn, ps);
                            }
                        }
                    }
                    finally
                    {
                        mconn.release();
                    }
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException("Update request failed", e);
                }
            }
        }
    }

    /**
     * Mapping Consumer used for generating the DELETE statement for an object in a table.
     * This statement will be of the form
     * <PRE>
     * DELETE FROM table-name WHERE id1=? AND id2=?
     * </PRE>
     * or (when also performing version checks)
     * <PRE>
     * DELETE FROM table-name WHERE id1=? AND id2=? AND version={oldvers}
     * </PRE>
     */
    private class DeleteMappingConsumer implements MappingConsumer
    {
        /** Flag for initialisation state of the consumer. */
        boolean initialized = false;

        /** Where clause for the statement. Built during the consumption process. */
        StringBuilder where = new StringBuilder();

        /** Current parameter index. */
        int paramIndex = 1;

        /** WHERE clause field numbers to use in identifying the record to delete. */
        private List whereFields = new ArrayList();

        /** Fields in a 1-1 relation with FK in the table of the other object. */
        private List oneToOneNonOwnerFields = new ArrayList();

        /** Mapping Callbacks to invoke at deletion. */
        private List mc = new ArrayList();

        /** ClassLoaderResolver **/
        private final ClassLoaderResolver clr;

        /** MetaData for the class of the object */
        private final AbstractClassMetaData cmd;

        private boolean whereClauseConsumption = false;

        /**
         * Constructor.
         * @param clr the ClassLoaderResolver
         * @param cmd AbstractClassMetaData
         */
        public DeleteMappingConsumer(ClassLoaderResolver clr, AbstractClassMetaData cmd)
        {
            this.clr = clr;
            this.cmd = cmd;
            this.paramIndex = 1;
        }

        public void setWhereClauseConsumption()
        {
            // Starts with this as false so we can calculate the callbacks, then set to true thereafter
            this.whereClauseConsumption = true;
        }

        public void preConsumeMapping(int highest)
        {
            if (!initialized)
            {
                mappingStatementIndex.setWhereFields(new StatementMappingIndex[highest]);
                mappingStatementIndex.setUpdateFields(new StatementMappingIndex[highest]);
                initialized = true;
            }
        }

        public void consumeMapping(JavaTypeMapping m, AbstractMemberMetaData mmd)
        {
            if (!mmd.getAbstractClassMetaData().isSameOrAncestorOf(cmd))
            {
                return;
            }
            if (m.includeInUpdateStatement())
            {
                if (whereClauseConsumption)
                {
                    // Where fields
                    VersionMetaData vermd = cmd.getVersionMetaDataForTable();
                    if (!table.managesClass(cmd.getFullClassName()))
                    {
                        // The candidate being updated isn't in this table, so go to base for metadata for this mapping
                        vermd = cmd.getBaseAbstractClassMetaData().getVersionMetaDataForClass();
                    }
                    if (vermd != null && vermd.getFieldName() != null && mmd.getName().equals(vermd.getFieldName()))
                    {
                        // Version field
                        StatementMappingIndex sei = new StatementMappingIndex(m);
                        mappingStatementIndex.setWhereVersion(sei);
                        int parametersIndex[] = {paramIndex++};
                        sei.addParameterOccurrence(parametersIndex);

                        if (where.length() > 0)
                        {
                            where.append(" AND ");
                        }
                        where.append(m.getColumnMapping(0).getColumn().getIdentifier());
                        where.append("=");
                        where.append(m.getColumnMapping(0).getUpdateInputParameter());
                    }
                    else
                    {
                        Integer abs_field_num = Integer.valueOf(mmd.getAbsoluteFieldNumber());
                        int parametersIndex[] = new int[m.getNumberOfColumnMappings()];
                        StatementMappingIndex sei = new StatementMappingIndex(m);
                        sei.addParameterOccurrence(parametersIndex);

                        mappingStatementIndex.getWhereFields()[mmd.getAbsoluteFieldNumber()] = sei; 
                        for (int j=0; j<parametersIndex.length; j++)
                        {
                            if (where.length() > 0)
                            {
                                where.append(" AND ");
                            }
                            where.append(m.getColumnMapping(j).getColumn().getIdentifier());
                            where.append("="); 
                            where.append(m.getColumnMapping(j).getUpdateInputParameter());

                            if (!whereFields.contains(abs_field_num))
                            {
                                whereFields.add(abs_field_num);
                            }
                            parametersIndex[j] = paramIndex++;
                        }
                    }
                }

                if (m instanceof PersistableMapping || m instanceof ReferenceMapping)
                {
                    if (m.getNumberOfColumnMappings() == 0)
                    {
                        // Field storing a PC object with FK at other side
                        RelationType relationType = mmd.getRelationType(clr);
                        if (relationType == RelationType.ONE_TO_ONE_BI)
                        {
                            if (mmd.getMappedBy() != null)
                            {
                                // 1-1 bidirectional field without datastore column(s) (with single FK at other side)
                                oneToOneNonOwnerFields.add(mmd);
                            }
                        }
                        else if (relationType == RelationType.MANY_TO_ONE_BI)
                        {
                            AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                            if (mmd.getJoinMetaData() != null || relatedMmds[0].getJoinMetaData() != null)
                            {
                                // 1-N bidirectional using join table for relation
                                // TODO Anything to do here ?
                            }
                        }
                    }
                }
            }

            // Build up list of mappings callbacks for the fields of this class.
            // The Mapping callback called delete is the preDelete
            if (m instanceof MappingCallbacks)
            {
                mc.add(m);
            }
        }

        /**
         * Consumes a mapping for a surrogate column (version, datastore identity, multitenancy).
         * Never called with whereClauseConsumption=false
         * @param m The mapping
         * @param mappingType the Mapping type
         */
        public void consumeMapping(JavaTypeMapping m, MappingType mappingType)
        {
            if (mappingType == MappingType.DATASTORE_ID)
            {
                if (where.length() > 0)
                {
                    where.append(" AND ");
                }
                where.append(m.getColumnMapping(0).getColumn().getIdentifier().toString());
                where.append("=");
                where.append(m.getColumnMapping(0).getUpdateInputParameter());

                StatementMappingIndex datastoreMappingIdx = new StatementMappingIndex(m);
                mappingStatementIndex.setWhereDatastoreId(datastoreMappingIdx);
                int[] param = { paramIndex++ };
                datastoreMappingIdx.addParameterOccurrence(param);
            }
            else if (mappingType == MappingType.VERSION)
            {
                if (where.length() > 0)
                {
                    where.append(" AND ");
                }
                where.append(m.getColumnMapping(0).getColumn().getIdentifier());
                where.append("=");
                where.append(m.getColumnMapping(0).getUpdateInputParameter());

                StatementMappingIndex versStmtIdx = new StatementMappingIndex(m);
                mappingStatementIndex.setWhereVersion(versStmtIdx);
                int[] param = { paramIndex++ };
                versStmtIdx.addParameterOccurrence(param);
            }
            else if (mappingType == MappingType.MULTITENANCY)
            {
                // Multitenancy column
                JavaTypeMapping tenantMapping = table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false);
                if (where.length() > 0)
                {
                    where.append(" AND ");
                }
                where.append(tenantMapping.getColumnMapping(0).getColumn().getIdentifier().toString());
                where.append("=");
                where.append(tenantMapping.getColumnMapping(0).getUpdateInputParameter());

                multitenancyStatementMapping = new StatementMappingIndex(tenantMapping);
                int[] param = { paramIndex++ };
                multitenancyStatementMapping.addParameterOccurrence(param);
            }
        }

        /**
         * Consumer a column without mapping.
         * @param col Column
         */
        public void consumeUnmappedColumn(Column col)
        {
            // Do nothing since we dont handle unmapped columns
        }

        /**
         * Accessor for the field numbers of any WHERE clause fields
         * @return array of absolute WHERE clause field numbers
         */
        public int[] getWhereFieldNumbers()
        {
            int[] fieldNumbers = new int[whereFields.size()];
            for (int i = 0; i < whereFields.size(); i++)
            {
                fieldNumbers[i] = ((Integer)whereFields.get(i)).intValue();
            }
            return fieldNumbers;
        }

        /**
         * All 1-1 bidirectional non-owner fields, with the FK In the other object.
         * @return The fields that are 1-1 bidirectional with the FK at the other side.
         */
        public AbstractMemberMetaData[] getOneToOneNonOwnerFields()
        {
            AbstractMemberMetaData[] fmds = new AbstractMemberMetaData[oneToOneNonOwnerFields.size()];
            for (int i = 0; i < oneToOneNonOwnerFields.size(); ++i)
            {
                fmds[i] = (AbstractMemberMetaData) oneToOneNonOwnerFields.get(i);
            }
            return fmds;
        }

        /**
         * Obtain a List of mapping callbacks that will be run for this deletion.
         * @return the mapping callbacks
         */
        public List getMappingCallBacks()
        {
            return mc;
        }

        /**
         * Accessor for the delete SQL statement.
         * @return The delete SQL statement
         */
        public String getStatement()
        {
            return "DELETE FROM " + table.toString() + " WHERE " + where;
        }

        public String getSoftDeleteStatement()
        {
            org.datanucleus.store.schema.table.Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
            return "UPDATE " + table.toString() + " SET " + softDeleteCol.getName() + "=TRUE WHERE " + where; // TODO Cater for columns that store the DELETED flag as ("Y","N") or (1,0)
        }
    }
}