/**********************************************************************
Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NotYetFlushedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.fieldmanager.DynamicSchemaFieldManager;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SelectStatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.CollectionTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.scostore.SetStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Implementation of a {@link SetStore} using join table.
 */
public class JoinSetStore<E> extends AbstractSetStore<E>
{
    /** Statement to check the existence of an owner-element relation. */
    protected String locateStmt;

    /** Statement to get the maximum order column id so we can set the next insert value. */
    protected String maxOrderColumnIdStmt;

    /**
     * Constructor for a join set store for RDBMS.
     * @param mmd Metadata for the member that has the set with join table
     * @param joinTable The join table
     * @param clr The ClassLoaderResolver
     */
    public JoinSetStore(AbstractMemberMetaData mmd, CollectionTable joinTable, ClassLoaderResolver clr)
    {
        super(joinTable.getStoreManager(), clr);
        this.containerTable = joinTable;
        setOwner(mmd);

        this.ownerMapping = joinTable.getOwnerMapping();
        this.elementMapping = joinTable.getElementMapping();
        this.orderMapping = joinTable.getOrderMapping();
        this.relationDiscriminatorMapping = joinTable.getRelationDiscriminatorMapping();
        this.relationDiscriminatorValue = joinTable.getRelationDiscriminatorValue();
        this.elementType = mmd.getCollection().getElementType();
        this.elementsAreEmbedded = joinTable.isEmbeddedElement();
        this.elementsAreSerialised = joinTable.isSerialisedElement();

        if (elementsAreSerialised)
        {
            elementInfo = null;
        }
        else
        {
            Class element_class = clr.classForName(elementType);
            if (ClassUtils.isReferenceType(element_class))
            {
                // Collection of reference types (interfaces/Objects)
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(
                    ownerMemberMetaData, FieldRole.ROLE_COLLECTION_ELEMENT, clr, storeMgr.getMetaDataManager());
                elementInfo = new ComponentInfo[implNames.length];
                for (int i = 0; i < implNames.length; i++)
                {
                    DatastoreClass table = storeMgr.getDatastoreClass(implNames[i], clr);
                    AbstractClassMetaData cmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(implNames[i], clr);
                    elementInfo[i] = new ComponentInfo(cmd, table);
                }
            }
            else
            {
                // Set<PC>, Set<Non-PC>
                // Generate the information for the possible elements
                elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(element_class, clr);
                if (elementCmd != null && !elementsAreEmbedded)
                {
                    elementInfo = getComponentInformationForClass(elementType, elementCmd);
                }
                else
                {
                    elementInfo = null;
                }
            }
        }
    }

    @Override
    public void update(DNStateManager sm, Collection<? extends E> coll)
    {
        if (coll == null || coll.isEmpty())
        {
            clear(sm);
            return;
        }

        if (ownerMemberMetaData.getCollection().isSerializedElement() || ownerMemberMetaData.getCollection().isEmbeddedElement())
        {
            // Serialized/Embedded elements so just clear and add again
            clear(sm);
            addAll(sm, coll, 0);
            return;
        }

        // Find existing elements, and remove any that are no longer present
        Iterator elemIter = iterator(sm);
        Collection existing = new HashSet();
        while (elemIter.hasNext())
        {
            Object elem = elemIter.next();
            if (!coll.contains(elem))
            {
                remove(sm, elem, -1, true);
            }
            else
            {
                existing.add(elem);
            }
        }

        if (existing.size() != coll.size())
        {
            // Add any elements that aren't already present
            Iterator<? extends E> iter = coll.iterator();
            while (iter.hasNext())
            {
                E elem = iter.next();
                if (!existing.contains(elem))
                {
                    add(sm, elem, 0);
                }
            }
        }
    }

    /**
     * Convenience method to check if an element already refers to the owner in an M-N relation (i.e added from other side).
     * @param ownerSM StateManager of the owner
     * @param element The element
     * @return Whether the element contains the owner
     */
    protected boolean elementAlreadyContainsOwnerInMtoN(DNStateManager ownerSM, Object element)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();

        if (ec.getOperationQueue() != null)
        {
            // Optimistic add, so additions are queued. No point checking the SCO on the other side, so just query the datastore whether already added (from other side)
            // TODO This means we always do a "SELECT 1 FROM JOINTABLE" for every addition when optimistic. Should seek to avoid this by updates to operationQueue maybe
            if (locate(ownerSM, element))
            {
                NucleusLogger.DATASTORE.info(Localiser.msg("056040", ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(ownerSM.getObject()), element));
                return true;
            }
            return false;
        }

        DNStateManager elementSM = ec.findStateManager(element);
        if (elementSM != null)
        {
            // Check the collection at the other side whether already added (to avoid the locate call)
            AbstractMemberMetaData[] relatedMmds = ownerMemberMetaData.getRelatedMemberMetaData(ec.getClassLoaderResolver());
            Object elementColl = elementSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
            if (elementColl != null && elementColl instanceof Collection && elementColl instanceof SCO && ((Collection)elementColl).contains(ownerSM.getObject()))
            {
                NucleusLogger.DATASTORE.info(Localiser.msg("056040", ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(ownerSM.getObject()), element));
                return true;
            }
        }
        else
        {
            // Element is still detached
            if (locate(ownerSM, element))
            {
                NucleusLogger.DATASTORE.info(Localiser.msg("056040", ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(ownerSM.getObject()), element));
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean add(DNStateManager sm, E element, int size)
    {
        // Check that the object is valid for writing
        ExecutionContext ec = sm.getExecutionContext();
        validateElementForWriting(ec, element, null);

        if (relationType == RelationType.ONE_TO_MANY_BI)
        {
            // TODO This is ManagedRelations - move into RelationshipManager
            // Managed Relations : make sure we have consistency of relation
            DNStateManager elementSM = ec.findStateManager(element);
            if (elementSM != null)
            {
                AbstractMemberMetaData[] relatedMmds = ownerMemberMetaData.getRelatedMemberMetaData(clr);
                Object elementOwner = elementSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                if (elementOwner == null)
                {
                    // No owner, so correct it
                    NucleusLogger.PERSISTENCE.info(Localiser.msg("056037", sm.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                        StringUtils.toJVMIDString(elementSM.getObject())));
                    elementSM.replaceField(relatedMmds[0].getAbsoluteFieldNumber(), sm.getObject());
                }
                else if (elementOwner != sm.getObject() && sm.getReferencedPC() == null)
                {
                    // Owner of the element is neither this container nor being attached
                    // Inconsistent owner, so throw exception
                    throw new NucleusUserException(Localiser.msg("056038", sm.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                        StringUtils.toJVMIDString(elementSM.getObject()), StringUtils.toJVMIDString(elementOwner)));
                }
            }
        }

        boolean modified = false;

        boolean toBeInserted = true;
        if (relationType == RelationType.MANY_TO_MANY_BI)
        {
            toBeInserted = !elementAlreadyContainsOwnerInMtoN(sm, element);
        }

        if (toBeInserted)
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                // Add a row to the join table
                int orderID = orderMapping != null ? getNextIDForOrderColumn(sm) : -1;
                int[] returnCode = doInternalAdd(sm, element, mconn, false, orderID, true);
                if (returnCode[0] > 0)
                {
                    modified = true;
                }
            }
            finally
            {
                mconn.release();
            }
        }

        return modified;
    }

    @Override
    public boolean addAll(DNStateManager sm, Collection<? extends E> elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        List exceptions = new ArrayList();
        boolean batched = (elements.size() > 1);

        // Validate all elements for writing
        ExecutionContext ec = sm.getExecutionContext();
        Iterator<? extends E> iter = elements.iterator();
        while (iter.hasNext())
        {
            Object element = iter.next();
            validateElementForWriting(ec, element, null);

            if (relationType == RelationType.ONE_TO_MANY_BI)
            {
                // TODO This is ManagedRelations - move into RelationshipManager
                // Managed Relations : make sure we have consistency of relation
                DNStateManager elementSM = sm.getExecutionContext().findStateManager(element);
                if (elementSM != null)
                {
                    AbstractMemberMetaData[] relatedMmds = ownerMemberMetaData.getRelatedMemberMetaData(clr);
                    Object elementOwner = elementSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                    if (elementOwner == null)
                    {
                        // No owner, so correct it
                        NucleusLogger.PERSISTENCE.info(Localiser.msg("056037", sm.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                            StringUtils.toJVMIDString(elementSM.getObject())));
                        elementSM.replaceField(relatedMmds[0].getAbsoluteFieldNumber(), sm.getObject());
                    }
                    else if (elementOwner != sm.getObject() && sm.getReferencedPC() == null)
                    {
                        // Owner of the element is neither this container nor its referenced object
                        // Inconsistent owner, so throw exception
                        throw new NucleusUserException(Localiser.msg("056038", sm.getObjectAsPrintable(),
                            ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(elementSM.getObject()), StringUtils.toJVMIDString(elementOwner)));
                    }
                }
            }
        }

        boolean modified = false;
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Process all waiting batched statements before we start our work
                sqlControl.processStatementsForConnection(mconn);
            }
            catch (SQLException e)
            {
                throw new NucleusDataStoreException("SQLException", e);
            }

            int nextOrderID = orderMapping != null ? getNextIDForOrderColumn(sm) : 0;

            // Loop through all elements to be added
            iter = elements.iterator();
            E element = null;
            while (iter.hasNext())
            {
                element = iter.next();

                // Add the row to the join table
                boolean toBeInserted = true;
                if (relationType == RelationType.MANY_TO_MANY_BI)
                {
                    toBeInserted = !elementAlreadyContainsOwnerInMtoN(sm, element);
                }

                if (toBeInserted)
                {
                    int[] rc = doInternalAdd(sm, element, mconn, batched, nextOrderID, !batched || (batched && !iter.hasNext()));
                    if (rc != null)
                    {
                        for (int i = 0; i < rc.length; i++)
                        {
                            if (rc[i] > 0)
                            {
                                // At least one record was inserted
                                modified = true;
                            }
                        }
                    }
                    nextOrderID++;
                }
            }
        }
        finally
        {
            mconn.release();
        }

        if (!exceptions.isEmpty())
        {
            // Throw all exceptions received as the cause of a NucleusDataStoreException so the user can see which record(s) didn't persist
            String msg = Localiser.msg("056009", ((Exception) exceptions.get(0)).getMessage());
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusDataStoreException(msg, (Throwable[]) exceptions.toArray(new Throwable[exceptions.size()]), sm.getObject());
        }

        return modified;
    }

    @Override
    public boolean removeAll(DNStateManager sm, Collection elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        // Remove specified elements from join table using single SQL
        boolean modified = false;
        String removeAllStmt = getRemoveAllStmt(sm, elements);
        try
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, removeAllStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    Iterator iter = elements.iterator();
                    while (iter.hasNext())
                    {
                        Object element = iter.next();
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                        jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, jdbcPosition, elementMapping);
                        if (relationDiscriminatorMapping != null)
                        {
                            jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                        }
                    }

                    int[] number = sqlControl.executeStatementUpdate(ec, mconn, removeAllStmt, ps, true);
                    if (number[0] > 0)
                    {
                        modified = true;
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
            NucleusLogger.DATASTORE.error("Exception on removeAll", e);
            throw new NucleusDataStoreException(Localiser.msg("056012", removeAllStmt), e);
        }

        // Delete elements if cascade required
        if (ownerMemberMetaData.getCollection().isDependentElement() || ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            // "delete-dependent" : delete elements if the collection is marked as dependent
            // TODO What if the collection contains elements that are not in the Set ? should not delete them
            sm.getExecutionContext().deleteObjects(elements.toArray());
        }

        return modified;
    }

    /**
     * Generate statement for deleting items from the Set.
     * The EMBEDDEDFIELDX is only present when the elements are PC(embedded).
     * <PRE>
     * DELETE FROM SETTABLE
     * WHERE OWNERCOL=?
     * AND ELEMENTCOL = ?
     * [AND EMBEDDEDFIELD1 = ? AND EMBEDDEDFIELD2 = ? AND EMBEDDEDFIELD3 = ?]
     * [AND RELATION_DISCRIM = ?]
     * </PRE>
     * @return Statement for deleting items from the Set.
     */
    protected String getRemoveStmt(Object element)
    {
        StringBuilder stmt = new StringBuilder("DELETE FROM ").append(containerTable.toString()).append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, elementsAreSerialised, null, false);
        if (relationDiscriminatorMapping != null)
        {
            BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
        }

        return stmt.toString();
    }

    /**
     * Generate statement for removing a collection of items from the Set.
     * <PRE>
     * DELETE FROM SETTABLE
     * WHERE (OWNERCOL=? AND ELEMENTCOL=?) OR
     *       (OWNERCOL=? AND ELEMENTCOL=?) OR
     *       (OWNERCOL=? AND ELEMENTCOL=?)
     * </PRE>
     * @param sm StateManager for the owner
     * @param elements Collection of elements to remove
     * @return Statement for deleting items from the Set.
     */
    protected String getRemoveAllStmt(DNStateManager sm, Collection elements)
    {
        if (elements == null || elements.size() == 0)
        {
            return null;
        }

        StringBuilder stmt = new StringBuilder("DELETE FROM ").append(containerTable.toString()).append(" WHERE ");

        boolean first = true;
        for (Object element : elements)
        {
            stmt.append(first ? "(" : " OR (");
            BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
            BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, elementsAreSerialised, null, false);
            if (relationDiscriminatorMapping != null)
            {
                BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
            }

            stmt.append(")");
            first = false;
        }

        return stmt.toString();
    }

    protected boolean locate(DNStateManager sm, Object element)
    {
        boolean exists = true;
        String stmt = getLocateStmt(element);
        try
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                    jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, jdbcPosition, elementMapping);
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        if (!rs.next())
                        {
                            exists = false;
                        }
                    }
                    catch (SQLException sqle)
                    {
                        rs.close();
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
            NucleusLogger.DATASTORE.error(Localiser.msg("RDBMS.SCO.LocateRequestFailed", stmt), e);
            throw new NucleusDataStoreException(Localiser.msg("RDBMS.SCO.LocateRequestFailed", stmt), e);
        }
        return exists;
    }

    protected int[] doInternalAdd(DNStateManager sm, E element, ManagedConnection conn, boolean batched, int orderId, boolean executeNow)
    {
        // Check for dynamic schema updates prior to addition
        if (storeMgr.getBooleanObjectProperty(RDBMSPropertyNames.PROPERTY_RDBMS_DYNAMIC_SCHEMA_UPDATES).booleanValue())
        {
            DynamicSchemaFieldManager dynamicSchemaFM = new DynamicSchemaFieldManager(storeMgr, sm);
            Collection coll = new HashSet();
            coll.add(element);
            dynamicSchemaFM.storeObjectField(ownerMemberMetaData.getAbsoluteFieldNumber(), coll);
            if (dynamicSchemaFM.hasPerformedSchemaUpdates())
            {
                invalidateAddStmt();
            }
        }

        String addStmt = getAddStmtForJoinTable();
        boolean notYetFlushedError = false;
        ExecutionContext ec = sm.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, addStmt, batched);
            try
            {
                // Insert the join table row
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, element, jdbcPosition, elementMapping);
                if (orderMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, orderId, jdbcPosition, orderMapping);
                }
                if (relationDiscriminatorMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                }

                // TODO If the element has requiresSetPostProcessing then call elementMapping.performSetPostProcessing
                return sqlControl.executeStatementUpdate(ec, conn, addStmt, ps, executeNow);
            }
            catch (NotYetFlushedException nfe)
            {
                notYetFlushedError = true;
                throw nfe;
            }
            finally
            {
                if (notYetFlushedError)
                {
                    sqlControl.abortStatementForConnection(conn, ps);
                }
                else
                {
                    sqlControl.closeStatement(conn, ps);
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056009", addStmt), e);
        }
    }

    private synchronized String getLocateStmt(Object element)
    {
        if (elementMapping instanceof ReferenceMapping && elementMapping.getNumberOfColumnMappings() > 1)
        {
            // The statement is based on the element passed in so don't cache
            return getLocateStatementString(element);
        }

        if (locateStmt == null)
        {
            synchronized (this)
            {
                locateStmt = getLocateStatementString(element);
            }
        }
        return locateStmt;
    }

    /**
     * Generate statement for checking the existence of an owner-element relation (used for M-N).
     * <PRE>
     * SELECT 1 FROM JOINTABLE WHERE OWNERCOL = ? AND ELEMENTCOL = ?
     * </PRE>
     * @param element The element to locate
     * @return Statement for locating an owner-element relation in the join table
     */
    protected String getLocateStatementString(Object element)
    {
        StringBuilder stmt = new StringBuilder("SELECT 1 FROM ").append(containerTable.toString()).append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, elementsAreSerialised, null, false);
        if (relationDiscriminatorMapping != null)
        {
            BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
        }

        return stmt.toString();
    }

    protected int getNextIDForOrderColumn(DNStateManager sm)
    {
        int nextID;
        ExecutionContext ec = sm.getExecutionContext();
        String stmt = getMaxOrderColumnIdStmt();
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();

            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                    if (relationDiscriminatorMapping != null)
                    {
                        BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        nextID = rs.next() ? rs.getInt(1) + 1 : 1;
                        JDBCUtils.logWarnings(rs);
                    }
                    finally
                    {
                        rs.close();
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
            throw new NucleusDataStoreException(Localiser.msg("056020", stmt), e);
        }

        return nextID;
    }

    /**
     * Generate statement for obtaining the maximum id for the order column.
     * <PRE>
     * SELECT MAX(SCOID) FROM SETTABLE
     * WHERE OWNERCOL=?
     * [AND RELATION_DISCRIM=?]
     * </PRE>
     * @return The Statement returning the higher id
     */
    private synchronized String getMaxOrderColumnIdStmt()
    {
        if (maxOrderColumnIdStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("SELECT MAX(" + orderMapping.getColumnMapping(0).getColumn().getIdentifier().toString() + ")");
                stmt.append(" FROM ").append(containerTable.toString()).append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }

                maxOrderColumnIdStmt = stmt.toString();
            }
        }

        return maxOrderColumnIdStmt;
    }

    @Override
    public Iterator<E> iterator(DNStateManager ownerSM)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();

        // Generate the statement, and statement mapping/parameter information
        ElementIteratorStatement iterStmt = getIteratorStatement(ec, ec.getFetchPlan(), true);
        SelectStatement sqlStmt = iterStmt.sqlStmt;
        StatementClassMapping iteratorMappingClass = iterStmt.elementClassMapping;

        // Input parameter(s) - the owner
        int inputParamNum = 1;
        StatementMappingIndex ownerStmtMapIdx = new StatementMappingIndex(ownerMapping);
        if (sqlStmt.getNumberOfUnions() > 0)
        {
            // Add parameter occurrence for each union of statement
            for (int j=0;j<sqlStmt.getNumberOfUnions()+1;j++)
            {
                int[] paramPositions = new int[ownerMapping.getNumberOfColumnMappings()];
                for (int k=0;k<paramPositions.length;k++)
                {
                    paramPositions[k] = inputParamNum++;
                }
                ownerStmtMapIdx.addParameterOccurrence(paramPositions);
            }
        }
        else
        {
            int[] paramPositions = new int[ownerMapping.getNumberOfColumnMappings()];
            for (int k=0;k<paramPositions.length;k++)
            {
                paramPositions[k] = inputParamNum++;
            }
            ownerStmtMapIdx.addParameterOccurrence(paramPositions);
        }

        if (ec.getTransaction().getSerializeRead() != null && ec.getTransaction().getSerializeRead())
        {
            sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
        }
        String stmt = sqlStmt.getSQLText().toSQL();

        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Create the statement
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);

                // Set the owner
                DNStateManager stmtOwnerSM = BackingStoreHelper.getOwnerStateManagerForBackingStore(ownerSM);
                int numParams = ownerStmtMapIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerStmtMapIdx.getMapping().setObject(ec, ps, ownerStmtMapIdx.getParameterPositionsForOccurrence(paramInstance), stmtOwnerSM.getObject());
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        if (elementsAreEmbedded || elementsAreSerialised)
                        {
                            // No ResultObjectFactory needed - handled by SetStoreIterator
                            return new CollectionStoreIterator(ownerSM, rs, null, this);
                        }
                        else if (elementMapping instanceof ReferenceMapping)
                        {
                            // No ResultObjectFactory needed - handled by SetStoreIterator
                            return new CollectionStoreIterator(ownerSM, rs, null, this);
                        }
                        else
                        {
                            ResultObjectFactory rof = new PersistentClassROF(ec, rs, ec.getFetchPlan(), iteratorMappingClass, elementCmd, clr.classForName(elementType));
                            return new CollectionStoreIterator(ownerSM, rs, rof, this);
                        }
                    }
                    finally
                    {
                        rs.close();
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
            throw new NucleusDataStoreException(Localiser.msg("056006", stmt),e);
        }
    }

    /**
     * Method to return the SQLStatement and mapping for an iterator for this backing store.
     * Create a statement of the form
     * <pre>
     * SELECT ELEM_COLS
     * FROM JOIN_TBL
     *   [JOIN ELEM_TBL ON ELEM_TBL.ID = JOIN_TBL.ELEM_ID]
     * [WHERE]
     *   [JOIN_TBL.OWNER_ID = {value}] [AND]
     *   [JOIN_TBL.DISCRIM = {discrimValue}]
     * [ORDER BY {orderClause}]
     * </pre>
     * This is public to provide access for BulkFetchXXXHandler class(es).
     * @param ec ExecutionContext
     * @param fp FetchPlan to use in determining which fields of element to select
     * @param addRestrictionOnOwner Whether to restrict to a particular owner (otherwise functions as bulk fetch for many owners).
     * @return The SQLStatement and its associated StatementClassMapping
     */
    public ElementIteratorStatement getIteratorStatement(ExecutionContext ec, FetchPlan fp, boolean addRestrictionOnOwner)
    {
        SelectStatement sqlStmt = null;
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        StatementClassMapping elementClsMapping = null;
        if (elementsAreEmbedded || elementsAreSerialised)
        {
            // Element = embedded, serialised (maybe Non-PC)
            // Just select the join table since we're going to return the embedded/serialised columns from it
            sqlStmt = new SelectStatement(storeMgr, containerTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);

            // Select the element column - first select is assumed by SetStoreIterator
            sqlStmt.select(sqlStmt.getPrimaryTable(), elementMapping, null);
            // TODO If embedded element and it includes 1-1/N-1 in FetchPlan then select its fields also
        }
        else if (elementMapping instanceof ReferenceMapping)
        {
            // Element = Reference type (interface/Object)
            // Just select the join table since we're going to return the implementation id columns only
            sqlStmt = new SelectStatement(storeMgr, containerTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);

            // Select the reference column(s) - first select is assumed by SetStoreIterator
            sqlStmt.select(sqlStmt.getPrimaryTable(), elementMapping, null);
        }
        else
        {
            // Element = PC
            // Join to the element table(s)
            elementClsMapping = new StatementClassMapping();
            for (int i = 0; i < elementInfo.length; i++)
            {
                final int elementNo = i;
                final Class elementCls = clr.classForName(elementInfo[elementNo].getClassName());
                SelectStatement elementStmt = null;
                if (elementInfo[elementNo].getDiscriminatorStrategy() != null && elementInfo[elementNo].getDiscriminatorStrategy() != DiscriminatorStrategy.NONE)
                {
                    // The element uses a discriminator so just use that in the SELECT
                    String elementType = ownerMemberMetaData.getCollection().getElementType();
                    if (ClassUtils.isReferenceType(clr.classForName(elementType)))
                    {
                        String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(elementType, clr);
                        Class[] cls = new Class[clsNames.length];
                        for (int j = 0; j < clsNames.length; j++)
                        {
                            cls[j] = clr.classForName(clsNames[j]);
                        }

                        SelectStatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null, containerTable, null, elementMapping);
                        if (allowNulls)
                        {
                            stmtGen.setOption(SelectStatementGenerator.OPTION_ALLOW_NULLS);
                        }
                        elementStmt = stmtGen.getStatement(ec);
                    }
                    else
                    {
                        SelectStatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, elementCls, true, null, null, containerTable, null, elementMapping);
                        if (allowNulls)
                        {
                            stmtGen.setOption(SelectStatementGenerator.OPTION_ALLOW_NULLS);
                        }
                        elementStmt = stmtGen.getStatement(ec);
                    }
                    iterateUsingDiscriminator = true;
                }
                else
                {
                    // No discriminator, but subclasses so use UNIONs
                    SelectStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, elementCls, true, null, null, containerTable, null, elementMapping);
                    stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                    elementClsMapping.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                    elementStmt = stmtGen.getStatement(ec);
                }

                // TODO What if the first elementInfo has fields selected in one order and then the second in a different order?
                if (sqlStmt == null)
                {
                    sqlStmt = elementStmt;

                    // Select the required fields
                    SQLTable elementSqlTbl = sqlStmt.getTable(elementInfo[i].getDatastoreClass(), sqlStmt.getPrimaryTable().getGroupName());
                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, elementClsMapping, fp, elementSqlTbl, elementCmd, fp.getMaxFetchDepth());
                }
                else
                {
                    // Select the required fields
                    SQLTable elementSqlTbl = elementStmt.getTable(elementInfo[i].getDatastoreClass(), elementStmt.getPrimaryTable().getGroupName());
                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(elementStmt, elementClsMapping, fp, elementSqlTbl, elementCmd, fp.getMaxFetchDepth());

                    sqlStmt.union(elementStmt);
                }
            }
        }
        if (sqlStmt == null)
        {
            throw new NucleusException("Error in generation of SQL statement for iterator over (Join) set. Statement is null");
        }

        if (addRestrictionOnOwner)
        {
            // Apply condition on join-table owner field to filter by owner
            SQLTable ownerSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerExpr = exprFactory.newExpression(sqlStmt, ownerSqlTbl, ownerMapping);
            SQLExpression ownerVal = exprFactory.newLiteralParameter(sqlStmt, ownerMapping, null, "OWNER");
            sqlStmt.whereAnd(ownerExpr.eq(ownerVal), true);
        }

        if (relationDiscriminatorMapping != null)
        {
            // Apply condition on distinguisher field to filter by distinguisher (when present)
            SQLTable distSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), relationDiscriminatorMapping);
            SQLExpression distExpr = exprFactory.newExpression(sqlStmt, distSqlTbl, relationDiscriminatorMapping);
            SQLExpression distVal = exprFactory.newLiteral(sqlStmt, relationDiscriminatorMapping, relationDiscriminatorValue);
            sqlStmt.whereAnd(distExpr.eq(distVal), true);
        }

        if (orderMapping != null)
        {
            // TODO If we have multiple roots then cannot allow this
            // Order by the ordering column, when present
            SQLTable orderSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
            SQLExpression[] orderExprs = new SQLExpression[orderMapping.getNumberOfColumnMappings()];
            boolean descendingOrder[] = new boolean[orderMapping.getNumberOfColumnMappings()];
            orderExprs[0] = exprFactory.newExpression(sqlStmt, orderSqlTbl, orderMapping);
            sqlStmt.setOrdering(orderExprs, descendingOrder);
        }

        return new ElementIteratorStatement(this, sqlStmt, elementClsMapping);
    }
}