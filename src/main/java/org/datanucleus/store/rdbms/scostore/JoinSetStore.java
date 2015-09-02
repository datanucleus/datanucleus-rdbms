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
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.exceptions.NotYetFlushedException;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.fieldmanager.DynamicSchemaFieldManager;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.StatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.CollectionTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.scostore.SetStore;
import org.datanucleus.store.types.SCO;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * RDBMS-specific implementation of a {@link SetStore} using join table.
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
                emd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(element_class, clr);
                if (emd != null && !elementsAreEmbedded)
                {
                    elementInfo = getComponentInformationForClass(elementType, emd);
                }
                else
                {
                    elementInfo = null;
                }
            }
        }
    }

    /**
     * Method to update the collection to be the supplied collection of elements.
     * @param op ObjectProvider of the object
     * @param coll The collection to use
     */
    public void update(ObjectProvider op, Collection coll)
    {
        if (coll == null || coll.isEmpty())
        {
            clear(op);
            return;
        }

        if (ownerMemberMetaData.getCollection().isSerializedElement() || ownerMemberMetaData.getCollection().isEmbeddedElement())
        {
            // Serialized/Embedded elements so just clear and add again
            clear(op);
            addAll(op, coll, 0);
            return;
        }

        // Find existing elements, and remove any that are no longer present
        Iterator elemIter = iterator(op);
        Collection existing = new HashSet();
        while (elemIter.hasNext())
        {
            Object elem = elemIter.next();
            if (!coll.contains(elem))
            {
                remove(op, elem, -1, true);
            }
            else
            {
                existing.add(elem);
            }
        }

        if (existing.size() != coll.size())
        {
            // Add any elements that aren't already present
            Iterator<E> iter = coll.iterator();
            while (iter.hasNext())
            {
                E elem = iter.next();
                if (!existing.contains(elem))
                {
                    add(op, elem, 0);
                }
            }
        }
    }

    /**
     * Convenience method to check if an element already refers to the owner in an M-N relation (i.e added from other side).
     * @param ownerOP ObjectProvider of the owner
     * @param element The element
     * @return Whether the element contains the owner
     */
    private boolean elementAlreadyContainsOwnerInMtoN(ObjectProvider ownerOP, Object element)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();

        if (ec.getOperationQueue() != null)
        {
            // Optimistic add, so additions are queued. No point checking the SCO on the other side, so just query the datastore whether already added (from other side)
            // TODO This means we always do a "SELECT 1 FROM JOINTABLE" for every addition when optimistic. Should seek to avoid this by updates to operationQueue maybe
            if (locate(ownerOP, element))
            {
                NucleusLogger.DATASTORE.info(Localiser.msg("056040", ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(ownerOP.getObject()), element));
                return true;
            }
            return false;
        }

        ObjectProvider elementOP = ec.findObjectProvider(element);
        if (elementOP != null)
        {
            // Check the collection at the other side whether already added (to avoid the locate call)
            AbstractMemberMetaData[] relatedMmds = ownerMemberMetaData.getRelatedMemberMetaData(ec.getClassLoaderResolver());
            Object elementColl = elementOP.provideField(relatedMmds[0].getAbsoluteFieldNumber());
            if (elementColl != null && elementColl instanceof Collection && elementColl instanceof SCO && ((Collection)elementColl).contains(ownerOP.getObject()))
            {
                NucleusLogger.DATASTORE.info(Localiser.msg("056040", ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(ownerOP.getObject()), element));
                return true;
            }
        }
        else
        {
            // Element is still detached
            if (locate(ownerOP, element))
            {
                NucleusLogger.DATASTORE.info(Localiser.msg("056040", ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(ownerOP.getObject()), element));
                return true;
            }
        }

        return false;
    }

    /**
     * Adds one element to the association owner vs elements.
     * @param op ObjectProvider for the container.
     * @param element Element to add
     * @return Whether it was successful
     */
    public boolean add(ObjectProvider op, E element, int size)
    {
        // Check that the object is valid for writing
        ExecutionContext ec = op.getExecutionContext();
        validateElementForWriting(ec, element, null);

        if (relationType == RelationType.ONE_TO_MANY_BI)
        {
            // TODO This is ManagedRelations - move into RelationshipManager
            // Managed Relations : make sure we have consistency of relation
            ObjectProvider elementOP = ec.findObjectProvider(element);
            if (elementOP != null)
            {
                AbstractMemberMetaData[] relatedMmds = ownerMemberMetaData.getRelatedMemberMetaData(clr);
                Object elementOwner = elementOP.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                if (elementOwner == null)
                {
                    // No owner, so correct it
                    NucleusLogger.PERSISTENCE.info(Localiser.msg("056037", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                        StringUtils.toJVMIDString(elementOP.getObject())));
                    elementOP.replaceField(relatedMmds[0].getAbsoluteFieldNumber(), op.getObject());
                }
                else if (elementOwner != op.getObject() && op.getReferencedPC() == null)
                {
                    // Owner of the element is neither this container nor being attached
                    // Inconsistent owner, so throw exception
                    throw new NucleusUserException(Localiser.msg("056038", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                        StringUtils.toJVMIDString(elementOP.getObject()), StringUtils.toJVMIDString(elementOwner)));
                }
            }
        }

        boolean modified = false;

        boolean toBeInserted = true;
        if (relationType == RelationType.MANY_TO_MANY_BI)
        {
            toBeInserted = !elementAlreadyContainsOwnerInMtoN(op, element);
        }

        if (toBeInserted)
        {
            try
            {
                ManagedConnection mconn = storeMgr.getConnection(ec);
                try
                {
                    // Add a row to the join table
                    int orderID = -1;
                    if (orderMapping != null)
                    {
                        orderID = getNextIDForOrderColumn(op);
                    }
                    int[] returnCode = doInternalAdd(op, element, mconn, false, orderID, true);
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
            catch (MappedDatastoreException e)
            {
                String msg = Localiser.msg("056009", e.getMessage());
                NucleusLogger.DATASTORE.error(msg, e);
                throw new NucleusDataStoreException(msg, e);
            }
        }

        return modified;
    }

    /**
     * Adds all elements from a collection to the association container.
     * @param op ObjectProvider for the container.
     * @param elements Collection of elements to add
     * @return Whether it was successful
     */
    public boolean addAll(ObjectProvider op, Collection<E> elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        List exceptions = new ArrayList();
        boolean batched = (elements.size() > 1);

        // Validate all elements for writing
        ExecutionContext ec = op.getExecutionContext();
        Iterator<E> iter = elements.iterator();
        while (iter.hasNext())
        {
            Object element = iter.next();
            validateElementForWriting(ec, element, null);

            if (relationType == RelationType.ONE_TO_MANY_BI)
            {
                // TODO This is ManagedRelations - move into RelationshipManager
                // Managed Relations : make sure we have consistency of relation
                ObjectProvider elementOP = op.getExecutionContext().findObjectProvider(element);
                if (elementOP != null)
                {
                    AbstractMemberMetaData[] relatedMmds = ownerMemberMetaData.getRelatedMemberMetaData(clr);
                    Object elementOwner = elementOP.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                    if (elementOwner == null)
                    {
                        // No owner, so correct it
                        NucleusLogger.PERSISTENCE.info(Localiser.msg("056037", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                            StringUtils.toJVMIDString(elementOP.getObject())));
                        elementOP.replaceField(relatedMmds[0].getAbsoluteFieldNumber(), op.getObject());
                    }
                    else if (elementOwner != op.getObject() && op.getReferencedPC() == null)
                    {
                        // Owner of the element is neither this container nor its referenced object
                        // Inconsistent owner, so throw exception
                        throw new NucleusUserException(Localiser.msg("056038", op.getObjectAsPrintable(),
                            ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(elementOP.getObject()), StringUtils.toJVMIDString(elementOwner)));
                    }
                }
            }
        }

        boolean modified = false;
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
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
                    throw new MappedDatastoreException("SQLException", e);
                }

                int nextOrderID = 0;
                if (orderMapping != null)
                {
                    // Get the order id for the first item
                    nextOrderID = getNextIDForOrderColumn(op);
                }

                // Loop through all elements to be added
                iter = elements.iterator();
                E element = null;
                while (iter.hasNext())
                {
                    element = iter.next();

                    try
                    {
                        // Add the row to the join table
                        boolean toBeInserted = true;
                        if (relationType == RelationType.MANY_TO_MANY_BI)
                        {
                            toBeInserted = !elementAlreadyContainsOwnerInMtoN(op, element);
                        }

                        if (toBeInserted)
                        {
                            int[] rc = doInternalAdd(op, element, mconn, batched, nextOrderID, !batched || (batched && !iter.hasNext()));
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
                    catch (MappedDatastoreException mde)
                    {
                        exceptions.add(mde);
                        NucleusLogger.DATASTORE.error("Exception thrown", mde);
                    }
                }
            }
            finally
            {
                mconn.release();
            }
        }
        catch (MappedDatastoreException e)
        {
            exceptions.add(e);
            NucleusLogger.DATASTORE.error("Exception thrown", e);
        }

        if (!exceptions.isEmpty())
        {
            // Throw all exceptions received as the cause of a NucleusDataStoreException so the user can see which record(s) didn't persist
            String msg = Localiser.msg("056009", ((Exception) exceptions.get(0)).getMessage());
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusDataStoreException(msg, (Throwable[]) exceptions.toArray(new Throwable[exceptions.size()]), op.getObject());
        }

        return modified;
    }

    /**
     * Remove all elements from a collection from the association owner vs elements.
     * @param op ObjectProvider for the container
     * @param elements Collection of elements to remove
     * @return Whether the database was updated
     */
    public boolean removeAll(ObjectProvider op, Collection elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        boolean modified = removeAllInternal(op, elements, size);
        boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent)
        {
            // "delete-dependent" : delete elements if the collection is marked as dependent
            // TODO What if the collection contains elements that are not in the Set ? should not delete them
            op.getExecutionContext().deleteObjects(elements.toArray());
        }

        return modified;
    }

    protected boolean removeAllInternal(ObjectProvider op, Collection elements, int size)
    {
        boolean modified = false;

        String removeAllStmt = getRemoveAllStmt(op, elements);
        try
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
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
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
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
     * @param op ObjectProvider for the owner
     * @param elements Collection of elements to remove
     * @return Statement for deleting items from the Set.
     */
    protected String getRemoveAllStmt(ObjectProvider op, Collection elements)
    {
        if (elements == null || elements.size() == 0)
        {
            return null;
        }

        StringBuilder stmt = new StringBuilder("DELETE FROM ").append(containerTable.toString()).append(" WHERE ");

        Iterator elementsIter = elements.iterator();
        boolean first = true;
        while (elementsIter.hasNext())
        {
            Object element = elementsIter.next();
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

    private boolean locate(ObjectProvider op, Object element)
    {
        boolean exists = true;
        String stmt = getLocateStmt(element);
        try
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
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

    protected int[] doInternalAdd(ObjectProvider op, E element, ManagedConnection conn, boolean batched, int orderId, boolean executeNow)
    throws MappedDatastoreException
    {
        // Check for dynamic schema updates prior to addition
        if (storeMgr.getBooleanObjectProperty(RDBMSPropertyNames.PROPERTY_RDBMS_DYNAMIC_SCHEMA_UPDATES).booleanValue())
        {
            DynamicSchemaFieldManager dynamicSchemaFM = new DynamicSchemaFieldManager(storeMgr, op);
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
        ExecutionContext ec = op.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, addStmt, batched);
            try
            {
                // Insert the join table row
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, element, jdbcPosition, elementMapping);
                if (orderMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, orderId, jdbcPosition, orderMapping);
                }
                if (relationDiscriminatorMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                }

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
            throw new MappedDatastoreException(addStmt, e);
        }
    }

    private synchronized String getLocateStmt(Object element)
    {
        if (elementMapping instanceof ReferenceMapping && elementMapping.getNumberOfDatastoreMappings() > 1)
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
    private String getLocateStatementString(Object element)
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

    protected int getNextIDForOrderColumn(ObjectProvider op)
    {
        int nextID;
        ExecutionContext ec = op.getExecutionContext();
        String stmt = getMaxOrderColumnIdStmt();
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();

            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    if (relationDiscriminatorMapping != null)
                    {
                        BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        if (!rs.next())
                        {
                            nextID = 1;
                        }
                        else
                        {
                            nextID = rs.getInt(1) + 1;
                        }

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
                StringBuilder stmt = new StringBuilder("SELECT MAX(" + orderMapping.getDatastoreMapping(0).getColumn().getIdentifier().toString() + ")");
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

    /**
     * Accessor for an iterator for the set.
     * @param ownerOP ObjectProvider for the set.
     * @return Iterator for the set.
     */
    public Iterator<E> iterator(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();

        // Generate the statement, and statement mapping/parameter information
        IteratorStatement iterStmt = getIteratorStatement(ec.getClassLoaderResolver(), ec.getFetchPlan(), true);
        SQLStatement sqlStmt = iterStmt.sqlStmt;
        StatementClassMapping iteratorMappingClass = iterStmt.stmtClassMapping;

        // Input parameter(s) - the owner
        int inputParamNum = 1;
        StatementMappingIndex ownerStmtMapIdx = new StatementMappingIndex(ownerMapping);
        if (sqlStmt.getNumberOfUnions() > 0)
        {
            // Add parameter occurrence for each union of statement
            for (int j=0;j<sqlStmt.getNumberOfUnions()+1;j++)
            {
                int[] paramPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
                for (int k=0;k<paramPositions.length;k++)
                {
                    paramPositions[k] = inputParamNum++;
                }
                ownerStmtMapIdx.addParameterOccurrence(paramPositions);
            }
        }
        else
        {
            int[] paramPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
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
        String stmt = sqlStmt.getSelectStatement().toSQL();

        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Create the statement and set the owner
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                int numParams = ownerStmtMapIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerStmtMapIdx.getMapping().setObject(ec, ps, ownerStmtMapIdx.getParameterPositionsForOccurrence(paramInstance), ownerOP.getObject());
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        if (elementsAreEmbedded || elementsAreSerialised)
                        {
                            // No ResultObjectFactory needed - handled by SetStoreIterator
                            return new CollectionStoreIterator(ownerOP, rs, null, this);
                        }
                        else if (elementMapping instanceof ReferenceMapping)
                        {
                            // No ResultObjectFactory needed - handled by SetStoreIterator
                            return new CollectionStoreIterator(ownerOP, rs, null, this);
                        }
                        else
                        {
                            ResultObjectFactory rof = new PersistentClassROF(storeMgr, emd, iteratorMappingClass, false, null, clr.classForName(elementType));
                            return new CollectionStoreIterator(ownerOP, rs, rof, this);
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
        catch (MappedDatastoreException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056006", stmt),e);
        }
    }

    /**
     * Method to return the SQLStatement and mapping for an iterator for this backing store.
     * @param clr ClassLoader resolver
     * @param fp FetchPlan to use in determining which fields of element to select
     * @param addRestrictionOnOwner Whether to restrict to a particular owner (otherwise functions as bulk fetch for many owners).
     * @return The SQLStatement and its associated StatementClassMapping
     */
    public IteratorStatement getIteratorStatement(ClassLoaderResolver clr, FetchPlan fp, boolean addRestrictionOnOwner)
    {
        SQLStatement sqlStmt = null;
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        StatementClassMapping iteratorMappingClass = null;
        if (elementsAreEmbedded || elementsAreSerialised)
        {
            // Element = embedded, serialised (maybe Non-PC)
            // Just select the join table since we're going to return the embedded/serialised columns from it
            sqlStmt = new SQLStatement(storeMgr, containerTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);

            // Select the element column - first select is assumed by SetStoreIterator
            sqlStmt.select(sqlStmt.getPrimaryTable(), elementMapping, null);
        }
        else if (elementMapping instanceof ReferenceMapping)
        {
            // Element = Reference type (interface/Object)
            // Just select the join table since we're going to return the implementation id columns only
            sqlStmt = new SQLStatement(storeMgr, containerTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);

            // Select the reference column(s) - first select is assumed by SetStoreIterator
            sqlStmt.select(sqlStmt.getPrimaryTable(), elementMapping, null);
        }
        else
        {
            // Element = PC
            // Join to the element table(s)
            iteratorMappingClass = new StatementClassMapping();
            for (int i = 0; i < elementInfo.length; i++)
            {
                final int elementNo = i;
                final Class elementCls = clr.classForName(elementInfo[elementNo].getClassName());
                SQLStatement elementStmt = null;
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

                        StatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null, containerTable, null, elementMapping);
                        if (allowNulls)
                        {
                            stmtGen.setOption(StatementGenerator.OPTION_ALLOW_NULLS);
                        }
                        elementStmt = stmtGen.getStatement();
                    }
                    else
                    {
                        StatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, elementCls, true, null, null, containerTable, null, elementMapping);
                        if (allowNulls)
                        {
                            stmtGen.setOption(StatementGenerator.OPTION_ALLOW_NULLS);
                        }
                        elementStmt = stmtGen.getStatement();
                    }
                    iterateUsingDiscriminator = true;
                }
                else
                {
                    // No discriminator, but subclasses so use UNIONs
                    StatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, elementCls, true, null, null, containerTable, null, elementMapping);
                    stmtGen.setOption(StatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                    iteratorMappingClass.setNucleusTypeColumnName(UnionStatementGenerator.NUC_TYPE_COLUMN);
                    elementStmt = stmtGen.getStatement();
                }

                // TODO What if the first elementInfo has fields selected in one order and then the second in a different order?
                if (sqlStmt == null)
                {
                    sqlStmt = elementStmt;

                    // Select the required fields
                    SQLTable elementSqlTbl = sqlStmt.getTable(elementInfo[i].getDatastoreClass(), sqlStmt.getPrimaryTable().getGroupName());
                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingClass, fp, elementSqlTbl, emd, 0);
                }
                else
                {
                    // Select the required fields
                    SQLTable elementSqlTbl = elementStmt.getTable(elementInfo[i].getDatastoreClass(), elementStmt.getPrimaryTable().getGroupName());
                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(elementStmt, iteratorMappingClass, fp, elementSqlTbl, emd, 0);

                    sqlStmt.union(elementStmt);
                }
            }
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
            SQLExpression[] orderExprs = new SQLExpression[orderMapping.getNumberOfDatastoreMappings()];
            boolean descendingOrder[] = new boolean[orderMapping.getNumberOfDatastoreMappings()];
            orderExprs[0] = exprFactory.newExpression(sqlStmt, orderSqlTbl, orderMapping);
            sqlStmt.setOrdering(orderExprs, descendingOrder);
        }

        return new IteratorStatement(this, sqlStmt, iteratorMappingClass);
    }
}