/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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
import java.util.Iterator;
import java.util.ListIterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.Transaction;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.OrderMetaData.FieldOrder;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.fieldmanager.DynamicSchemaFieldManager;
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
import org.datanucleus.store.scostore.ListStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * RDBMS-specific implementation of a {@link ListStore} using join table.
 */
public class JoinListStore extends AbstractListStore
{
    private String setStmt;

    /**
     * Constructor for an RDBMS implementation of a join list store.
     * @param mmd Metadata for the owning field/property
     * @param collTable The Join table
     * @param clr ClassLoader resolver
     */
    public JoinListStore(AbstractMemberMetaData mmd, CollectionTable collTable, ClassLoaderResolver clr)
    {
        super(collTable.getStoreManager(), clr);

        // A List really needs a ListTable, but we need to cope with the situation
        // where a user declares a field as Collection but is instantiated as a List or a Set
        // so we just accept CollectionTable and rely on it being adequate
        this.containerTable = collTable;
        setOwner(mmd);

        this.ownerMapping = collTable.getOwnerMapping();
        this.elementMapping = collTable.getElementMapping();

        this.orderMapping = collTable.getOrderMapping();
        if (ownerMemberMetaData.getOrderMetaData() != null && !ownerMemberMetaData.getOrderMetaData().isIndexedList())
        {
            indexedList = false;
        }
        if (orderMapping == null && indexedList)
        {
            // If the user declares a field as java.util.Collection we use SetTable to generate the join table
            // If they then instantiate it as a List type it will come through here, so we need to ensure the order column exists
            throw new NucleusUserException(Localiser.msg("056044", 
                ownerMemberMetaData.getFullFieldName(), collTable.toString()));
        }
        this.relationDiscriminatorMapping = collTable.getRelationDiscriminatorMapping();
        this.relationDiscriminatorValue = collTable.getRelationDiscriminatorValue();

        elementType = mmd.getCollection().getElementType();
        this.elementsAreEmbedded = collTable.isEmbeddedElement();
        this.elementsAreSerialised = collTable.isSerialisedElement();

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
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(ownerMemberMetaData, 
                    FieldRole.ROLE_COLLECTION_ELEMENT, clr, storeMgr.getMetaDataManager());
                elementInfo = new ElementInfo[implNames.length];
                for (int i=0;i<implNames.length;i++)
                {
                    DatastoreClass table = storeMgr.getDatastoreClass(implNames[i], clr);
                    AbstractClassMetaData cmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(implNames[i], clr);
                    elementInfo[i] = new ElementInfo(cmd,table);
                }
            }
            else
            {
                // Collection of PC or non-PC
                // Generate the information for the possible elements
                emd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(element_class, clr);
                if (emd != null)
                {
                    if (!elementsAreEmbedded)
                    {
                        elementInfo = getElementInformationForClass();
                        /*if (elementInfo != null && elementInfo.length > 1)
                        {
                            throw new NucleusUserException(Localiser.msg("056031", 
                                ownerFieldMetaData.getFullFieldName()));
                        }*/
                    }
                    else
                    {
                        elementInfo = null;
                    }
                }
                else
                {
                    elementInfo = null;
                }
            }
        }
    }

    /**
     * Internal method to add element(s) to the List.
     * Performs the add in 2 steps.
     * <ol>
     * <li>Shift all existing elements into their new positions so we can insert.</li>
     * <li>Insert all new elements directly at their desired positions</li>
     * </ol>
     * Both steps can be batched (separately).
     * @param op The ObjectProvider
     * @param start The start location (if required)
     * @param atEnd Whether to add the element at the end
     * @param c The collection of objects to add.
     * @param size Current size of list if known. -1 if not known
     * @return Whether it was successful
     */
    protected boolean internalAdd(ObjectProvider op, int start, boolean atEnd, Collection c, int size)
    {
        if (c == null || c.size() == 0)
        {
            return true;
        }

        if (relationType == RelationType.MANY_TO_MANY_BI && ownerMemberMetaData.getMappedBy() != null)
        {
            // M-N non-owner : don't add from this side to avoid duplicates
            return true;
        }

        // Calculate the amount we need to shift any existing elements by
        // This is used where inserting between existing elements and have to shift down all elements after the start point
        int shift = c.size();

        // check all elements are valid for persisting and exist (persistence-by-reachability)
        ExecutionContext ec = op.getExecutionContext();
        Iterator iter = c.iterator();
        while (iter.hasNext())
        {
            Object element = iter.next();
            validateElementForWriting(ec, element, null);

            if (relationType == RelationType.ONE_TO_MANY_BI)
            {
                // TODO This is ManagedRelations - move into RelationshipManager
                ObjectProvider elementSM = ec.findObjectProvider(element);
                if (elementSM != null)
                {
                    AbstractMemberMetaData[] relatedMmds = ownerMemberMetaData.getRelatedMemberMetaData(clr);
                    // TODO Cater for more than 1 related field
                    Object elementOwner = elementSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                    if (elementOwner == null)
                    {
                        // No owner, so correct it
                        NucleusLogger.PERSISTENCE.info(Localiser.msg("056037",
                            op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                            StringUtils.toJVMIDString(elementSM.getObject())));
                        elementSM.replaceField(relatedMmds[0].getAbsoluteFieldNumber(), op.getObject());
                    }
                    else if (elementOwner != op.getObject() && op.getReferencedPC() == null)
                    {
                        // Owner of the element is neither this container nor being attached
                        // Inconsistent owner, so throw exception
                        throw new NucleusUserException(Localiser.msg("056038",
                            op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                            StringUtils.toJVMIDString(elementSM.getObject()),
                            StringUtils.toJVMIDString(elementOwner)));
                    }
                }
            }

        }

        // Check what we have persistent already
        int currentListSize = 0;
        if (size < 0)
        {
            // Get the current size from the datastore
            currentListSize = size(op);
        }
        else
        {
            currentListSize = size;
        }

        // Check for dynamic schema updates prior to addition
        if (storeMgr.getBooleanObjectProperty(RDBMSPropertyNames.PROPERTY_RDBMS_DYNAMIC_SCHEMA_UPDATES).booleanValue())
        {
            DynamicSchemaFieldManager dynamicSchemaFM = new DynamicSchemaFieldManager(storeMgr, op);
            dynamicSchemaFM.storeObjectField(getOwnerMemberMetaData().getAbsoluteFieldNumber(), c);
            if (dynamicSchemaFM.hasPerformedSchemaUpdates())
            {
                invalidateAddStmt();
            }
        }

        String addStmt = getAddStmtForJoinTable();
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Shift any existing elements so that we can insert the new element(s) at their position
                if (!atEnd && start != currentListSize)
                {
                    boolean batched = currentListSize - start > 0;

                    for (int i = currentListSize - 1; i >= start; i--)
                    {
                        // Shift the index for this row by "shift"
                        internalShift(op, mconn, batched, i, shift, (i == start));
                    }
                }
                else
                {
                    start = currentListSize;
                }

                // Insert the elements at their required location
                int jdbcPosition = 1;
                boolean batched = (c.size() > 1);

                Iterator elemIter = c.iterator();
                while (elemIter.hasNext())
                {
                    Object element = elemIter.next();
                    PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, addStmt, batched);
                    try
                    {
                        JavaTypeMapping orderMapping = getOrderMapping();
                        JavaTypeMapping elementMapping = getElementMapping();
                        JavaTypeMapping relationDiscriminatorMapping = getRelationDiscriminatorMapping();

                        jdbcPosition = 1;
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                        jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, element, jdbcPosition, elementMapping);
                        if (orderMapping != null)
                        {
                            jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, start, jdbcPosition, orderMapping);
                        }
                        if (relationDiscriminatorMapping != null)
                        {
                            jdbcPosition =
                                BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                        }
                        start++;

                        // Execute the statement
                        sqlControl.executeStatementUpdate(ec, mconn, addStmt, ps, !iter.hasNext());
                    }
                    finally
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
        catch (MappedDatastoreException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056009", addStmt), e);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056009", addStmt), e);
        }
        return true;
    }

    /**
     * Method to set an object in the List.
     * @param op ObjectProvider for the owner
     * @param index The item index
     * @param element What to set it to.
     * @param allowDependentField Whether to allow dependent field deletes
     * @return The value before setting.
     */
    public Object set(ObjectProvider op, int index, Object element, boolean allowDependentField)
    {
        ExecutionContext ec = op.getExecutionContext();
        validateElementForWriting(ec, element, null);
        Object oldElement = get(op, index);

        // Check for dynamic schema updates prior to update
        if (storeMgr.getBooleanObjectProperty(RDBMSPropertyNames.PROPERTY_RDBMS_DYNAMIC_SCHEMA_UPDATES).booleanValue())
        {
            DynamicSchemaFieldManager dynamicSchemaFM = new DynamicSchemaFieldManager(storeMgr, op);
            Collection coll = new ArrayList();
            coll.add(element);
            dynamicSchemaFM.storeObjectField(getOwnerMemberMetaData().getAbsoluteFieldNumber(), coll);
            if (dynamicSchemaFM.hasPerformedSchemaUpdates())
            {
                setStmt = null;
            }
        }

        String setStmt = getSetStmt();
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, setStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, element, jdbcPosition, elementMapping);
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    if (getOwnerMemberMetaData().getOrderMetaData() != null && 
                        !getOwnerMemberMetaData().getOrderMetaData().isIndexedList())
                    {
                        // Ordered list, so can't easily do a set!!!
                        NucleusLogger.PERSISTENCE.warn("Calling List.addElement at a position for an ordered list is a stupid thing to do; the ordering is set my the ordering specification. Use an indexed list to do this correctly");
                    }
                    else
                    {
                        jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, index, jdbcPosition, orderMapping);
                    }
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = 
                            BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    sqlControl.executeStatementUpdate(ec, mconn, setStmt, ps, true);
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
            throw new NucleusDataStoreException(Localiser.msg("056015", setStmt), e);
        }

        // Dependent field
        CollectionMetaData collmd = ownerMemberMetaData.getCollection();
        boolean dependent = collmd.isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent && !collmd.isEmbeddedElement() && allowDependentField)
        {
            if (oldElement != null && !contains(op, oldElement))
            {
                // Delete the element if it is dependent and doesn't have a duplicate entry in the list
                ec.deleteObjectInternal(oldElement);
            }
        }

        return oldElement;
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

        if (ownerMemberMetaData.getCollection().isSerializedElement() || 
            ownerMemberMetaData.getCollection().isEmbeddedElement())
        {
            // Serialized/Embedded elements so just clear and add again
            clear(op);
            addAll(op, coll, 0);
            return;
        }

        // Find existing elements, and remove any that are no longer present
        Collection existing = new ArrayList();
        Iterator elemIter = iterator(op);
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

        if (existing.equals(coll))
        {
            // Existing (after any removals) is same as the specified so job done
            return;
        }

        // TODO Improve this - need to allow for list element position changes etc
        clear(op);
        addAll(op, coll, 0);
    }

    /**
     * Convenience method to remove the specified element from the List.
     * @param element The element
     * @param ownerOP ObjectProvider of the owner
     * @param size Current size of list if known. -1 if not known
     * @return Whether the List was modified
     */
    protected boolean internalRemove(ObjectProvider ownerOP, Object element, int size)
    {
        boolean modified = false;
        if (indexedList)
        {
            // Indexed List, so retrieve the index of the element and remove the object
            // Get the indices of the elements to remove in reverse order (highest first)
            // This is done because the element could be duplicated in the list.
            Collection elements = new ArrayList();
            elements.add(element);
            int[] indices = getIndicesOf(ownerOP, elements);

            // Remove each element in turn, doing the shifting of indexes each time
            // TODO : Change this to remove all in one go and then shift once
            for (int i=0;i<indices.length;i++)
            {
                internalRemoveAt(ownerOP, indices[i], size);
                modified = true;
            }
        }
        else
        {
            // Ordered List - just remove the list item since no indexing present
            ExecutionContext ec = ownerOP.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                int[] rcs = internalRemove(ownerOP, mconn, false, element, true);
                if (rcs != null)
                {
                    if (rcs[0] > 0)
                    {
                        modified = true;
                    }
                }
            }
            catch (MappedDatastoreException sqe)
            {
                String msg = Localiser.msg("056012", sqe.getMessage());
                NucleusLogger.DATASTORE.error(msg, sqe.getCause());
                throw new NucleusDataStoreException(msg, sqe, ownerOP.getObject());
            }
            finally
            {
                mconn.release();
            }
        }

        return modified;
    }

    private int[] internalRemove(ObjectProvider op, ManagedConnection conn, boolean batched, Object element, boolean executeNow) 
    throws MappedDatastoreException
    {
        ExecutionContext ec = op.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        String removeStmt = getRemoveStmt(element);
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, removeStmt, batched);
            try
            {
                int jdbcPosition = 1;

                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, jdbcPosition, elementMapping);
                if (relationDiscriminatorMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                }

                // Execute the statement
                return sqlControl.executeStatementUpdate(ec, conn, removeStmt, ps, executeNow);
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
        catch (SQLException sqle)
        {
            throw new MappedDatastoreException("SQLException", sqle);
        }
    }

    /**
     * Remove all elements from a collection from the association owner vs
     * elements. Performs the removal in 3 steps. The first gets the indices
     * that will be removed (and the highest index present). The second step
     * removes these elements from the list. The third step updates the indices
     * of the remaining indices to fill the holes created.
     * @param op ObjectProvider
     * @param elements Collection of elements to remove 
     * @return Whether the database was updated 
     */
    public boolean removeAll(ObjectProvider op, Collection elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        // Get the current size of the list (and hence maximum index size)
        int currentListSize = size(op);

        // Get the indices of the elements we are going to remove (highest first)
        int[] indices = getIndicesOf(op, elements);

        boolean modified = false;
        SQLController sqlControl = storeMgr.getSQLController();
        ExecutionContext ec = op.getExecutionContext();

        // Remove the specified elements from the join table
        String removeAllStmt = getRemoveAllStmt(elements);
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
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
            NucleusLogger.DATASTORE.error(e);
            throw new NucleusDataStoreException(Localiser.msg("056012", removeAllStmt), e);
        }

        // Shift the remaining indices to remove the holes in ordering
        try
        {
            boolean batched = storeMgr.allowsBatching();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                for (int i = 0; i < currentListSize; i++)
                {
                    // Find the number of deleted indexes above this index
                    int shift = 0;
                    boolean removed = false;
                    for (int j = 0; j < indices.length; j++)
                    {
                        if (indices[j] == i)
                        {
                            removed = true;
                            break;
                        }
                        if (indices[j] < i)
                        {
                            shift++;
                        }
                    }
                    if (!removed && shift > 0)
                    {
                        internalShift(op, mconn, batched, i, -1 * shift, (i == currentListSize - 1));
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
            NucleusLogger.DATASTORE.error(e);
            throw new NucleusDataStoreException(Localiser.msg("056012", removeAllStmt), e);
        }

        // Dependent field
        boolean dependent = getOwnerMemberMetaData().getCollection().isDependentElement();
        if (getOwnerMemberMetaData().isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent)
        {
            // "delete-dependent" : delete elements if the collection is marked as dependent
            // TODO What if the collection contains elements that are not in the List ? should not delete them
            op.getExecutionContext().deleteObjects(elements.toArray());
        }

        return modified;
    }

    /**
     * Method to remove an element from the specified position
     * @param op The ObjectProvider for the list
     * @param index The index of the element
     * @param size Current size of list (if known). -1 if not known
     */
    protected void internalRemoveAt(ObjectProvider op, int index, int size)
    {
        if (!indexedList)
        {
            throw new NucleusUserException("Cannot remove an element from a particular position with an ordered list since no indexes exist");
        }

        internalRemoveAt(op, index, getRemoveAtStmt(), size);
    }

    /**
     * Accessor for an iterator through the list elements.
     * @param op ObjectProvider for the owner
     * @param startIdx The start point in the list (only for indexed lists).
     * @param endIdx End index in the list (only for indexed lists).
     * @return The List Iterator
     */
    protected ListIterator listIterator(ObjectProvider op, int startIdx, int endIdx)
    {
        ExecutionContext ec = op.getExecutionContext();
        Transaction tx = ec.getTransaction();

        // Generate the statement. Note that this is not cached since depends on the current FetchPlan and other things
        IteratorStatement iterStmt = getIteratorStatement(op.getExecutionContext().getClassLoaderResolver(), ec.getFetchPlan(), true, startIdx, endIdx);
        SQLStatement sqlStmt = iterStmt.getSQLStatement();
        StatementClassMapping resultMapping = iterStmt.getStatementClassMapping();

        // Input parameter(s) - the owner
        int inputParamNum = 1;
        StatementMappingIndex ownerIdx = new StatementMappingIndex(ownerMapping);
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
                ownerIdx.addParameterOccurrence(paramPositions);
            }
        }
        else
        {
            int[] paramPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<paramPositions.length;k++)
            {
                paramPositions[k] = inputParamNum++;
            }
            ownerIdx.addParameterOccurrence(paramPositions);
        }

        if (tx.getSerializeRead() != null && tx.getSerializeRead())
        {
            sqlStmt.addExtension("lock-for-update", true);
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
                int numParams = ownerIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerIdx.getMapping().setObject(ec, ps,
                        ownerIdx.getParameterPositionsForOccurrence(paramInstance), op.getObject());
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        if (elementsAreEmbedded || elementsAreSerialised)
                        {
                            // No ResultObjectFactory needed - handled by SetStoreIterator
                            return new ListStoreIterator(op, rs, null, this);
                        }
                        else if (elementMapping instanceof ReferenceMapping)
                        {
                            // No ResultObjectFactory needed - handled by SetStoreIterator
                            return new ListStoreIterator(op, rs, null, this);
                        }
                        else
                        {
                            ResultObjectFactory rof = storeMgr.newResultObjectFactory(emd, 
                                resultMapping, false, null, clr.classForName(elementType));
                            return new ListStoreIterator(op, rs, rof, this);
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
     * Generates the statement for setting an item.
     * <PRE>
     * UPDATE LISTTABLE SET [ELEMENTCOL = ?]
     * [EMBEDDEDFIELD1=?, EMBEDDEDFIELD2=?, ...]
     * WHERE OWNERCOL = ?
     * AND INDEXCOL = ?
     * [AND DISTINGUISHER=?]
     * </PRE>
     * @return The Statement for setting an item
     */
    protected String getSetStmt()
    {
        if (setStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("UPDATE ");
                stmt.append(containerTable.toString());
                stmt.append(" SET ");
                for (int i = 0; i < elementMapping.getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(",");
                    }
                    stmt.append(elementMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    stmt.append(" = ");
                    stmt.append(((AbstractDatastoreMapping) elementMapping.getDatastoreMapping(i)).getUpdateInputParameter());
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                if (getOwnerMemberMetaData().getOrderMetaData() == null || 
                        getOwnerMemberMetaData().getOrderMetaData().isIndexedList())
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, orderMapping, null, false);
                }
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }
                setStmt = stmt.toString();
            }
        }

        return setStmt;
    }

    /**
     * Generate statement for removing a collection of items from the List.
     * <PRE>
     * DELETE FROM LISTTABLE
     * WHERE (OWNERCOL=? AND ELEMENTCOL=?) OR
     * (OWNERCOL=? AND ELEMENTCOL=?) OR
     * (OWNERCOL=? AND ELEMENTCOL=?)
     * </PRE>
     * @param elements Collection of elements to remove
     * @return Statement for deleting items from the List.
     */
    protected String getRemoveAllStmt(Collection elements)
    {
        if (elements == null || elements.size() == 0)
        {
            return null;
        }

        StringBuilder stmt = new StringBuilder("DELETE FROM ");
        stmt.append(containerTable.toString());
        stmt.append(" WHERE ");

        boolean first = true;
        Iterator elementsIter = elements.iterator();
        while (elementsIter.hasNext())
        {
            Object element = elementsIter.next();
            if (first)
            {
                stmt.append("(");
            }
            else
            {
                stmt.append(" OR (");
            }

            BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
            BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, isElementsAreSerialised(),
                null, false);
            if (relationDiscriminatorMapping != null)
            {
                BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
            }

            stmt.append(")");
            first = false;
        }

        return stmt.toString();
    }

    /**
     * Method to return the SQLStatement and mapping for an iterator for this backing store.
     * @param clr ClassLoader resolver
     * @param fp FetchPlan to use in determing which fields of element to select
     * @param addRestrictionOnOwner Whether to restrict to a particular owner (otherwise functions as bulk fetch for many owners).
     * @param startIdx Start index for the iterator (or -1)
     * @param endIdx End index for the iterator (or -1)
     * @return The SQLStatement and its associated StatementClassMapping
     */
    public IteratorStatement getIteratorStatement(ClassLoaderResolver clr, FetchPlan fp, boolean addRestrictionOnOwner, int startIdx, int endIdx)
    {
        SQLStatement sqlStmt = null;
        StatementClassMapping stmtClassMapping = new StatementClassMapping();
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        if (elementsAreEmbedded || elementsAreSerialised)
        {
            // Element = embedded, serialised (maybe Non-PC)
            // Just select the join table since we're going to return the embedded/serialised columns from it
            sqlStmt = new SQLStatement(storeMgr, containerTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);

            // Select the element column - first select is assumed by ListStoreIterator
            sqlStmt.select(sqlStmt.getPrimaryTable(), elementMapping, null);
        }
        else if (elementMapping instanceof ReferenceMapping)
        {
            // Element = Reference type (interface/Object)
            // Just select the join table since we're going to return the implementation id columns only
            sqlStmt = new SQLStatement(storeMgr, containerTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);

            // Select the reference column(s) - first select is assumed by ListStoreIterator
            sqlStmt.select(sqlStmt.getPrimaryTable(), elementMapping, null);
        }
        else
        {
            // Element = PC
            // Join to the element table(s)
            for (int i = 0; i < elementInfo.length; i++)
            {
                // TODO This will only work if all element types have a discriminator
                final int elementNo = i;
                final Class elementCls = clr.classForName(elementInfo[elementNo].getClassName());
                SQLStatement elementStmt = null;
                if (elementInfo[elementNo].getDiscriminatorStrategy() != null &&
                    elementInfo[elementNo].getDiscriminatorStrategy() != DiscriminatorStrategy.NONE)
                {
                    // The element uses a discriminator so just use that in the SELECT
                    String elementType = ownerMemberMetaData.getCollection().getElementType();
                    if (ClassUtils.isReferenceType(clr.classForName(elementType)))
                    {
                        String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(
                            elementType, clr);
                        Class[] cls = new Class[clsNames.length];
                        for (int j = 0; j < clsNames.length; j++)
                        {
                            cls[j] = clr.classForName(clsNames[j]);
                        }

                        StatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, cls, 
                            true, null, null, containerTable, null, elementMapping);
                        if (allowNulls)
                        {
                            stmtGen.setOption(StatementGenerator.OPTION_ALLOW_NULLS);
                        }
                        elementStmt = stmtGen.getStatement();
                    }
                    else
                    {
                        StatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, elementCls,
                            true, null, null, containerTable, null, elementMapping);
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
                    StatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, elementCls, true, null,
                        null, containerTable, null, elementMapping);
                    stmtGen.setOption(StatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                    stmtClassMapping.setNucleusTypeColumnName(UnionStatementGenerator.NUC_TYPE_COLUMN);
                    elementStmt = stmtGen.getStatement();
                }

                if (sqlStmt == null)
                {
                    sqlStmt = elementStmt;
                }
                else
                {
                    sqlStmt.union(elementStmt);
                }
            }

            // Select the required fields
            SQLTable elementSqlTbl = sqlStmt.getTable(elementInfo[0].getDatastoreClass(),
                sqlStmt.getPrimaryTable().getGroupName());
            SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, stmtClassMapping,
                fp, elementSqlTbl, emd, 0);
        }

        if (addRestrictionOnOwner)
        {
            // Apply condition on join-table owner field to filter by owner
            SQLTable ownerSqlTbl =
                    SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerExpr = exprFactory.newExpression(sqlStmt, ownerSqlTbl, ownerMapping);
            SQLExpression ownerVal = exprFactory.newLiteralParameter(sqlStmt, ownerMapping, null, "OWNER");
            sqlStmt.whereAnd(ownerExpr.eq(ownerVal), true);
        }

        if (relationDiscriminatorMapping != null)
        {
            // Apply condition on distinguisher field to filter by distinguisher (when present)
            SQLTable distSqlTbl =
                SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), relationDiscriminatorMapping);
            SQLExpression distExpr = exprFactory.newExpression(sqlStmt, distSqlTbl, relationDiscriminatorMapping);
            SQLExpression distVal = exprFactory.newLiteral(sqlStmt, relationDiscriminatorMapping, relationDiscriminatorValue);
            sqlStmt.whereAnd(distExpr.eq(distVal), true);
        }

        if (indexedList)
        {
            // "Indexed List" so allow restriction on returned indexes
            boolean needsOrdering = true;
            if (startIdx == -1 && endIdx == -1)
            {
                // Just restrict to >= 0 so we don't get any disassociated elements
                SQLExpression indexExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
                SQLExpression indexVal = exprFactory.newLiteral(sqlStmt, orderMapping, 0);
                sqlStmt.whereAnd(indexExpr.ge(indexVal), true);
            }
            else if (startIdx >= 0 && endIdx == startIdx)
            {
                // Particular index required so add restriction
                needsOrdering = false;
                SQLExpression indexExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
                SQLExpression indexVal = exprFactory.newLiteral(sqlStmt, orderMapping, startIdx);
                sqlStmt.whereAnd(indexExpr.eq(indexVal), true);
            }
            else
            {
                // Add restrictions on start/end indices as required
                if (startIdx >= 0)
                {
                    SQLExpression indexExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
                    SQLExpression indexVal = exprFactory.newLiteral(sqlStmt, orderMapping, startIdx);
                    sqlStmt.whereAnd(indexExpr.ge(indexVal), true);
                }
                else
                {
                    // Just restrict to >= 0 so we don't get any disassociated elements
                    SQLExpression indexExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
                    SQLExpression indexVal = exprFactory.newLiteral(sqlStmt, orderMapping, 0);
                    sqlStmt.whereAnd(indexExpr.ge(indexVal), true);
                }

                if (endIdx >= 0)
                {
                    SQLExpression indexExpr2 = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
                    SQLExpression indexVal2 = exprFactory.newLiteral(sqlStmt, orderMapping, endIdx);
                    sqlStmt.whereAnd(indexExpr2.lt(indexVal2), true);
                }
            }

            if (needsOrdering)
            {
                // Order by the ordering column, when present
                SQLTable orderSqlTbl =
                    SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
                SQLExpression[] orderExprs = new SQLExpression[orderMapping.getNumberOfDatastoreMappings()];
                boolean descendingOrder[] = new boolean[orderMapping.getNumberOfDatastoreMappings()];
                orderExprs[0] = exprFactory.newExpression(sqlStmt, orderSqlTbl, orderMapping);
                sqlStmt.setOrdering(orderExprs, descendingOrder);
            }
        }
        else
        {
            // Apply ordering defined by <order-by>
            if (elementInfo != null && elementInfo.length > 0)
            {
                DatastoreClass elementTbl = elementInfo[0].getDatastoreClass();
                FieldOrder[] orderComponents = ownerMemberMetaData.getOrderMetaData().getFieldOrders();
                SQLExpression[] orderExprs = new SQLExpression[orderComponents.length];
                boolean[] orderDirs = new boolean[orderComponents.length];

                for (int i=0;i<orderComponents.length;i++)
                {
                    String fieldName = orderComponents[i].getFieldName();
                    JavaTypeMapping fieldMapping = elementTbl.getMemberMapping(elementInfo[0].getAbstractClassMetaData().getMetaDataForMember(fieldName));
                    orderDirs[i] = !orderComponents[i].isForward();
                    SQLTable fieldSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), fieldMapping);
                    orderExprs[i] = exprFactory.newExpression(sqlStmt, fieldSqlTbl, fieldMapping);
                }

                sqlStmt.setOrdering(orderExprs, orderDirs);
            }
        }

        return new IteratorStatement(this, sqlStmt, stmtClassMapping);
    }
}