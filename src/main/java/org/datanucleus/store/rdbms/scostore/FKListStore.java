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
import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.Transaction;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.OrderMetaData.FieldOrder;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SelectStatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.scostore.ListStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * RDBMS-specific implementation of an {@link ListStore} using foreign keys.
 */
public class FKListStore<E> extends AbstractListStore<E>
{
    /** Field number of owner link in element class. */
    private final int ownerFieldNumber;

    /** Statement for updating a foreign key in a 1-N unidirectional */
    private String updateFkStmt;

    private String clearNullifyStmt;

    private String removeAtNullifyStmt;

    private String setStmt;
    private String unsetStmt;

    /**
     * @param mmd Metadata for owning field/property
     * @param storeMgr Manager for the datastore
     * @param clr ClassLoader resolver
     */
    public FKListStore(AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);

        setOwner(mmd);
        CollectionMetaData colmd = mmd.getCollection();
        if (colmd == null)
        {
            throw new NucleusUserException(Localiser.msg("056001", mmd.getFullFieldName()));
        }

        // Load the element class
        elementType = colmd.getElementType();
        Class element_class = clr.classForName(elementType);
        if (ClassUtils.isReferenceType(element_class))
        {
            elementIsPersistentInterface = storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterface(element_class.getName());
            if (elementIsPersistentInterface)
            {
                emd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForInterface(element_class,clr);
            }
            else
            {
                // Take the metadata for the first implementation of the reference type
                emd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForImplementationOfReference(element_class,null,clr);
            }
        }
        else
        {
            // Check that the element class has MetaData
            emd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(element_class, clr);
        }
        if (emd == null)
        {
            throw new NucleusUserException(Localiser.msg("056003", element_class.getName(), mmd.getFullFieldName()));
        }

        elementInfo = getComponentInformationForClass(elementType, emd);
        if (elementInfo == null || elementInfo.length == 0)
        {
            throw new NucleusUserException(Localiser.msg("056075", ownerMemberMetaData.getFullFieldName(), elementType));
        }
        else if (elementInfo.length == 1 && ClassUtils.isReferenceType(element_class))
        {
            // Special case : reference element type (interface/object) and single "implementation"
            elementType = emd.getFullClassName();
        }

        elementMapping = elementInfo[0].getDatastoreClass().getIdMapping(); // Just use the first element type as the guide for the element mapping
        elementsAreEmbedded = false; // Can't embed element when using FK relation
        elementsAreSerialised = false; // Can't serialise element when using FK relation

        // Get the field in the element table (if any)
        String mappedByFieldName = mmd.getMappedBy();
        if (mappedByFieldName != null)
        {
            // 1-N FK bidirectional - the element class has a field for the owner.
            AbstractMemberMetaData eofmd = elementInfo[0].getAbstractClassMetaData().getMetaDataForMember(mappedByFieldName);
            if (eofmd == null)
            {
                // Field for owner not found in element
                throw new NucleusUserException(Localiser.msg("056024", mmd.getFullFieldName(), mappedByFieldName, element_class.getName()));
            }
            if (!clr.isAssignableFrom(eofmd.getType(), mmd.getAbstractClassMetaData().getFullClassName()))
            {
                // Type of the element "mapped-by" field is not consistent with the owner type
                throw new NucleusUserException(Localiser.msg("056025", mmd.getFullFieldName(), 
                    eofmd.getFullFieldName(), eofmd.getTypeName(), mmd.getAbstractClassMetaData().getFullClassName()));
            }

            String ownerFieldName = eofmd.getName();
            ownerFieldNumber = elementInfo[0].getAbstractClassMetaData().getAbsolutePositionOfMember(ownerFieldName);
            ownerMapping = elementInfo[0].getDatastoreClass().getMemberMapping(eofmd);
            if (ownerMapping == null)
            {
                throw new NucleusUserException(Localiser.msg("056029", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), elementType, ownerFieldName));
            }
            if (isEmbeddedMapping(ownerMapping))
            {
                throw new NucleusUserException(Localiser.msg("056026", ownerFieldName, elementType, eofmd.getTypeName(), mmd.getClassName()));
            }
        }
        else
        {
            // 1-N FK unidirectional
            // The element class knows nothing about the owner (but its table has external mappings)
            ownerFieldNumber = -1;
            ownerMapping = elementInfo[0].getDatastoreClass().getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            // TODO Allow for the situation where the user specified "table" in the elementMetaData to put the FK in a supertable. This only checks against default element table
            if (ownerMapping == null)
            {
                throw new NucleusUserException(Localiser.msg("056030", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), elementType));
            }
        }

        orderMapping = elementInfo[0].getDatastoreClass().getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
        // TODO If we have List<interface> we need to find the index by mappedBy name
        if (mmd.getOrderMetaData() != null && !mmd.getOrderMetaData().isIndexedList())
        {
            indexedList = false;
        }
        if (orderMapping == null && indexedList)
        {
            // "Indexed List" but no order mapping present!
            throw new NucleusUserException(Localiser.msg("056041", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), elementType));
        }

        relationDiscriminatorMapping = elementInfo[0].getDatastoreClass().getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM);
        if (relationDiscriminatorMapping != null)
        {
            relationDiscriminatorValue = mmd.getValueForExtension("relation-discriminator-value");
            if (relationDiscriminatorValue == null)
            {
                // No value defined so just use the field name
                relationDiscriminatorValue = mmd.getFullFieldName();
            }
        }

        // TODO Cater for multiple element tables
        containerTable = elementInfo[0].getDatastoreClass();
        if (mmd.getMappedBy() != null && ownerMapping.getTable() != containerTable)
        {
            // Element and owner don't have consistent tables so use the one with the mapping
            // e.g collection is of subclass, yet superclass has the link back to the owner
            containerTable = ownerMapping.getTable();
        }
    }

    /**
     * Method to set an object in the List at a position.
     * @param op ObjectProvider for the owner
     * @param index The item index
     * @param element What to set it to.
     * @param allowDependentField Whether to enable dependent-field deletes during the set
     * @return The value before setting.
     */
    public E set(ObjectProvider op, int index, Object element, boolean allowDependentField)
    {
        validateElementForWriting(op, element, -1); // Last argument means don't set the position on any INSERT
        E oldElement = get(op, index);
//        return set(op, index, element, allowDependentField, oldElement);

        try
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Unset the existing object from this position
                String unsetStmt = getUnsetStmt();
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, unsetStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    if (orderMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, index, jdbcPosition, orderMapping);
                    }
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    sqlControl.executeStatementUpdate(ec, mconn, unsetStmt, ps, true);
                }
                finally
                {
                    sqlControl.closeStatement(mconn, ps);
                }

                // Set the new object at this position
                String setStmt = getSetStmt(element);
                PreparedStatement ps2 = sqlControl.getStatementForUpdate(mconn, setStmt, false);
                try
                {
                    ComponentInfo elemInfo = getComponentInfoForElement(element);
                    JavaTypeMapping elemMapping = this.elementMapping;
                    JavaTypeMapping orderMapping = this.orderMapping;
                    if (elemInfo != null)
                    {
                        elemMapping = elemInfo.getDatastoreClass().getIdMapping();
                        orderMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
                    }

                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps2, jdbcPosition, this);
                    if (orderMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps2, index, jdbcPosition, orderMapping);
                    }
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps2, jdbcPosition, this);
                    }
                    jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps2, element, jdbcPosition, elemMapping);

                    sqlControl.executeStatementUpdate(ec, mconn, setStmt, ps2, true);
                }
                finally
                {
                    ps2.close();
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
        boolean dependent = getOwnerMemberMetaData().getCollection().isDependentElement();
        if (getOwnerMemberMetaData().isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent && allowDependentField)
        {
            if (oldElement != null)
            {
                // Delete the element if it is dependent and doesnt have a duplicate entry in the list
                op.getExecutionContext().deleteObjectInternal(oldElement);
            }
        }

        return oldElement;
    }

    /**
     * Utility to update a foreign-key in the element in the case of a unidirectional 1-N relationship.
     * @param op ObjectProvider for the owner
     * @param element The element to update
     * @param owner The owner object to set in the FK
     * @param index The index position (or -1 if not known)
     * @return Whether it was performed successfully
     */
    private boolean updateElementFk(ObjectProvider op, Object element, Object owner, int index)
    {
        if (element == null)
        {
            return false;
        }

        ExecutionContext ec = op.getExecutionContext();
        String updateFkStmt = getUpdateFkStmt(element);
        boolean retval;
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, updateFkStmt, false);
                try
                {
                    ComponentInfo elemInfo = getComponentInfoForElement(element);
                    JavaTypeMapping ownerMapping = null;
                    if (ownerMemberMetaData.getMappedBy() != null)
                    {
                        ownerMapping = elemInfo.getDatastoreClass().getMemberMapping(ownerMemberMetaData.getMappedBy());
                    }
                    else
                    {
                        ownerMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                    }
                    JavaTypeMapping elemMapping = elemInfo.getDatastoreClass().getIdMapping();
                    JavaTypeMapping orderMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);

                    int jdbcPosition = 1;
                    if (owner == null)
                    {
                        if (ownerMemberMetaData != null)
                        {
                            ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, ownerMapping), null, op, ownerMemberMetaData.getAbsoluteFieldNumber());
                        }
                        else
                        {
                            ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, ownerMapping), null);
                        }
                        jdbcPosition += ownerMapping.getNumberOfDatastoreMappings();
                    }
                    else
                    {
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    }
                    if (orderMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, index, jdbcPosition, orderMapping);
                    }
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }
                    jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, jdbcPosition, elemMapping);

                    sqlControl.executeStatementUpdate(ec, mconn, updateFkStmt, ps, true);
                    retval = true;
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
            throw new NucleusDataStoreException(Localiser.msg("056027", updateFkStmt), e);
        }

        return retval;
    }

    /**
     * Method to update the collection to be the supplied collection of elements.
     * @param op ObjectProvider for the owner
     * @param coll The collection to use
     */
    public void update(ObjectProvider op, Collection coll)
    {
        if (coll == null || coll.isEmpty())
        {
            clear(op);
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
     * Internal method for adding an item to the List.
     * @param op ObjectProvider for the owner
     * @param startAt The start position
     * @param atEnd Whether to add at the end
     * @param c The Collection of elements to add.
     * @param size Current size of list (if known). -1 if not known
     * @return Whether it was successful
     */
    protected boolean internalAdd(ObjectProvider op, int startAt, boolean atEnd, Collection<E> c, int size)
    {
        if (c == null || c.size() == 0)
        {
            return true;
        }

        // Check what we have persistent already
        int currentListSize = (size < 0 ? size(op) : size);

        boolean shiftingElements = true;
        if (atEnd || startAt == currentListSize)
        {
            shiftingElements = false;
            startAt = currentListSize; // Not shifting so we insert from the end
        }

        boolean elementsNeedPositioning = false;
        int position = startAt;
        Iterator elementIter = c.iterator();
        while (elementIter.hasNext())
        {
            // Persist any non-persistent objects optionally at their final list position (persistence-by-reachability)
            if (shiftingElements)
            {
                // We have to shift things so dont bother with positioning
                position = -1;
            }

            boolean inserted = validateElementForWriting(op, elementIter.next(), position);
            if (!inserted || shiftingElements)
            {
                // This element wasnt positioned in the validate so we need to set the positions later
                elementsNeedPositioning = true;
            }
            if (!shiftingElements)
            {
                position++;
            }
        }

        if (shiftingElements)
        {
            // We need to shift existing elements before positioning the new ones
            // TODO Consider doing all of this in smaller number of statements
            // e.g if insert at start then do "UPDATE IDX=IDX+1 WHERE ID={...}" or similar, where the ID values are
            // all ids after the position we insert at
            try
            {
                // Calculate the amount we need to shift any existing elements by
                // This is used where inserting between existing elements and have to shift down all elements after the start point
                int shift = c.size();

                ExecutionContext ec = op.getExecutionContext();
                ManagedConnection mconn = storeMgr.getConnection(ec);
                try
                {
                    // shift up existing elements after start position by "shift"
                    for (int i=currentListSize-1; i>=startAt; i--)
                    {
                        // Shift the index of this row by "shift"
                        internalShift(op, mconn, true, i, shift, false);
                    }
                }
                finally
                {
                    mconn.release();
                }
            }
            catch (MappedDatastoreException e)
            {
                // An error was encountered during the shift process so abort here
                throw new NucleusDataStoreException(Localiser.msg("056009", e.getMessage()), e.getCause());
            }
        }

        if (shiftingElements || elementsNeedPositioning)
        {
            // Some elements have been shifted so the new elements need positioning now, or we already had some
            // of the new elements persistent and so they need their positions setting now
            elementIter = c.iterator();
            while (elementIter.hasNext())
            {
                Object element = elementIter.next();
                updateElementFk(op, element, op.getObject(), startAt);
                startAt++;
            }
        }

        return true;
    }

    /**
     * Remove all elements from a collection from the association owner vs elements.
     * TODO : Change the query to do all in one go for efficiency. Currently
     * removes an element and shuffles the indexes, then removes an element
     * and shuffles the indexes, then removes an element and shuffles the
     * indexes etc ... a bit inefficient !!!
     * @param op ObjectProvider for the owner
     * @param elements Collection of elements to remove 
     * @return Whether the database was updated 
     */
    public boolean removeAll(ObjectProvider op, Collection elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        boolean modified = false;
        if (indexedList)
        {
            // Get the indices of the elements to remove in reverse order (highest first)
            int[] indices = getIndicesOf(op,elements);
            if (indices == null)
            {
                return false;
            }

            // Remove each element in turn, doing the shifting of indexes each time
            // TODO : Change this to remove all in one go and then shift once
            for (int i=0;i<indices.length;i++)
            {
                internalRemoveAt(op, indices[i], -1);
                modified = true;
            }

            // Dependent-element
            boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
            if (ownerMemberMetaData.isCascadeRemoveOrphans())
            {
                dependent = true;
            }
            if (dependent)
            {
                // "delete-dependent" : delete elements if the collection is marked as dependent
                // TODO What if the collection contains elements that are not in the List ? should not delete them
                op.getExecutionContext().deleteObjects(elements.toArray());
            }
        }
        else
        {
            // Ordered List
            Iterator iter = elements.iterator();
            while (iter.hasNext())
            {
                Object element = iter.next();
                remove(op, element, size, true);
            }
        }

        return modified;
    }

    /**
     * Convenience method to remove the specified element from the List.
     * @param op ObjectProvider for the owner
     * @param element The element
     * @return Whether the List was modified
     */
    protected boolean internalRemove(ObjectProvider op, Object element, int size)
    {
        if (indexedList)
        {
            // Indexed List
            // The element can be at one position only (no duplicates allowed in FK list)
            int index = indexOf(op, element);
            if (index == -1)
            {
                return false;
            }
            internalRemoveAt(op, index, size);
        }
        else
        {
            // Ordered List - no index so null the FK (if nullable) or delete the element
            if (ownerMapping.isNullable())
            {
                // Nullify the FK
                ExecutionContext ec = op.getExecutionContext();
                ObjectProvider elementSM = ec.findObjectProvider(element);
                if (relationType == RelationType.ONE_TO_MANY_BI)
                {
                    // Set field in element to null (so it nulls the FK)
                    // TODO This is ManagedRelations - move into RelationshipManager
                    elementSM.replaceFieldMakeDirty(ownerMemberMetaData.getRelatedMemberMetaData(clr)[0].getAbsoluteFieldNumber(), null);
                    if (op.getExecutionContext().isFlushing())
                    {
                        elementSM.flush();
                    }
                }
                else
                {
                    // Null the (external) FK in the element
                    updateElementFk(op, element, null, -1);
                }
            }
            else
            {
                // Delete the element
                op.getExecutionContext().deleteObjectInternal(element);
            }
        }

        return true;
    }

    /**
     * Convenience method to manage the removal of an element from the collection, performing
     * any necessary "managed relationship" updates when the field is bidirectional.
     * @param ownerOP ObjectProvider for the collection owner
     * @param element The element
     */
    protected void manageRemovalOfElement(ObjectProvider ownerOP, Object element)
    {
        // TODO Complete this
        /*ExecutionContext om = ownerSM.getExecutionContext();
        if (relationType == Relation.ONE_TO_MANY_BI && om.getManageRelations())
        {
            // Managed Relations : 1-N bidirectional so null the owner on the elements
            if (!om.getApiAdapter().isDeleted(element))
            {
                ObjectProvider elementSM = om.findObjectProvider(element);
                if (elementSM != null)
                {
                    // Null the owner of the element
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(
                            Localiser.msg("055010", ownerSM.toPrintableID(), ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(element)));
                    }

                    elementSM.replaceField(getFieldNumberInElementForBidirectional(elementSM), null, true);
                    if (om.isFlushing())
                    {
                        // Make sure this change gets flushed
                        elementSM.flush();
                    }
                }
            }
        }*/
    }

    /**
     * Internal method to remove an object at a location in the List.
     * Differs from the JoinTable List in that it nulls out the owner FK.
     * @param op ObjectProvider for the owner
     * @param index The location
     * @param size Current size of list (if known). -1 if not known
     */
    protected void internalRemoveAt(ObjectProvider op, int index, int size)
    {
        if (!indexedList)
        {
            throw new NucleusUserException("Cannot remove an element from a particular position with an ordered list since no indexes exist");
        }

        boolean nullify = false;
        if (ownerMapping.isNullable() && orderMapping != null && orderMapping.isNullable())
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("056043"));
            nullify = true;
        }
        else
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("056042"));
        }

        String stmt;
        if (nullify)
        {
            // TODO When using this statement we need to plug in the element table name, and do it for
            // all possible element tables
            stmt = getRemoveAtNullifyStmt();
        }
        else
        {
            // TODO Really ought to make this delete via ExecutionContext
            stmt = getRemoveAtStmt();
        }
        internalRemoveAt(op, index, stmt, size);
    }

    /**
     * Method to clear the List.
     * This is called by the List.clear() method, or when the container object is being deleted
     * and the elements are to be removed (maybe for dependent field), or also when updating a Collection
     * and removing all existing prior to adding all new.
     * @param op ObjectProvider for the owner
     */
    public void clear(ObjectProvider op)
    {
        boolean deleteElements = false;
        ExecutionContext ec = op.getExecutionContext();
        boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent)
        {
            // Elements are dependent and can't exist on their own, so delete them all
            NucleusLogger.DATASTORE.debug(Localiser.msg("056034"));
            deleteElements = true;
        }
        else
        {
            if (ownerMapping.isNullable() && orderMapping == null)
            {
                // Field is not dependent, and nullable so we null the FK
                NucleusLogger.DATASTORE.debug(Localiser.msg("056036"));
                deleteElements = false;
            }
            else if (ownerMapping.isNullable() && orderMapping != null && orderMapping.isNullable())
            {
                // Field is not dependent, and nullable so we null the FK
                NucleusLogger.DATASTORE.debug(Localiser.msg("056036"));
                deleteElements = false;
            }
            else
            {
                // Field is not dependent, and not nullable so we just delete the elements
                NucleusLogger.DATASTORE.debug(Localiser.msg("056035"));
                deleteElements = true;
            }
        }

        if (deleteElements)
        {
            // Find elements present in the datastore and delete them one-by-one
            Iterator elementsIter = iterator(op);
            if (elementsIter != null)
            {
                while (elementsIter.hasNext())
                {
                    Object element = elementsIter.next();
                    if (ec.getApiAdapter().isPersistable(element) && ec.getApiAdapter().isDeleted(element))
                    {
                        // Element is waiting to be deleted so flush it (it has the FK)
                        ObjectProvider objSM = ec.findObjectProvider(element);
                        objSM.flush();
                    }
                    else
                    {
                        // Element not yet marked for deletion so go through the normal process
                        ec.deleteObjectInternal(element);
                    }
                }
            }
        }
        else
        {
            // Clear without delete
            // TODO If the relation is bidirectional we need to clear the owner in the element
            String clearNullifyStmt = getClearNullifyStmt();
            try
            {
                ManagedConnection mconn = storeMgr.getConnection(ec);
                SQLController sqlControl = storeMgr.getSQLController();
                try
                {
                    PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, clearNullifyStmt, false);
                    try
                    {
                        int jdbcPosition = 1;
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                        if (relationDiscriminatorMapping != null)
                        {
                            BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                        }
                        sqlControl.executeStatementUpdate(ec, mconn, clearNullifyStmt, ps, true);
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
                throw new NucleusDataStoreException(Localiser.msg("056013", clearNullifyStmt), e);
            }
        }
    }

    /**
     * Method to validate that an element is valid for writing to the datastore.
     * TODO Minimise differences to super.validateElementForWriting()
     * @param op ObjectProvider for the List
     * @param element The element to validate
     * @param index The position that the element is being stored at in the list
     * @return Whether the element was inserted
     */
    protected boolean validateElementForWriting(final ObjectProvider op, final Object element, final int index)
    {
        final Object newOwner = op.getObject();

        ComponentInfo info = getComponentInfoForElement(element);

        final DatastoreClass elementTable;
        if (storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterface(elementType))
        {
            elementTable = storeMgr.getDatastoreClass(storeMgr.getNucleusContext().getMetaDataManager().getImplementationNameForPersistentInterface(elementType), clr);
        }
        else
        {
            elementTable = storeMgr.getDatastoreClass(element.getClass().getName(), clr);
        }
        final JavaTypeMapping orderMapping;
        if (info != null)
        {
            orderMapping = info.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
        }
        else
        {
            orderMapping = this.orderMapping;
        }

        // Check if element is ok for use in the datastore, specifying any external mappings that may be required
        boolean inserted = super.validateElementForWriting(op.getExecutionContext(), element, new FieldValues()
        {
            public void fetchFields(ObjectProvider elemOP)
            {
                // Find the (element) table storing the FK back to the owner
                if (elementTable != null)
                {
                    JavaTypeMapping externalFKMapping = elementTable.getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                    if (externalFKMapping != null)
                    {
                        // The element has an external FK mapping so set the value it needs to use in the INSERT
                        elemOP.setAssociatedValue(externalFKMapping, op.getObject());
                    }
                    if (relationDiscriminatorMapping != null)
                    {
                        elemOP.setAssociatedValue(relationDiscriminatorMapping, relationDiscriminatorValue);
                    }
                    if (orderMapping != null && index >= 0)
                    {
                        if (ownerMemberMetaData.getOrderMetaData() != null && ownerMemberMetaData.getOrderMetaData().getMappedBy() != null)
                        {
                            // Order is stored in a field in the element so update it
                            // We support mapped-by fields of types int/long/Integer/Long currently
                            Object indexValue = null;
                            if (orderMapping.getMemberMetaData().getTypeName().equals(ClassNameConstants.JAVA_LANG_LONG) ||
                                orderMapping.getMemberMetaData().getTypeName().equals(ClassNameConstants.LONG))
                            {
                                indexValue = Long.valueOf(index);
                            }
                            else
                            {
                                indexValue = Integer.valueOf(index);
                            }
                            elemOP.replaceFieldMakeDirty(orderMapping.getMemberMetaData().getAbsoluteFieldNumber(), indexValue);
                        }
                        else
                        {
                            // Order is stored in a surrogate column so save its vaue for the element to use later
                            elemOP.setAssociatedValue(orderMapping, Integer.valueOf(index));
                        }
                    }
                }

                if (ownerFieldNumber >= 0)
                {
                    // TODO This is ManagedRelations - move into RelationshipManager
                    // Managed Relations : 1-N bidir, so make sure owner is correct at persist
                    Object currentOwner = elemOP.provideField(ownerFieldNumber);
                    if (currentOwner == null)
                    {
                        // No owner, so correct it
                        NucleusLogger.PERSISTENCE.info(Localiser.msg("056037", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                            StringUtils.toJVMIDString(elemOP.getObject())));
                        elemOP.replaceFieldMakeDirty(ownerFieldNumber, newOwner);
                    }
                    else if (currentOwner != newOwner && op.getReferencedPC() == null)
                    {
                        // Owner of the element is neither this container nor is it being attached
                        // Inconsistent owner, so throw exception
                        throw new NucleusUserException(Localiser.msg("056038", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                            StringUtils.toJVMIDString(elemOP.getObject()), StringUtils.toJVMIDString(currentOwner)));
                    }
                }
            }
            public void fetchNonLoadedFields(ObjectProvider op)
            {
            }
            public FetchPlan getFetchPlanForLoading()
            {
                return null;
            }
        });

        return inserted;
    }

    /**
     * Accessor for an iterator through the list elements.
     * @param op ObjectProvider for the container.
     * @param startIdx The start index in the list (only for indexed lists)
     * @param endIdx The end index in the list (only for indexed lists)
     * @return The List Iterator
     */
    protected ListIterator<E> listIterator(ObjectProvider op, int startIdx, int endIdx)
    {
        ExecutionContext ec = op.getExecutionContext();
        Transaction tx = ec.getTransaction();

        if (elementInfo == null || elementInfo.length == 0)
        {
            return null;
        }

        // Generate the statement. Note that this is not cached since depends on the current FetchPlan and other things
        IteratorStatement iterStmt = getIteratorStatement(op.getExecutionContext().getClassLoaderResolver(), ec.getFetchPlan(), true, startIdx, endIdx);
        SelectStatement sqlStmt = iterStmt.getSelectStatement();
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
                for (int k=0;k<ownerMapping.getNumberOfDatastoreMappings();k++)
                {
                    paramPositions[k] = inputParamNum++;
                }
                ownerIdx.addParameterOccurrence(paramPositions);
            }
        }
        else
        {
            int[] paramPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<ownerMapping.getNumberOfDatastoreMappings();k++)
            {
                paramPositions[k] = inputParamNum++;
            }
            ownerIdx.addParameterOccurrence(paramPositions);
        }

        if (tx.getSerializeRead() != null && tx.getSerializeRead())
        {
            sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
        }
        String stmt = sqlStmt.getSQLText().toSQL();

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
                    ownerIdx.getMapping().setObject(ec, ps, ownerIdx.getParameterPositionsForOccurrence(paramInstance), op.getObject());
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        ResultObjectFactory rof = null;
                        if (elementsAreEmbedded || elementsAreSerialised)
                        {
                            throw new NucleusException("Cannot have FK set with non-persistent objects");
                        }

                        rof = new PersistentClassROF(storeMgr, emd, resultMapping, false, null, clr.classForName(elementType));
                        return new ListStoreIterator(op, rs, rof, this);
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
     * Generate statement for updating the owner, index columns in an inverse 1-N. 
     * Will result in the statement
     * <PRE>
     * UPDATE ELEMENTTABLE SET FK_COL_1 = ?, FK_COL_2 = ?, FK_IDX = ? [,DISTINGUISHER=?]
     * WHERE ELEMENT_ID = ?
     * </PRE>
     * when we have a single element table, and
     * <PRE>
     * UPDATE ? SET FK_COL_1=?, FK_COL_2=?, FK_IDX=? [,DISTINGUISHER=?]
     * WHERE ELEMENT_ID=?
     * </PRE>
     * when we have multiple element tables possible
     * @return Statement for updating the owner/index of an element in an inverse 1-N
     */
    private String getUpdateFkStmt(Object element)
    {
        if (elementMapping instanceof ReferenceMapping && elementMapping.getNumberOfDatastoreMappings() > 1)
        {
            // Don't cache since depends on the element
            return getUpdateFkStatementString(element);
        }

        if (updateFkStmt == null)
        {
            synchronized (this)
            {
                updateFkStmt = getUpdateFkStatementString(element);
            }
        }
        return updateFkStmt;
    }

    private String getUpdateFkStatementString(Object element)
    {
        JavaTypeMapping ownerMapping = this.ownerMapping;
        JavaTypeMapping elemMapping = this.elementMapping;
        JavaTypeMapping orderMapping = this.orderMapping;
        JavaTypeMapping relDiscrimMapping = this.relationDiscriminatorMapping;
        Table table = containerTable;
        if (elementInfo.length > 1)
        {
            ComponentInfo elemInfo = getComponentInfoForElement(element);
            if (elemInfo != null)
            {
                table = elemInfo.getDatastoreClass();
                if (ownerMemberMetaData.getMappedBy() != null)
                {
                    ownerMapping = elemInfo.getDatastoreClass().getMemberMapping(ownerMemberMetaData.getMappedBy());
                }
                else
                {
                    ownerMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                }
                elemMapping = elemInfo.getDatastoreClass().getIdMapping();
                orderMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
                relDiscrimMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM);
            }
        }

        StringBuilder stmt = new StringBuilder("UPDATE ").append(table.toString()).append(" SET ");
        for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            stmt.append("=");
            stmt.append(((AbstractDatastoreMapping) ownerMapping.getDatastoreMapping(i)).getUpdateInputParameter());
        }
        if (orderMapping != null)
        {
            for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                stmt.append("=");
                stmt.append(((AbstractDatastoreMapping) orderMapping.getDatastoreMapping(i)).getUpdateInputParameter());
            }
        }
        if (relDiscrimMapping != null)
        {
            for (int i = 0; i < relDiscrimMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(relDiscrimMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                stmt.append("=");
                stmt.append(((AbstractDatastoreMapping) relDiscrimMapping.getDatastoreMapping(i)).getUpdateInputParameter());
            }
        }

        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForElement(stmt, elemMapping, element, elementsAreSerialised, null, true);

        return stmt.toString();
    }

    /**
     * Generates the statement for clearing items by nulling the owner link out. The statement will be
     * <PRE>
     * UPDATE LISTTABLE SET OWNERCOL=NULL, INDEXCOL=-1 [,DISTINGUISHER=NULL]
     * WHERE OWNERCOL=? [AND DISTINGUISHER=?]
     * </PRE>
     * when there is only one element table, and will be
     * <PRE>
     * UPDATE ? SET OWNERCOL=NULL, INDEXCOL=-1 [,DISTINGUISHER=NULL]
     * WHERE OWNERCOL=? [AND DISTINGUISHER=?]
     * </PRE>
     * when there is more than 1 element table.
     * @return The Statement for clearing items for the owner.
     */
    private String getClearNullifyStmt()
    {
        if (clearNullifyStmt == null)
        {
            synchronized (this)
            {
                // TODO If ownerMapping is not for containerTable then use owner table for the UPDATE
                StringBuilder stmt = new StringBuilder("UPDATE ");
                if (elementInfo.length > 1)
                {
                    stmt.append("?");
                }
                else
                {
                    // Could use elementInfo[0].getDatastoreClass but need to allow for relation in superclass table
                    stmt.append(containerTable.toString());
                }
                stmt.append(" SET ");
                for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(", ");
                    }
                    stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString() + "=NULL");
                }
                if (orderMapping != null)
                {
                    for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(", ");
                        stmt.append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString() + "=-1");
                    }
                }
                if (relationDiscriminatorMapping != null)
                {
                    for (int i = 0; i < relationDiscriminatorMapping.getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(", ");
                        stmt.append(relationDiscriminatorMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                        stmt.append("=NULL");
                    }
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }

                clearNullifyStmt = stmt.toString();
            }
        }
        return clearNullifyStmt;
    }

    /**
     * Generates the statement for setting an item to be at a position.
     * <PRE>
     * UPDATE LISTTABLE SET OWNERCOL=?, INDEXCOL = ? [,DISTINGUISHER=?]
     * WHERE ELEMENTCOL = ?
     * </PRE>
     * @param element The element to set
     * @return The Statement for setting an item
     */
    private String getSetStmt(Object element)
    {
        if (setStmt != null)
        {
            return setStmt;
        }

        String stmt = getSetStatementString(element);
        if (elementInfo.length == 1)
        {
            setStmt = stmt;
        }
        return stmt;
    }

    private String getSetStatementString(Object element)
    {
        ComponentInfo elemInfo = getComponentInfoForElement(element);
        Table table = this.containerTable;
        JavaTypeMapping ownerMapping = this.ownerMapping;
        JavaTypeMapping elemMapping = this.elementMapping;
        JavaTypeMapping relDiscrimMapping = this.relationDiscriminatorMapping;
        JavaTypeMapping orderMapping = this.orderMapping;
        if (elemInfo != null)
        {
            table = elemInfo.getDatastoreClass();
            elemMapping = elemInfo.getDatastoreClass().getIdMapping();
            if (ownerMemberMetaData.getMappedBy() != null)
            {
                ownerMapping = table.getMemberMapping(elemInfo.getAbstractClassMetaData().getMetaDataForMember(ownerMemberMetaData.getMappedBy()));
            }
            else
            {
                ownerMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }
            orderMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
            relDiscrimMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM);
        }

        // TODO If ownerMapping is not for containerTable then use owner table for the UPDATE
        StringBuilder stmt = new StringBuilder("UPDATE ").append(table.toString()).append(" SET ");
        for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            stmt.append(" = ");
            stmt.append(((AbstractDatastoreMapping) ownerMapping.getDatastoreMapping(i)).getUpdateInputParameter());
        }

        if (orderMapping != null)
        {
            for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                stmt.append(" = ");
                stmt.append(((AbstractDatastoreMapping) orderMapping.getDatastoreMapping(i)).getUpdateInputParameter());
            }
        }
        if (relDiscrimMapping != null)
        {
            for (int i = 0; i < relDiscrimMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(relDiscrimMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                stmt.append(" = ");
                stmt.append(((AbstractDatastoreMapping) relDiscrimMapping.getDatastoreMapping(i)).getUpdateInputParameter());
            }
        }

        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForElement(stmt, elemMapping, element, isElementsAreSerialised(), null, true);

        return stmt.toString();
    }

    /**
     * Generates the statement for unsetting an item from a list position.
     * <PRE>
     * UPDATE LISTTABLE SET OWNERCOL=NULL, INDEXCOL=-1 [, DISTINGUISHER = NULL]
     * WHERE OWNERCOL = ? AND INDEXCOL = ? [AND DISTINGUISHER = ?]
     * </PRE>
     * @return The Statement for unsetting an item
     */
    private String getUnsetStmt()
    {
        if (unsetStmt == null)
        {
            synchronized (this)
            {
                // TODO If ownerMapping is not for containerTable then use owner table for the UPDATE
                StringBuilder stmt = new StringBuilder("UPDATE ");
                // TODO Allow for multiple element tables
                stmt.append(containerTable.toString());
                stmt.append(" SET ");
                for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(",");
                    }
                    stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    stmt.append("=NULL");
                }

                if (orderMapping != null)
                {
                    for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(",");
                        stmt.append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                        stmt.append("=-1");
                    }
                }
                if (relationDiscriminatorMapping != null)
                {
                    for (int i = 0; i < relationDiscriminatorMapping.getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(",");
                        stmt.append(relationDiscriminatorMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                        stmt.append(" = NULL");
                    }
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                BackingStoreHelper.appendWhereClauseForMapping(stmt, orderMapping, null, false);
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }

                unsetStmt = stmt.toString();
            }
        }
        return unsetStmt;
    }

    /**
     * Generates the statement for removing an item by nulling it out.
     * When there is only a single element table the statement will be
     * <PRE>
     * UPDATE LISTTABLE SET OWNERCOL=NULL, INDEXCOL=-1
     * WHERE OWNERCOL = ?
     * AND INDEXCOL = ?
     * [AND DISTINGUISHER = ?]
     * </PRE>
     * and when there are multiple element tables the statement will be
     * <PRE>
     * UPDATE ? SET OWNERCOL=NULL, INDEXCOL=-1
     * WHERE OWNERCOL=?
     * AND INDEXCOL=?
     * [AND DISTINGUISHER = ?]
     * </PRE>
     * @return The Statement for removing an item from a position
     */
    private String getRemoveAtNullifyStmt()
    {
        if (removeAtNullifyStmt == null)
        {
            synchronized (this)
            {
                // TODO If ownerMapping is not for containerTable then use owner table for the UPDATE
                StringBuilder stmt = new StringBuilder("UPDATE ");
                if (elementInfo.length > 1)
                {
                    stmt.append("?");
                }
                else
                {
                    // Could use elementInfo[0].getDatastoreClass but need to allow for relation in superclass table
                    stmt.append(containerTable.toString());
                }
                stmt.append(" SET ");
                for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(", ");
                    }
                    stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    stmt.append("=NULL");
                }
                if (orderMapping != null)
                {
                    for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(", ");
                        stmt.append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                        stmt.append("=-1");
                    }
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                BackingStoreHelper.appendWhereClauseForMapping(stmt, orderMapping, null, false);
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }

                removeAtNullifyStmt = stmt.toString();
            }
        }
        return removeAtNullifyStmt;
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
        SelectStatement sqlStmt = null;
        StatementClassMapping stmtClassMapping = new StatementClassMapping();
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();

        if (elementInfo.length == 1 &&
            elementInfo[0].getDatastoreClass().getDiscriminatorMetaData() != null &&
            elementInfo[0].getDatastoreClass().getDiscriminatorMetaData().getStrategy() != DiscriminatorStrategy.NONE)
        {
            String elementType = ownerMemberMetaData.getCollection().getElementType();
            if (ClassUtils.isReferenceType(clr.classForName(elementType)))
            {
                String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(elementType, clr);
                Class[] cls = new Class[clsNames.length];
                for (int i=0; i<clsNames.length; i++)
                {
                    cls[i] = clr.classForName(clsNames[i]);
                }
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null).getStatement();
            }
            else
            {
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, clr.classForName(elementInfo[0].getClassName()), true, null, null).getStatement();
            }
            iterateUsingDiscriminator = true;

            // Select the required fields
            SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, stmtClassMapping, fp, sqlStmt.getPrimaryTable(), emd, 0);
        }
        else
        {
            for (int i=0;i<elementInfo.length;i++)
            {
                final Class elementCls = clr.classForName(this.elementInfo[i].getClassName());
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, elementCls, true, null, null);
                stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                stmtClassMapping.setNucleusTypeColumnName(UnionStatementGenerator.NUC_TYPE_COLUMN);
                SelectStatement subStmt = stmtGen.getStatement();

                // Select the required fields (of the element class)
                if (sqlStmt == null)
                {
                    if (elementInfo.length > 1)
                    {
                        SQLStatementHelper.selectIdentityOfCandidateInStatement(subStmt, stmtClassMapping, elementInfo[i].getAbstractClassMetaData());
                    }
                    else
                    {
                        SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(subStmt, stmtClassMapping, fp, subStmt.getPrimaryTable(), elementInfo[i].getAbstractClassMetaData(), 0);
                    }
                }
                else
                {
                    if (elementInfo.length > 1)
                    {
                        SQLStatementHelper.selectIdentityOfCandidateInStatement(subStmt, null, elementInfo[i].getAbstractClassMetaData());
                    }
                    else
                    {
                        SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(subStmt, null, fp, subStmt.getPrimaryTable(), elementInfo[i].getAbstractClassMetaData(), 0);
                    }
                }

                if (sqlStmt == null)
                {
                    sqlStmt = subStmt;
                }
                else
                {
                    sqlStmt.union(subStmt);
                }
            }
            if (sqlStmt == null)
            {
                throw new NucleusException("Unable to generate iterator statement for field=" + getOwnerMemberMetaData().getFullFieldName());
            }
        }

        if (addRestrictionOnOwner)
        {
            // Apply condition to filter by owner
            // TODO If ownerMapping is not for containerTable then do JOIN to ownerTable in the FROM clause (or find if already done)
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
                // Order by the ordering column
                SQLTable orderSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
                SQLExpression[] orderExprs = new SQLExpression[orderMapping.getNumberOfDatastoreMappings()];
                boolean descendingOrder[] = new boolean[orderMapping.getNumberOfDatastoreMappings()];
                orderExprs[0] = exprFactory.newExpression(sqlStmt, orderSqlTbl, orderMapping);
                sqlStmt.setOrdering(orderExprs, descendingOrder);
            }
        }
        else
        {
            // Apply ordering defined by <order-by>
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

        return new IteratorStatement(this, sqlStmt, stmtClassMapping);
    }
}