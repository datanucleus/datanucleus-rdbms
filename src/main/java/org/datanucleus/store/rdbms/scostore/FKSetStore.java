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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.state.RelationshipManager;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
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
import org.datanucleus.store.types.scostore.SetStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * RDBMS-specific implementation of an {@link SetStore} using foreign keys.
 */
public class FKSetStore<E> extends AbstractSetStore<E>
{
    /** Field number of owner link in element class. */
    private final int ownerFieldNumber;

    /** Statement for updating a FK in the element. */
    private String updateFkStmt;

    /** Statement for clearing a FK in the element. */
    private String clearNullifyStmt;

    /**
     * Constructor for the backing store of a FK set for RDBMS.
     * @param mmd The MetaData for the field that this represents
     * @param storeMgr The StoreManager managing the associated datastore.
     * @param clr The ClassLoaderResolver
     */
    public FKSetStore(AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
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
                elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForInterface(element_class,clr);
            }
            else
            {
                // Take the metadata for the first implementation of the reference type
                elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForImplementationOfReference(element_class,null,clr);
            }
        }
        else
        {
            // Check that the element class has MetaData
            elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(element_class, clr);
        }
        if (elementCmd == null)
        {
            throw new NucleusUserException(Localiser.msg("056003", element_class.getName(), mmd.getFullFieldName()));
        }

        elementInfo = getComponentInformationForClass(elementType, elementCmd);
        if (elementInfo == null || elementInfo.length == 0)
        {
            throw new NucleusUserException(Localiser.msg("056075", ownerMemberMetaData.getFullFieldName(), elementType));
        }
        elementMapping = elementInfo[0].getDatastoreClass().getIdMapping(); // Just use the first element type as the guide for the element mapping
        elementsAreEmbedded = false; // Can't embed element when using FK relation
        elementsAreSerialised = false; // Can't serialise element when using FK relation

        // Get the field in the element table (if any)
        if (mmd.getMappedBy() != null)
        {
            // 1-N FK bidirectional
            // The element class has a field for the owner.
            AbstractMemberMetaData eofmd = elementCmd.getMetaDataForMember(mmd.getMappedBy());
            if (eofmd == null)
            {
                throw new NucleusUserException(Localiser.msg("056024", mmd.getFullFieldName(), mmd.getMappedBy(), element_class.getName()));
            }

            // Check that the type of the element "mapped-by" field is consistent with the owner type
            //TODO check does not work if mapped by has relation to super class. Enable this
            //and run the PersistentInterfacesTest to reproduce the issue
            //TODO there is equivalent code in FKList and FKMap that was
            //not commented out. When fixing, add tests for all types of Inverses
            /*
            if (!clr.isAssignableFrom(eofmd.getType(), mmd.getAbstractClassMetaData().getFullClassName()))
            {
                throw new NucleusUserException(Localiser.msg("056025", mmd.getFullFieldName(),
                    eofmd.getFullFieldName(), eofmd.getTypeName(), mmd.getAbstractClassMetaData().getFullClassName()));
            }
            */
            String ownerFieldName = eofmd.getName();
            ownerFieldNumber = elementCmd.getAbsolutePositionOfMember(ownerFieldName);
            ownerMapping = elementInfo[0].getDatastoreClass().getMemberMapping(eofmd);
            if (ownerMapping == null && elementInfo.length > 1)
            {
                // Lookup by name only since may be interface field with multiple implementations
                ownerMapping = elementInfo[0].getDatastoreClass().getMemberMapping(eofmd.getName());
            }
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

        // TODO Remove use of containerTable - just use elementTable[0] or equivalent
        containerTable = elementInfo[0].getDatastoreClass();
        if (mmd.getMappedBy() != null && ownerMapping.getTable() != containerTable)
        {
            // Element and owner don't have consistent tables so use the one with the mapping
            // e.g collection is of subclass, yet superclass has the link back to the owner
            containerTable = ownerMapping.getTable();
        }
    }

    /**
     * This seems to return the field number in the element of the relation when it is a bidirectional relation.
     * @param op ObjectProvider for the owner.
     * @return The field number in the element for this relation
     */
    protected int getFieldNumberInElementForBidirectional(ObjectProvider op)
    {
        if (ownerFieldNumber < 0)
        {
            // Unidirectional
            return -1;
        }
        // This gives a different result when using persistent interfaces.
        // For example with the JDO2 TCK, org.apache.jdo.tck.pc.company.PIDepartmentImpl.employees will
        // return 3, yet the ownerMemberMetaData.getRelatedMetaData returns 8 since the generated implementation
        // will have all fields in a single MetaData (numbering from 0), whereas in a normal inheritance
        // tree there will be multiple MetaData (the root starting from 0)
        return op.getClassMetaData().getAbsolutePositionOfMember(ownerMemberMetaData.getMappedBy());
    }

    /**
     * Utility to update a foreign-key (and distinguisher) in the element in the case of
     * a unidirectional 1-N relationship.
     * @param op ObjectProvider for the owner
     * @param element The element to update
     * @param owner The owner object to set in the FK
     * @return Whether it was performed successfully
     */
    private boolean updateElementFk(ObjectProvider op, Object element, Object owner)
    {
        if (element == null)
        {
            return false;
        }

        validateElementForWriting(op.getExecutionContext(), element, null);

        boolean retval;
        ExecutionContext ec = op.getExecutionContext();
        String stmt = getUpdateFkStmt(element);
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
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

                    int jdbcPosition = 1;
                    if (owner == null)
                    {
                        ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, ownerMapping), null, op, ownerMemberMetaData.getAbsoluteFieldNumber());
                    }
                    else
                    {
                        ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, ownerMapping), op.getObject(), op, ownerMemberMetaData.getAbsoluteFieldNumber());
                    }
                    jdbcPosition += ownerMapping.getNumberOfDatastoreMappings();
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }
                    jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, jdbcPosition, elemMapping);

                    sqlControl.executeStatementUpdate(ec, mconn, stmt, ps, true);
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
            throw new NucleusDataStoreException(Localiser.msg("056027", stmt), e);
        }

        return retval;
    }

    /**
     * Method to update the collection to be the supplied collection of elements.
     * @param op ObjectProvider for the owner.
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
        // TODO Create set of elements to remove and remove in one call, and add new ones in one call
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
     * Method to add an object to the relationship at the collection end.
     * @param op ObjectProvider for the owner.
     * @param element Element to be added
     * @return Success indicator
     */
    public boolean add(final ObjectProvider op, E element, int size)
    {
        if (element == null)
        {
            // Sets allow no duplicates
            throw new NucleusUserException(Localiser.msg("056039"));
        }

        // Make sure that the element is persisted in the datastore (reachability)
        final Object newOwner = op.getObject();
        final ExecutionContext ec = op.getExecutionContext();

        // Find the (element) table storing the FK back to the owner
        boolean isPersistentInterface = storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterface(elementType);
        DatastoreClass elementTable = null;
        if (isPersistentInterface)
        {
            elementTable = storeMgr.getDatastoreClass(storeMgr.getNucleusContext().getMetaDataManager().getImplementationNameForPersistentInterface(elementType), clr);
        }
        else
        {
            Class elementTypeCls = clr.classForName(elementType);
            if (elementTypeCls.isInterface())
            {
                // Set<interface> so use type of element passed in and get its table
                elementTable = storeMgr.getDatastoreClass(element.getClass().getName(), clr);
            }
            else
            {
                // Use table for element type
                elementTable = storeMgr.getDatastoreClass(elementType, clr);
            }
        }
        if (elementTable == null)
        {
            // "subclass-table", persisted into table of other class
            AbstractClassMetaData[] managingCmds = storeMgr.getClassesManagingTableForClass(elementCmd, clr);
            if (managingCmds != null && managingCmds.length > 0)
            {
                // Find which of these subclasses is appropriate for this element
                for (int i=0;i<managingCmds.length;i++)
                {
                    Class tblCls = clr.classForName(managingCmds[i].getFullClassName());
                    if (tblCls.isAssignableFrom(element.getClass()))
                    {
                        elementTable = storeMgr.getDatastoreClass(managingCmds[i].getFullClassName(), clr);
                        break;
                    }
                }
            }
        }
        final DatastoreClass elementTbl = elementTable;

        boolean inserted = validateElementForWriting(ec, element, new FieldValues()
        {
            public void fetchFields(ObjectProvider elementOP)
            {
                if (elementTbl != null)
                {
                    JavaTypeMapping externalFKMapping = elementTbl.getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                    if (externalFKMapping != null)
                    {
                        // The element has an external FK mapping so set the value it needs to use in the INSERT
                        elementOP.setAssociatedValue(externalFKMapping, op.getObject());
                    }
                    if (relationDiscriminatorMapping != null)
                    {
                        // Element type has a shared FK so set the discriminator value for this relation
                        elementOP.setAssociatedValue(relationDiscriminatorMapping, relationDiscriminatorValue);
                    }
                }

                int fieldNumInElement = getFieldNumberInElementForBidirectional(elementOP);
                if (fieldNumInElement >= 0)
                {
                    // TODO Move this into RelationshipManager
                    // Managed Relations : 1-N bidir, so make sure owner is correct at persist
                    Object currentOwner = elementOP.provideField(fieldNumInElement);
                    if (currentOwner == null)
                    {
                        // No owner, so correct it
                        NucleusLogger.PERSISTENCE.info(Localiser.msg("056037", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                            StringUtils.toJVMIDString(elementOP.getObject())));
                        elementOP.replaceFieldMakeDirty(fieldNumInElement, newOwner);
                    }
                    else if (currentOwner != newOwner)
                    {
                        // Check for owner change
                        Object ownerId1 = ec.getApiAdapter().getIdForObject(currentOwner);
                        Object ownerId2 = ec.getApiAdapter().getIdForObject(newOwner);
                        if (ownerId1 != null && ownerId2 != null && ownerId1.equals(ownerId2))
                        {
                            // Must be attaching
                            if (!ec.getApiAdapter().isDetached(newOwner))
                            {
                                // Attaching, so make sure we set to the attached owner
                                elementOP.replaceField(fieldNumInElement, newOwner);
                            }
                        }
                        else if (op.getReferencedPC() == null)
                        {
                            // Not being attached so must be inconsistent owner, so throw exception
                            throw new NucleusUserException(Localiser.msg("056038", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), 
                                StringUtils.toJVMIDString(elementOP.getObject()), StringUtils.toJVMIDString(currentOwner)));
                        }
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

        if (inserted)
        {
            // Element has just been persisted so the FK will be set
            return true;
        }

        // Element was already persistent so make sure the FK is in place
        // TODO This is really "ManagedRelationships" so needs to go in RelationshipManager
        ObjectProvider elementOP = ec.findObjectProvider(element);
        if (elementOP == null)
        {
            // Element is likely being attached and this is the detached element; lookup the attached element via the id
            Object elementId = ec.getApiAdapter().getIdForObject(element);
            if (elementId != null)
            {
                element = (E) ec.findObject(elementId, false, false, element.getClass().getName());
                if (element != null)
                {
                    elementOP = ec.findObjectProvider(element);
                }
            }
        }
        int fieldNumInElement = getFieldNumberInElementForBidirectional(elementOP);
        if (fieldNumInElement >= 0 && elementOP != null)
        {
            // Managed Relations : 1-N bidir, so update the owner of the element
            elementOP.isLoaded(fieldNumInElement); // Ensure is loaded
            Object oldOwner = elementOP.provideField(fieldNumInElement);
            if (oldOwner != newOwner)
            {
                if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                {
                    NucleusLogger.PERSISTENCE.debug(Localiser.msg("055009", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(element)));
                }

                elementOP.replaceFieldMakeDirty(fieldNumInElement, newOwner);
                if (ec.getManageRelations())
                {
                    // Managed Relationships - add the change we've made here to be analysed at flush
                    RelationshipManager relationshipManager = ec.getRelationshipManager(elementOP);
                    relationshipManager.relationChange(fieldNumInElement, oldOwner, newOwner);
                    
                    if (ec.isFlushing())
                    {
                        // When already flushing process the changes right away to make them effective during the current flush
                        relationshipManager.process();
                    }
                }

                if (ec.isFlushing())
                {
                    elementOP.flush();
                }
            }
            return oldOwner != newOwner;
        }

        // 1-N unidir so update the FK if not set to be contained in the set
        boolean contained = contains(op, element);
        return (contained ? false : updateElementFk(op, element, newOwner));
    }
 
    /**
     * Method to add a collection of object to the relationship at the collection end.
     * @param op ObjectProvider for the owner.
     * @param elements Elements to be added
     * @return Success indicator
     */
    public boolean addAll(ObjectProvider op, Collection<E> elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        boolean success = false;
        Iterator<E> iter = elements.iterator();
        while (iter.hasNext())
        {
            if (add(op, iter.next(), -1))
            {
                success = true;
            }
        }

        return success;
    }

    /**
     * Method to remove the link to the collection object specified.
     * Depending on the column characteristics in the collection table, the id of the owner field may
     * be NULLed, or the record may be deleted completely (as per cascade-delete in EJB).
     * @param op ObjectProvider for the owner.
     * @param element The element of the collection to be deleted.
     * @param allowDependentField Whether to allow any cascade deletes caused by this removal
     * @return A success indicator.
     */
    public boolean remove(ObjectProvider op, Object element, int size, boolean allowDependentField)
    {
        if (element == null)
        {
            return false;
        }
        if (!validateElementForReading(op, element))
        {
            return false;
        }

        // Find the ObjectProvider for the element
        Object elementToRemove = element;
        ExecutionContext ec = op.getExecutionContext();
        if (ec.getApiAdapter().isDetached(element)) // User passed in detached object to collection.remove()!
        {
            // Find an attached equivalent of this detached object (DON'T attach the object itself)
            elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false, element.getClass().getName());
        }

        ObjectProvider elementOP = ec.findObjectProvider(elementToRemove);
        Object oldOwner = null;
        if (ownerFieldNumber >= 0)
        {
            if (!ec.getApiAdapter().isDeleted(elementToRemove))
            {
                // Find the existing owner if the record hasn't already been deleted
                elementOP.isLoaded(ownerFieldNumber);
                oldOwner = elementOP.provideField(ownerFieldNumber);
            }
        }
        else
        {
            // TODO Check if the element is managed by a different owner now
        }

        // Owner of the element has been changed
        if (ownerFieldNumber >= 0 && oldOwner != op.getObject() && oldOwner != null)
        {
            return false;
        }

        boolean deleteElement = checkRemovalOfElementShouldDelete(op);
        if (deleteElement)
        {
            if (ec.getApiAdapter().isPersistable(elementToRemove) && ec.getApiAdapter().isDeleted(elementToRemove))
            {
                // Element is waiting to be deleted so flush it (it has the FK)
                elementOP.flush();
            }
            else
            {
                // Element not yet marked for deletion so go through the normal process
                ec.deleteObjectInternal(elementToRemove);
            }
        }
        else
        {
            // Perform any necessary "managed relationships" updates on the element (when bidirectional)
            manageRemovalOfElement(op, elementToRemove);

            // Update the datastore FK
            updateElementFk(op, elementToRemove, null);
        }

        return true;
    }

    /**
     * Method to remove the links to a collection of elements specified.
     * Depending on the column characteristics in the collection table, the id
     * of the owner fields may be NULLed, or the records may be deleted completely.
     *
     * @param op ObjectProvider for the owner.
     * @param elements The elements of the collection to be deleted.
     * @return A success indicator.
     */
    public boolean removeAll(ObjectProvider op, Collection elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        // Check the first element for whether we can null the column or
        // whether we have to delete
        boolean success = true;

        Iterator iter=elements.iterator();
        while (iter.hasNext())
        {
            if (remove(op, iter.next(), -1, true))
            {
                success = false;
            }
        }

        return success;
    }

    /**
     * Convenience method to return if the removal of an element should delete the element.
     * @param op ObjectProvider for the owner.
     * @return Whether we should delete the element on removing from the collection
     */
    protected boolean checkRemovalOfElementShouldDelete(ObjectProvider op)
    {
        boolean delete = false;
        boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent)
        {
            // Elements are dependent and can't exist on their own, so delete them all
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE.debug(Localiser.msg("056034"));
            }
            delete = true;
        }
        else
        {
            if (ownerMapping.isNullable())
            {
                // Field is not dependent, but is nullable so we null the FK
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("056036"));
                }
                delete = false;
            }
            else
            {
                // Field is not dependent, and is not nullable so we just delete the elements
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("056035"));
                }
                delete = true;
            }
        }
        return delete;
    }

    /**
     * Convenience method to manage the removal of an element from the collection, performing
     * any necessary "managed relationship" updates when the field is bidirectional.
     * @param op ObjectProvider for the owner.
     * @param element The element
     */
    protected void manageRemovalOfElement(ObjectProvider op, Object element)
    {
        ExecutionContext ec = op.getExecutionContext();
        if (relationType == RelationType.ONE_TO_MANY_BI)
        {
            // TODO Move this into RelationshipManager
            // Managed Relations : 1-N bidirectional so null the owner on the elements
            if (!ec.getApiAdapter().isDeleted(element))
            {
                ObjectProvider elementOP = ec.findObjectProvider(element);
                if (elementOP != null)
                {
                    // Null the owner of the element
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("055010", op.getObjectAsPrintable(), ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(element)));
                    }

                    int relatedFieldNumber = getFieldNumberInElementForBidirectional(elementOP);
                    Object currentValue = elementOP.provideField(relatedFieldNumber);
                    if (currentValue != null)
                    {
                        elementOP.replaceFieldMakeDirty(relatedFieldNumber, null);
                        if (ec.isFlushing())
                        {
                            // Make sure this change gets flushed
                            elementOP.flush();
                        }
                    }
                }
            }
        }
    }

    /**
     * Method to allow the Set relationship to be cleared out.
     * This is called by the List.clear() method, or when the container object is being deleted
     * and the elements are to be removed (maybe for dependent field), or also when updating a Collection
     * and removing all existing prior to adding all new.
     * @param op ObjectProvider for the owner.
     */
    public void clear(ObjectProvider op)
    {
        ExecutionContext ec = op.getExecutionContext();
        boolean deleteElements = checkRemovalOfElementShouldDelete(op);
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
                        ec.findObjectProvider(element).flush();
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
            // Perform any necessary "managed relationships" updates on the element
            op.isLoaded(ownerMemberMetaData.getAbsoluteFieldNumber()); // Make sure the field is loaded
            Collection value = (Collection) op.provideField(ownerMemberMetaData.getAbsoluteFieldNumber());
            Iterator elementsIter = null;
            if (value != null && !value.isEmpty())
            {
                elementsIter = value.iterator();
            }
            else
            {
                // Maybe deleting the owner with optimistic transactions so the elements are no longer cached
                elementsIter = iterator(op);
            }
            if (elementsIter != null)
            {
                while (elementsIter.hasNext())
                {
                    Object element = elementsIter.next();
                    manageRemovalOfElement(op, element);
                }
            }

            // Clear the FKs in the datastore
            // TODO This is likely not necessary in the 1-N bidir case since we've just set the owner FK to null above
            for (int i=0;i<elementInfo.length;i++)
            {
                String stmt = getClearNullifyStmt(elementInfo[i]);
                try
                {
                    ManagedConnection mconn = storeMgr.getConnection(ec);
                    SQLController sqlControl = storeMgr.getSQLController();
                    try
                    {
                        PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
                        try
                        {
                            int jdbcPosition = 1;
                            BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                            sqlControl.executeStatementUpdate(ec, mconn, stmt, ps, true);
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
                    throw new NucleusDataStoreException(Localiser.msg("056013",stmt), e);
                }
            }
        }
    }

    /**
     * Generates the statement for clearing items by nulling the owner link out.
     * The statement will be of the form
     * <PRE>
     * UPDATE LISTTABLE SET OWNERCOL=NULL [,DISTINGUISHER=NULL]
     * WHERE OWNERCOL=?
     * </PRE>
     * @param info ElementInfo for the element
     * @return The Statement for clearing items for the owner.
     */
    protected String getClearNullifyStmt(ComponentInfo info)
    {
        if (elementInfo.length == 1 && clearNullifyStmt != null)
        {
            return clearNullifyStmt;
        }

        StringBuilder stmt = new StringBuilder("UPDATE ");
        if (elementInfo.length > 1)
        {
            stmt.append(info.getDatastoreClass().toString());
        }
        else
        {
            // Could use elementInfo[0].getDatastoreClass but need to allow for relation being in superclass table
            stmt.append(containerTable.toString());
        }

        stmt.append(" SET ");
        JavaTypeMapping ownerMapping = this.ownerMapping;
        if (ownerMemberMetaData.getMappedBy() != null)
        {
            ownerMapping = info.getDatastoreClass().getMemberMapping(ownerMemberMetaData.getMappedBy());
        }
        else
        {
            ownerMapping = info.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
        }
        for (int i=0; i<ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(", ");
            }
            stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            stmt.append("=NULL");
        }

        JavaTypeMapping relDiscrimMapping = info.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM);
        if (relDiscrimMapping != null)
        {
            for (int i=0; i<relDiscrimMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(", ");
                stmt.append(relDiscrimMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                stmt.append("=NULL");
            }
        }

        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);

        if (elementInfo.length == 1)
        {
            clearNullifyStmt = stmt.toString();
        }

        return stmt.toString();
    }

    /**
     * Generate statement for updating a Foreign Key in a FK 1-N.
     * The statement generated will be of the form
     * <PRE>
     * UPDATE ELEMENTTABLE SET FK_COL_1=?, FK_COL_2=?, [DISTINGUISHER=?]
     * WHERE ELEMENT_ID = ?
     * </PRE>
     * @return Statement for updating the FK in a FK 1-N
     */
    private String getUpdateFkStmt(Object element)
    {
        if (elementInfo.length > 1)
        {
            // Can't cache in this situation since next time may be a different implementation
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
                relDiscrimMapping = elemInfo.getDatastoreClass().getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM);
            }
        }

        StringBuilder stmt = new StringBuilder("UPDATE ").append(table.toString()).append(" SET ");
        for (int i=0; i<ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            stmt.append("=");
            stmt.append(((AbstractDatastoreMapping)ownerMapping.getDatastoreMapping(i)).getUpdateInputParameter());
        }
        if (relDiscrimMapping != null)
        {
            for (int i=0; i<relDiscrimMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(relDiscrimMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                stmt.append("=");
                stmt.append(((AbstractDatastoreMapping)relDiscrimMapping.getDatastoreMapping(i)).getUpdateInputParameter());
            }
        }

        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForElement(stmt, elemMapping, element, elementsAreSerialised, null, true);

        return stmt.toString();
    }

    /**
     * Accessor for an iterator for the set.
     * @param op ObjectProvider for the owner.
     * @return Iterator for the set.
     */
    public Iterator<E> iterator(ObjectProvider op)
    {
        ExecutionContext ec = op.getExecutionContext();

        if (elementInfo == null || elementInfo.length == 0)
        {
            return null;
        }

        // Generate the statement, and statement mapping/parameter information
        IteratorStatement iterStmt = getIteratorStatement(ec, ec.getFetchPlan(), true);
        SelectStatement sqlStmt = iterStmt.getSelectStatement();
        StatementClassMapping iteratorMappingClass = iterStmt.getStatementClassMapping();

        // Input parameter(s) - the owner
        int inputParamNum = 1;
        StatementMappingIndex ownerStmtMapIdx = new StatementMappingIndex(ownerMapping);
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
                ownerStmtMapIdx.addParameterOccurrence(paramPositions);
            }
        }
        else
        {
            int[] paramPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<ownerMapping.getNumberOfDatastoreMappings();k++)
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
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Create the statement and set the owner
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                int numParams = ownerStmtMapIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerStmtMapIdx.getMapping().setObject(ec, ps, ownerStmtMapIdx.getParameterPositionsForOccurrence(paramInstance), op.getObject());
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
                        rof = new PersistentClassROF(storeMgr, elementCmd, iteratorMappingClass, false, null, clr.classForName(elementType));

                        return new CollectionStoreIterator(op, rs, rof, this);
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
     * @param ec ExecutionContext
     * @param fp FetchPlan to use in determining which fields of element to select
     * @param addRestrictionOnOwner Whether to restrict to a particular owner (otherwise functions as bulk fetch for many owners).
     * @return The SQLStatement and its associated StatementClassMapping
     */
    public IteratorStatement getIteratorStatement(ExecutionContext ec, FetchPlan fp, boolean addRestrictionOnOwner)
    {
        SelectStatement sqlStmt = null;
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        StatementClassMapping iteratorMappingClass = new StatementClassMapping();
        if (elementInfo[0].getDatastoreClass().getDiscriminatorMetaData() != null &&
            elementInfo[0].getDatastoreClass().getDiscriminatorMetaData().getStrategy() != DiscriminatorStrategy.NONE)
        {
            // TODO Only caters for one elementInfo, but with subclass-table we can have multiple
            String elementType = ownerMemberMetaData.getCollection().getElementType();
            if (ClassUtils.isReferenceType(clr.classForName(elementType)))
            {
                String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(elementType, clr);
                Class[] cls = new Class[clsNames.length];
                for (int i=0; i<clsNames.length; i++)
                {
                    cls[i] = clr.classForName(clsNames[i]);
                }
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null).getStatement(ec);
            }
            else
            {
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, clr.classForName(elementInfo[0].getClassName()), true, null, null).getStatement(ec);
            }
            iterateUsingDiscriminator = true;

            // TODO Cater for having all possible subclasses stored in the same table (so we can select their fields too)
//            String[] elemSubclasses = op.getExecutionContext().getMetaDataManager().getSubclassesForClass(emd.getFullClassName(), false);
//            NucleusLogger.GENERAL.info(">> FKSetStore.iter iterMapDef=" + iteratorMappingDef + " table=" + sqlStmt.getPrimaryTable() +
//                " emd=" + emd.getFullClassName() + " elem.subclasses=" + StringUtils.objectArrayToString(elemSubclasses));
            // Select the required fields (of the element class)
            SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingClass, fp, sqlStmt.getPrimaryTable(), elementCmd, 0);
        }
        else
        {
            boolean selectFetchPlan = true;
            Class elementTypeCls = clr.classForName(elementType);
            if (elementTypeCls.isInterface() && elementInfo.length > 1)
            {
                // Multiple implementations of an interface, so assume the FetchPlan differs between implementation
                selectFetchPlan = false;
            }

            // TODO This only works if the different elementInfos have the same number of PK fields (otherwise get SQL error in UNION)
            for (int i=0;i<elementInfo.length;i++)
            {
                final Class elementCls = clr.classForName(this.elementInfo[i].getClassName());
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, elementCls, true, null, null);
                stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                iteratorMappingClass.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                SelectStatement subStmt = stmtGen.getStatement(ec);

                if (selectFetchPlan)
                {
                    // Select the FetchPlan fields (of the element class)
                    if (sqlStmt == null)
                    {
                        SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(subStmt, iteratorMappingClass,
                            fp, subStmt.getPrimaryTable(), elementInfo[i].getAbstractClassMetaData(), 0);
                    }
                    else
                    {
                        SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(subStmt, null,
                            fp, subStmt.getPrimaryTable(), elementInfo[i].getAbstractClassMetaData(), 0);
                    }
                }
                else
                {
                    // Select the candidate id of the element class only
                    if (sqlStmt == null)
                    {
                        SQLStatementHelper.selectIdentityOfCandidateInStatement(subStmt, iteratorMappingClass, elementInfo[i].getAbstractClassMetaData());
                    }
                    else
                    {
                        SQLStatementHelper.selectIdentityOfCandidateInStatement(subStmt, null, elementInfo[i].getAbstractClassMetaData());
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
                throw new NucleusException("Unable to generate iterator statement for field " + getOwnerMemberMetaData().getFullFieldName());
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

        if (orderMapping != null)
        {
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