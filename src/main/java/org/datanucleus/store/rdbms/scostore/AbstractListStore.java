/**********************************************************************
Copyright (c) 2003 David Jencks and others. All rights reserved. 
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
2003 Andy Jefferson - updated to support inherited objects
2003 Andy Jefferson - revised logging
2004 Andy Jefferson - merged IteratorStmt and GetStmt into GetRangeStmt
2005 Andy Jefferson - added embedded PC element capability
2005 Andy Jefferson - added dependent-element when removed from collection
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.types.scostore.ListStore;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.util.Localiser;

/**
 * Abstract representation of a backing store for a List.
 */
public abstract class AbstractListStore<E> extends AbstractCollectionStore<E> implements ListStore<E>
{
    /** Whether the list is indexed. If false then it will have no orderMapping. */
    protected boolean indexedList = true;

    protected String indexOfStmt;
    protected String lastIndexOfStmt;
    protected String removeAtStmt;
    protected String shiftStmt;

    /**
     * Constructor. Protected to prevent instantiation.
     * @param storeMgr Manager for the store
     * @param clr ClassLoader resolver
     */
    protected AbstractListStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    // -------------------------- List Method implementations ------------------

    /**
     * Accessor for an iterator through the list elements.
     * @param sm StateManager for the container.
     * @return The Iterator
     */
    public Iterator<E> iterator(ObjectProvider sm)
    {
        return listIterator(sm);
    }

    /**
     * Accessor for an iterator through the list elements.
     * @param sm StateManager for the container.
     * @return The List Iterator
     */
    public ListIterator<E> listIterator(ObjectProvider sm)
    {
        return listIterator(sm, -1, -1);
    }

    /**
     * Accessor for an iterator through the list elements.
     * @param sm StateManager for the container.
     * @param startIdx The start point in the list (only for indexed lists).
     * @param endIdx The end point in the list (only for indexed lists).
     * @return The List Iterator
     */
    protected abstract ListIterator<E> listIterator(ObjectProvider sm, int startIdx, int endIdx);

    /**
     * Method to add an element to the List.
     * @param sm StateManager
     * @param element The element to remove
     * @param size Size of the current list (if known, -1 if not)
     * @return Whether it was added successfully.
     */
    public boolean add(ObjectProvider sm, E element, int size)
    {
        return internalAdd(sm, 0, true, Collections.singleton(element), size);
    }

    /**
     * Method to add an element to the List.
     * @param element The element to add.
     * @param index The location to add at
     * @param sm StateManager.
     */
    public void add(ObjectProvider sm, E element, int index, int size)
    {
        internalAdd(sm, index, false, Collections.singleton(element), size);
    }

    /**
     * Method to add a collection of elements to the List.
     * @param sm StateManager
     * @param elements The elements to remove
     * @param size Current size of the list (if known). -1 if not known
     * @return Whether they were added successfully.
     */
    public boolean addAll(ObjectProvider sm, Collection<E> elements, int size)
    {
        return internalAdd(sm, 0, true, elements, size);
    }

    /**
     * Method to add all elements from a Collection to the List.
     * @param sm StateManager
     * @param elements The collection
     * @param index The location to add at
     * @param size Current size of the list (if known). -1 if not known
     * @return Whether it was successful
     */
    public boolean addAll(ObjectProvider sm, Collection<E> elements, int index, int size)
    {
        return internalAdd(sm, index, false, elements, size);
    }

    /**
     * Internal method for adding an item to the List.
     * @param sm StateManager
     * @param startAt The start position
     * @param atEnd Whether to add at the end
     * @param elements The Collection of elements to add.
     * @param size Current size of List (if known). -1 if not known
     * @return Whether it was successful
     */
    protected abstract boolean internalAdd(ObjectProvider sm, int startAt, boolean atEnd, Collection<E> elements, int size);

    /**
     * Method to retrieve an element from the List.
     * @param sm StateManager for the owner
     * @param index The index of the element required.
     * @return The object
     */
    public E get(ObjectProvider sm, int index)
    {
        ListIterator<E> iter = listIterator(sm, index, index);
        if (iter == null || !iter.hasNext())
        {
            return null;
        }
        if (!indexedList)
        {
            // Restrict to the actual element since can't be done in the query
            E obj = null;
            int position = 0;
            while (iter.hasNext())
            {
                obj = iter.next();
                if (position == index)
                {
                    return obj;
                }
                position++;
            }
        }

        return iter.next();
    }

    /**
     * Accessor for the indexOf an object in the List.
     * @param sm StateManager for the owner
     * @param element The element.
     * @return The index
     */
    public int indexOf(ObjectProvider sm, Object element)
    {
        validateElementForReading(sm, element);
        return internalIndexOf(sm, element, getIndexOfStmt(element));
    }

    /**
     * Method to retrieve the last index of an object in the list.
     * @param sm StateManager for the owner
     * @param element The object
     * @return The last index
     */
    public int lastIndexOf(ObjectProvider sm, Object element)
    {
        validateElementForReading(sm, element);
        return internalIndexOf(sm, element, getLastIndexOfStmt(element));
    }

    /**
     * Method to remove the specified element from the List.
     * @param sm StateManager for the owner
     * @param element The element to remove.
     * @param size Current size of list if known. -1 if not known
     * @param allowDependentField Whether to allow any cascade deletes caused by this removal
     * @return Whether it was removed successfully.
     */
    public boolean remove(ObjectProvider sm, Object element, int size, boolean allowDependentField)
    {
        if (!validateElementForReading(sm, element))
        {
            return false;
        }

        Object elementToRemove = element;
        ExecutionContext ec = sm.getExecutionContext();
        if (ec.getApiAdapter().isDetached(element))
        {
            // Element passed in is detached so find attached version (DON'T attach this object)
            elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false, element.getClass().getName());
        }

        boolean modified = internalRemove(sm, elementToRemove, size);

        if (allowDependentField)
        {
            CollectionMetaData collmd = ownerMemberMetaData.getCollection();
            boolean dependent = collmd.isDependentElement();
            if (ownerMemberMetaData.isCascadeRemoveOrphans())
            {
                dependent = true;
            }
            if (dependent && !collmd.isEmbeddedElement())
            {
                // Delete the element if it is dependent
                sm.getExecutionContext().deleteObjectInternal(elementToRemove);
            }
        }

        return modified;
    }

    /**
     * Method to remove an object at an index in the List.
     * If the list is ordered, will remove the element completely since no index positions exist.
     * @param sm StateManager
     * @param index The location
     * @param size Current size of the list (if known). -1 if not known
     * @return The object that was removed
     */
    public E remove(ObjectProvider sm, int index, int size)
    {
        E element = get(sm, index);
        if (indexedList)
        {
            // Remove the element at this position
            internalRemoveAt(sm, index, size);
        }
        else
        {
            // Ordered list doesn't allow indexed removal so just remove the element
            internalRemove(sm, element, size);
        }

        // Dependent element
        CollectionMetaData collmd = ownerMemberMetaData.getCollection();
        boolean dependent = collmd.isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent && !collmd.isEmbeddedElement())
        {
            if (!contains(sm, element))
            {
                // Delete the element if it is dependent and doesn't have a duplicate entry in the list
                sm.getExecutionContext().deleteObjectInternal(element);
            }
        }

        return element;
    }

    /**
     * Internal method to remove the specified element from the List.
     * @param sm StateManager of the owner
     * @param element The element
     * @param size Current size of list if known. -1 if not known
     * @return Whether the List was modified
     */
    protected abstract boolean internalRemove(ObjectProvider sm, Object element, int size);

    /**
     * Internal method to remove an object at a location from the List.
     * @param sm StateManager
     * @param index The index of the element to remove
     * @param size Current list size (if known). -1 if not known
     */
    protected abstract void internalRemoveAt(ObjectProvider sm, int index, int size);

    /**
     * Method to retrieve a list of elements in a range.
     * @param sm StateManager
     * @param startIdx From index (inclusive).
     * @param endIdx To index (exclusive)
     * @return Sub List of elements in this range.
     */
    public java.util.List<E> subList(ObjectProvider sm, int startIdx, int endIdx)
    {
        ListIterator iter = listIterator(sm, startIdx, endIdx);
        java.util.List list = new ArrayList();
        while (iter.hasNext())
        {
            list.add(iter.next());
        }
        if (!indexedList)
        {
            if (list.size() > (endIdx-startIdx))
            {
                // Iterator hasn't restricted what is returned so do the index range restriction here
                return list.subList(startIdx, endIdx);
            }
        }
        return list;
    }

    /**
     * Utility to find the indices of a collection of elements.
     * The returned list are in reverse order (highest index first).
     * @param sm StateManager
     * @param elements The elements
     * @return The indices of the elements in the List.
     */
    protected int[] getIndicesOf(ObjectProvider sm, Collection elements)
    {
        if (elements == null || elements.isEmpty())
        {
            return null;
        }

        Iterator iter = elements.iterator();
        while (iter.hasNext())
        {
            validateElementForReading(sm, iter.next());
        }

        String stmt = getIndicesOfStmt(elements);
        try
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
                try
                {
                    Iterator elemIter = elements.iterator();
                    int jdbcPosition = 1;
                    while (elemIter.hasNext())
                    {
                        Object element = elemIter.next();

                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                        jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, jdbcPosition, elementMapping);
                        if (relationDiscriminatorMapping != null)
                        {
                            jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                        }
                    }

                    List<Integer> indexes = new ArrayList();
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        while (rs.next())
                        {
                            indexes.add(rs.getInt(1));
                        }
                        JDBCUtils.logWarnings(rs);
                    }
                    finally
                    {
                        rs.close();
                    }

                    if (indexes.isEmpty())
                    {
                        return null;
                    }

                    int i=0;
                    int[] indicesReturn = new int[indexes.size()];
                    for (Integer idx : indexes)
                    {
                        indicesReturn[i++] = idx;
                    }
                    return indicesReturn;
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
            throw new NucleusDataStoreException(Localiser.msg("056017", stmt), e);
        }
    }

    /**
     * Internal method to find the index of an element.
     * @param sm StateManager
     * @param element The element
     * @param stmt The statement to find the element.
     * @return The index of the element in the List.
     */
    protected int internalIndexOf(ObjectProvider sm, Object element, String stmt)
    {
        try
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
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
                        boolean found = rs.next();
                        if (!found)
                        {
                            JDBCUtils.logWarnings(rs);
                            return -1;
                        }
                        int index = rs.getInt(1);
                        JDBCUtils.logWarnings(rs);
                        return index;
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
            throw new NucleusDataStoreException(Localiser.msg("056017", stmt), e);
        }
    }

    /**
     * Internal method to remove an object at a location in the List.
     * @param sm StateManager
     * @param index The location
     * @param stmt The statement to remove the element from the List
     * @param size Current list size (if known). -1 if not known
     */
    protected void internalRemoveAt(ObjectProvider sm, int index, String stmt, int size)
    {
        int currentListSize = 0;
        if (size < 0)
        {
            // Get the current size from the datastore
            currentListSize = size(sm);
        }
        else
        {
            currentListSize = size;
        }

        ExecutionContext ec = sm.getExecutionContext();
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                    jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, index, jdbcPosition, orderMapping);
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    int[] rowsDeleted = sqlControl.executeStatementUpdate(ec, mconn, stmt, ps, true);
                    if (rowsDeleted[0] == 0)
                    {
                        // ?? throw exception??
                    }
                }
                finally
                {
                    sqlControl.closeStatement(mconn, ps);
                }

                // shift down
                if (index != currentListSize - 1)
                {
                    for (int i = index + 1; i < currentListSize; i++)
                    {
                        // Shift this index down 1
                        internalShift(sm, mconn, false, i, -1, true);
                    }
                }
            }
            finally
            {
                mconn.release();
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056012", stmt), e);
        }
        catch (MappedDatastoreException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056012", stmt), e);
        }
    }

    /**
     * Method to process a "shift" statement, updating the index in the list of the specified index.
     * @param sm StateManager
     * @param conn The connection
     * @param batched Whether the statement is batched
     * @param oldIndex The old index
     * @param amount Amount to shift by (negative means shift down)
     * @param executeNow Whether to execute the statement now (or wait for batching)
     * @return Return code(s) from any executed statements
     * @throws MappedDatastoreException Thrown if an error occurs
     */
    protected int[] internalShift(ObjectProvider sm, ManagedConnection conn, boolean batched, int oldIndex, int amount, boolean executeNow) 
    throws MappedDatastoreException
    {
        ExecutionContext ec = sm.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        String shiftStmt = getShiftStmt();
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, shiftStmt, batched);
            try
            {
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, amount, jdbcPosition, orderMapping);
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, oldIndex, jdbcPosition, orderMapping);
                if (relationDiscriminatorMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                }

                // Execute the statement
                return sqlControl.executeStatementUpdate(ec, conn, shiftStmt, ps, executeNow);
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
        catch (SQLException sqle)
        {
            throw new MappedDatastoreException(shiftStmt, sqle);
        }
    }

    /**
     * Generate statement for getting the index of an item.
     * <PRE>
     * SELECT INDEXCOL FROM LISTTABLE
     * WHERE OWNERCOL=?
     * AND ELEMENTCOL=?
     * [AND EMBEDDEDFIELD1=? AND EMBEDDEDFIELD2=? AND ...]
     * [AND DISTINGUISHER=?]
     * ORDER BY INDEXCOL
     * </PRE>
     * @param element The element to get the index of
     * @return The Statement for getting the index of an item
     */
    protected String getIndexOfStmt(Object element)
    {
        if (elementMapping instanceof ReferenceMapping && elementMapping.getNumberOfColumnMappings() > 1)
        {
            // Don't cache since depends on the element
            return getIndexOfStatementString(element);
        }

        if (indexOfStmt == null)
        {
            synchronized (this)
            {
                indexOfStmt = getIndexOfStatementString(element);
            }
        }
        return indexOfStmt;
    }

    private String getIndexOfStatementString(Object element)
    {
        StringBuilder stmt = new StringBuilder("SELECT ");
        for (int i = 0; i < orderMapping.getNumberOfColumnMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
        }
        stmt.append(" FROM ").append(containerTable.toString()).append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, isElementsAreSerialised(), null, false);
        if (relationDiscriminatorMapping != null)
        {
            BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
        }

        stmt.append(" ORDER BY ");
        for (int i = 0; i < orderMapping.getNumberOfColumnMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
        }

        return stmt.toString();
    }

    /**
     * Generates the statement for getting the index of the last item.
     * 
     * <PRE>
     * SELECT INDEXCOL FROM LISTTABLE
     * WHERE OWNERCOL=?
     * AND ELEMENTCOL=?
     * [AND EMBEDDEDFIELD1=? AND EMBEDDEDFIELD2=? AND ...]
     * [AND DISTINGUISHER=?]
     * ORDER BY INDEXCOL DESC
     * </PRE>
     * @param element The element to get index of
     * @return The Statement for getting the last item
     */
    protected String getLastIndexOfStmt(Object element)
    {
        if (elementMapping instanceof ReferenceMapping && elementMapping.getNumberOfColumnMappings() > 1)
        {
            return getLastIndexOfStatementString(element);
        }

        if (lastIndexOfStmt == null)
        {
            synchronized (this)
            {
                lastIndexOfStmt = getLastIndexOfStatementString(element);
            }
        }
        return lastIndexOfStmt;
    }

    private String getLastIndexOfStatementString(Object element)
    {
        StringBuilder stmt = new StringBuilder("SELECT ");
        for (int i = 0; i < orderMapping.getNumberOfColumnMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
        }
        stmt.append(" FROM ").append(containerTable.toString()).append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, isElementsAreSerialised(), null, false);
        if (relationDiscriminatorMapping != null)
        {
            BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
        }

        stmt.append(" ORDER BY ");
        for (int i = 0; i < orderMapping.getNumberOfColumnMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
            stmt.append(" DESC ");
        }

        return stmt.toString();
    }

    /**
     * Generates the statement for getting the indices of a collection of element. Order into descending index order
     * (highest first) so they will NOT be in the same order as they appear in the input collection "elements".
     * 
     * <PRE>
     * SELECT INDEXCOL FROM LISTTABLE
     * WHERE (OWNERCOL=? AND ELEMENT_COL=? [AND DISTINGUISHER=?]) OR
     *       (OWNERCOL=? AND ELEMENT_COL=? [AND DISTINGUISHER=?]) OR
     *       (OWNERCOL=? AND ELEMENT_COL=? [AND DISTINGUISHER=?])
     * ORDER BY INDEXCOL DESC
     * </PRE>
     * @param elements The elements to retrieve the indices for.
     * @return The Statement for getting the indices of the collection.
     */
    protected String getIndicesOfStmt(Collection elements)
    {
        StringBuilder stmt = new StringBuilder("SELECT ");
        for (int i = 0; i < orderMapping.getNumberOfColumnMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
        }
        stmt.append(" FROM ").append(containerTable.toString()).append(" WHERE ");
        Iterator iter = elements.iterator();
        boolean first_element = true;
        while (iter.hasNext())
        {
            Object element = iter.next(); // Move to next element

            stmt.append(first_element ? "(" : " OR (");
            BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
            BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, isElementsAreSerialised(), null, false);
            if (relationDiscriminatorMapping != null)
            {
                BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
            }

            stmt.append(")");
            first_element = false;
        }

        stmt.append(" ORDER BY ");
        for (int i = 0; i < orderMapping.getNumberOfColumnMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString()).append(" DESC");
        }

        return stmt.toString();
    }

    /**
     * Generates the statement for removing an item.
     * 
     * <PRE>
     * DELETE FROM LISTTABLE
     * WHERE OWNERCOL = ?
     * AND INDEXCOL = ?
     * [AND DISTINGUISHER=?]
     * </PRE>
     * @return The Statement for removing an item from a position
     */
    protected String getRemoveAtStmt()
    {
        if (removeAtStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("DELETE FROM ").append(containerTable.toString()).append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                if (orderMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, orderMapping, null, false);
                }
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }
                removeAtStmt = stmt.toString();
            }
        }
        return removeAtStmt;
    }

    /**
     * Generates the statement for shifting items.
     * 
     * <PRE>
     * UPDATE LISTTABLE SET INDEXCOL = ? + INDEXCOL
     * WHERE OWNERCOL = ?
     * AND INDEXCOL = ?
     * [AND DISTINGUISHER=?]
     * </PRE>
     * @return The Statement for shifting elements
     */
    protected String getShiftStmt()
    {
        if (shiftStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("UPDATE ").append(containerTable.toString()).append(" SET ");

                for (int i = 0; i < orderMapping.getNumberOfColumnMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(",");
                    }
                    stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
                    stmt.append(" = ");
                    stmt.append(orderMapping.getColumnMapping(i).getUpdateInputParameter());
                    stmt.append(" + ");
                    stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                BackingStoreHelper.appendWhereClauseForMapping(stmt, orderMapping, null, false);
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }
                shiftStmt = stmt.toString();
            }
        }
        return shiftStmt;
    }
}