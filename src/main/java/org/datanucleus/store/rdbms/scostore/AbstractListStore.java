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
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.connection.ManagedConnection;
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
    protected String shiftBulkStmt;

    /**
     * Constructor. Protected to prevent instantiation.
     * @param storeMgr Manager for the store
     * @param clr ClassLoader resolver
     */
    protected AbstractListStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    @Override
    public Iterator<E> iterator(DNStateManager ownerSM)
    {
        return listIterator(ownerSM);
    }

    @Override
    public ListIterator<E> listIterator(DNStateManager ownerSM)
    {
        return listIterator(ownerSM, -1, -1);
    }

    /**
     * Accessor for an iterator through the list elements.
     * @param ownerSM StateManager for the container.
     * @param startIdx The start point in the list (only for indexed lists).
     * @param endIdx The end point in the list (only for indexed lists).
     * @return The List Iterator
     */
    protected abstract ListIterator<E> listIterator(DNStateManager ownerSM, int startIdx, int endIdx);

    @Override
    public boolean add(DNStateManager ownerSM, E element, int size)
    {
        return internalAdd(ownerSM, 0, true, Collections.singleton(element), size);
    }

    @Override
    public void add(DNStateManager ownerSM, E element, int index, int size)
    {
        internalAdd(ownerSM, index, false, Collections.singleton(element), size);
    }

    @Override
    public boolean addAll(DNStateManager ownerSM, Collection<E> elements, int size)
    {
        return internalAdd(ownerSM, 0, true, elements, size);
    }

    @Override
    public boolean addAll(DNStateManager ownerSM, Collection<E> elements, int index, int size)
    {
        return internalAdd(ownerSM, index, false, elements, size);
    }

    /**
     * Internal method for adding an item to the List.
     * @param ownerSM StateManager
     * @param startAt The start position
     * @param atEnd Whether to add at the end
     * @param elements The Collection of elements to add.
     * @param size Current size of List (if known). -1 if not known
     * @return Whether it was successful
     */
    protected abstract boolean internalAdd(DNStateManager ownerSM, int startAt, boolean atEnd, Collection<E> elements, int size);

    @Override
    public E get(DNStateManager ownerSM, int index)
    {
        ListIterator<E> iter = listIterator(ownerSM, index, index);
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

    @Override
    public int indexOf(DNStateManager ownerSM, Object element)
    {
        validateElementForReading(ownerSM, element);
        return internalIndexOf(ownerSM, element, getIndexOfStmt(element));
    }

    @Override
    public int lastIndexOf(DNStateManager ownerSM, Object element)
    {
        validateElementForReading(ownerSM, element);
        return internalIndexOf(ownerSM, element, getLastIndexOfStmt(element));
    }

    @Override
    public boolean remove(DNStateManager ownerSM, Object element, int size, boolean allowDependentField)
    {
        if (!validateElementForReading(ownerSM, element))
        {
            return false;
        }

        Object elementToRemove = element;
        ExecutionContext ec = ownerSM.getExecutionContext();
        if (ec.getApiAdapter().isDetached(element))
        {
            // Element passed in is detached so find attached version (DON'T attach this object)
            elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false, element.getClass().getName());
        }

        boolean modified = internalRemove(ownerSM, elementToRemove, size);

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
                ec.deleteObjectInternal(elementToRemove);
            }
        }

        return modified;
    }

    /**
     * Internal method to remove the specified element from the List.
     * @param ownerSM StateManager of the owner
     * @param element The element
     * @param size Current size of list if known. -1 if not known
     * @return Whether the List was modified
     */
    protected abstract boolean internalRemove(DNStateManager ownerSM, Object element, int size);

    @Override
    public java.util.List<E> subList(DNStateManager sm, int startIdx, int endIdx)
    {
        ListIterator iter = listIterator(sm, startIdx, endIdx);
        java.util.List list = new ArrayList();
        while (iter.hasNext())
        {
            list.add(iter.next());
        }

        if (!indexedList)
        {
            // Ordered list
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
     * @param ownerSM StateManager
     * @param elements The elements
     * @return The indices of the elements in the List.
     */
    protected int[] getIndicesOf(DNStateManager ownerSM, Collection elements)
    {
        if (elements == null || elements.isEmpty())
        {
            return null;
        }

        for (Object elem : elements)
        {
            validateElementForReading(ownerSM, elem);
        }

        String stmt = getIndicesOfStmt(elements);
        try
        {
            ExecutionContext ec = ownerSM.getExecutionContext();
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

                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerSM, ec, ps, jdbcPosition, this);
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
     * @param ownerSM StateManager
     * @param element The element
     * @param stmt The statement to find the element.
     * @return The index of the element in the List.
     */
    protected int internalIndexOf(DNStateManager ownerSM, Object element, String stmt)
    {
        try
        {
            ExecutionContext ec = ownerSM.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
                try
                {
                    int jdbcPosition = 1;

                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerSM, ec, ps, jdbcPosition, this);
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
     * @param ownerSM StateManager for the list owner
     * @param index The location
     * @param stmt The statement to remove the element from the List
     * @param size Current list size (if known). -1 if not known
     */
    protected void internalRemoveAt(DNStateManager ownerSM, int index, String stmt, int size)
    {
        // Get current size from datastore if not provided
        int currentListSize = (size < 0) ? size(ownerSM) : size;

        try
        {
            ExecutionContext ec = ownerSM.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerSM, ec, ps, jdbcPosition, this);
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

                if (index != currentListSize - 1)
                {
                    // shift all elements above this down by 1 in single statement
                    internalShiftBulk(ownerSM, index, -1, mconn, true, true);
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
    }

    /**
     * Method to process a "shift" statement for all rows from the start point, updating the index in the list for the specified owner.
     * @param ownerSM StateManager for the list owner
     * @param start The start index for the shift
     * @param amount Amount to shift by (negative means shift down)
     * @param conn The connection
     * @param batched Whether the statement is batched
     * @param executeNow Whether to execute the statement now (or wait for batching)
     * @return Return code(s) from any executed statements
     * @throws NucleusDataStoreException Thrown if an error occurs
     */
    protected int[] internalShiftBulk(DNStateManager ownerSM, int start, int amount, ManagedConnection conn, boolean batched, boolean executeNow)
    {
        String shiftBulkStmt = getShiftBulkStmt();
        try
        {
            SQLController sqlControl = storeMgr.getSQLController();
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, shiftBulkStmt, batched);
            try
            {
                ExecutionContext ec = ownerSM.getExecutionContext();
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, amount, jdbcPosition, orderMapping);
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerSM, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, start, jdbcPosition, orderMapping);
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
            throw new NucleusDataStoreException(Localiser.msg("056012", shiftStmt), sqle);
        }
    }

    /**
     * Method to process a "shift" statement, updating the index in the list of the specified index.
     * @param ownerSM StateManager for the list owner
     * @param oldIndex The old index
     * @param amount Amount to shift by (negative means shift down)
     * @param conn The connection
     * @param batched Whether the statement is batched
     * @param executeNow Whether to execute the statement now (or wait for batching)
     * @return Return code(s) from any executed statements
     * @throws NucleusDataStoreException Thrown if an error occurs
     */
    protected int[] internalShift(DNStateManager ownerSM, int oldIndex, int amount, ManagedConnection conn, boolean batched, boolean executeNow)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        String shiftStmt = getShiftStmt();
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, shiftStmt, batched);
            try
            {
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, amount, jdbcPosition, orderMapping);
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerSM, ec, ps, jdbcPosition, this);
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
            throw new NucleusDataStoreException(Localiser.msg("056012", shiftStmt), sqle);
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
            // Don't cache since depends on the element
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

    /**
     * Generates the statement for shifting items in bulk.
     * <PRE>
     * UPDATE LISTTABLE SET INDEXCOL = INDEXCOL + ?
     * WHERE OWNERCOL = ?
     * AND INDEXCOL > ?
     * [AND DISTINGUISHER=?]
     * </PRE>
     * @return The Statement for shifting elements in bulk
     */
    protected String getShiftBulkStmt()
    {
        if (shiftBulkStmt == null)
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
                    stmt.append(orderMapping.getColumnMapping(i).getColumn().getIdentifier().toString());
                    stmt.append(" + ");
                    stmt.append(orderMapping.getColumnMapping(i).getUpdateInputParameter());
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);

                stmt.append(" AND ");
                stmt.append(orderMapping.getColumnMapping(0).getColumn().getIdentifier().toString());
                stmt.append(">");
                stmt.append(orderMapping.getColumnMapping(0).getInsertionInputParameter()); // Start position

                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }
                shiftBulkStmt = stmt.toString();
            }
        }
        return shiftBulkStmt;
    }
}