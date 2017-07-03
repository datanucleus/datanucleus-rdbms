/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NotYetFlushedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.types.scostore.ArrayStore;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Abstract representation of the backing store for an array.
 * @param <E> Type of element in this array
 */
public abstract class AbstractArrayStore<E> extends ElementContainerStore implements ArrayStore<E>
{
    /**
     * Constructor.
     * @param storeMgr Manager for the store
     * @param clr ClassLoader resolver
     */
    protected AbstractArrayStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    /**
     * Accessor for the array from the datastore.
     * @param op SM for the owner
     * @return The array (as a List of objects)
     */
    public List<E> getArray(ObjectProvider op)
    {
        Iterator<E> iter = iterator(op);
        List elements = new ArrayList();
        while (iter.hasNext())
        {
            Object obj = iter.next();
            elements.add(obj);
        }

        return elements;
    }

    /**
     * Clear the association from owner to all elements. Observes the necessary dependent field settings 
     * with respect to whether it should delete the element when doing so.
     * @param op ObjectProvider for the container.
     */
    public void clear(ObjectProvider op)
    {
        Collection dependentElements = null;
        if (ownerMemberMetaData.getArray().isDependentElement())
        {
            // Retain the dependent elements that need deleting after clearing
            dependentElements = new HashSet();
            Iterator iter = iterator(op);
            while (iter.hasNext())
            {
                Object elem = iter.next();
                if (op.getExecutionContext().getApiAdapter().isPersistable(elem))
                {
                    dependentElements.add(elem);
                }
            }
        }
        clearInternal(op);

        if (dependentElements != null && dependentElements.size() > 0)
        {
            op.getExecutionContext().deleteObjects(dependentElements.toArray());
        }
    }

    /**
     * Method to set the array for the specified owner to the passed value.
     * @param op ObjectProvider for the owner
     * @param array the array
     * @return Whether the array was updated successfully
     */
    public boolean set(ObjectProvider op, Object array)
    {
        if (array == null || Array.getLength(array) == 0)
        {
            return true;
        }

        // Validate all elements for writing
        ExecutionContext ec = op.getExecutionContext();
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++)
        {
            Object obj = Array.get(array, i);
            validateElementForWriting(ec, obj, null);
        }

        boolean modified = false;
        List exceptions = new ArrayList();
        boolean batched = allowsBatching() && length > 1;

        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                processBatchedWrites(mconn);

                // Loop through all elements to be added
                E element = null;
                for (int i = 0; i < length; i++)
                {
                    element = (E) Array.get(array, i);

                    try
                    {
                        // Add the row to the join table
                        int[] rc = internalAdd(op, element, mconn, batched, i, (i == length - 1));
                        if (rc != null)
                        {
                            for (int j = 0; j < rc.length; j++)
                            {
                                if (rc[j] > 0)
                                {
                                    // At least one record was inserted
                                    modified = true;
                                }
                            }
                        }
                    }
                    catch (MappedDatastoreException mde)
                    {
                        exceptions.add(mde);
                        NucleusLogger.DATASTORE.error("Exception thrown in set of element", mde);
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
            NucleusLogger.DATASTORE.error("Exception thrown in set of element", e);
        }

        if (!exceptions.isEmpty())
        {
            // Throw all exceptions received as the cause of a NucleusDataStoreException so the user can see which
            // record(s) didn't persist
            String msg = Localiser.msg("056009", ((Exception) exceptions.get(0)).getMessage());
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusDataStoreException(msg, (Throwable[]) exceptions.toArray(new Throwable[exceptions.size()]), op.getObject());
        }

        return modified;
    }

    /**
     * Adds one element to the association owner vs elements
     * @param op ObjectProvider for the container
     * @param element The element to add
     * @param position The position to add this element at
     * @return Whether it was successful
     */
    public boolean add(ObjectProvider op, E element, int position)
    {
        ExecutionContext ec = op.getExecutionContext();
        validateElementForWriting(ec, element, null);

        boolean modified = false;

        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);

            try
            {
                // Add a row to the join table
                int[] returnCode = internalAdd(op, element, mconn, false, position, true);
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
            throw new NucleusDataStoreException(Localiser.msg("056009", e.getMessage()), e.getCause());
        }

        return modified;
    }

    /**
     * Accessor for an iterator through the array elements.
     * @param ownerOP ObjectProvider for the container.
     * @return The Iterator
     */
    public abstract Iterator<E> iterator(ObjectProvider ownerOP);

    public void clearInternal(ObjectProvider ownerOP)
    {
        String clearStmt = getClearStmt();
        try
        {
            ExecutionContext ec = ownerOP.getExecutionContext();
            ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, clearStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                    if (relationDiscriminatorMapping != null)
                    {
                        BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    sqlControl.executeStatementUpdate(ec, mconn, clearStmt, ps, true);
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
            throw new NucleusDataStoreException(Localiser.msg("056013", clearStmt), e);
        }
    }

    /**
     * Internal method to add a row to the join table.
     * Used by add() and set() to add a row to the join table.
     * @param op ObjectProvider for the owner of the collection
     * @param element The element to add the relation to
     * @param conn The connection
     * @param batched Whether we are batching
     * @param orderId The order id to use for this element relation
     * @param executeNow Whether to execute the statement now (and not wait for any batch)
     * @return Whether a row was inserted
     * @throws MappedDatastoreException Thrown if an error occurs
     */
    public int[] internalAdd(ObjectProvider op, E element, ManagedConnection conn, boolean batched, int orderId, boolean executeNow) 
            throws MappedDatastoreException
    {
        ExecutionContext ec = op.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        String addStmt = getAddStmtForJoinTable();
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, addStmt, false);
            boolean notYetFlushedError = false;
            try
            {
                // Insert the join table row
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, element, jdbcPosition, elementMapping);
                jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, orderId, jdbcPosition, orderMapping);
                if (relationDiscriminatorMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                }

                // Execute the statement
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

    public void processBatchedWrites(ManagedConnection mconn) throws MappedDatastoreException
    {
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            sqlControl.processStatementsForConnection(mconn); // Process all waiting batched statements before we start our work
        }
        catch (SQLException e)
        {
            throw new MappedDatastoreException("SQLException", e);
        }
    }
}