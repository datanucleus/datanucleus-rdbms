/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved.
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
2002 Mike Martin (TJDO)
2003 Andy Jefferson - coding standards
2003 Andy Jefferson - changed to use Logger
2004 Andy Jefferson - moved statements from subclasses to this class.
2005 Andy Jefferson - added embedded PC element capability
2005 Andy Jefferson - added dependent-element when removed from collection
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.scostore.SetStore;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Abstract representation of the backing store for a Set/Collection.
 */
public abstract class AbstractSetStore extends AbstractCollectionStore implements SetStore
{
    /**
     * Constructor.
     * @param storeMgr Manager for the store
     * @param clr The ClassLoaderResolver
     */
    protected AbstractSetStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    /**
     * Accessor for an iterator for the set.
     * Implemented by the subclass using whatever mechanism the underlying datastore provides.
     * @param op ObjectProvider for the set. 
     * @return Iterator for the set.
     */
    public abstract Iterator iterator(ObjectProvider op);

    /**
     * Adds one element to the association owner vs elements.
     * @param op ObjectProvider for the container
     * @param element The element to add
     * @return Whether it was successful 
     */
    public boolean add(ObjectProvider op, Object element, int size)
    {
        ExecutionContext ec = op.getExecutionContext();
        validateElementForWriting(ec, element, null);

        boolean modified = false;

        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                int[] num = internalAdd(op, mconn, false, element, true);
                if (num[0] > 0)
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
            NucleusLogger.DATASTORE.error(e);
            String msg = Localiser.msg("056009", e.getMessage());
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusDataStoreException(msg, e);
        }

        return modified;
    }
    
    /**
     * Adds all elements from a collection to the association owner vs elements
     * @param op ObjectProvider for the container
     * @param elements The elements to add
     * @param size Current size of set (if known). Not used by sets
     * @return Whether it was successful 
     */
    public boolean addAll(ObjectProvider op, Collection elements, int size)
    {
        if (elements == null || elements.size() == 0)
        {
            return false;
        }

        boolean modified = false;
        List exceptions = new ArrayList();
        boolean batched = (elements.size() > 1);

        // Validate all elements for writing
        ExecutionContext ec = op.getExecutionContext();
        Iterator iter = elements.iterator();
        while (iter.hasNext())
        {
            validateElementForWriting(ec, iter.next(), null);
        }

        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            // Loop through all elements to be added
            iter = elements.iterator();
            Object element = null;
            int[] returnCode = null;
            while (iter.hasNext())
            {
                element = iter.next();

                try
                {
                    returnCode = internalAdd(op, mconn, batched, element, !batched || (batched && !iter.hasNext()));
                }
                catch (MappedDatastoreException mde)
                {
                    exceptions.add(mde);
                    NucleusLogger.DATASTORE.error(mde);
                }
            }

            if (exceptions.size() == 0)
            {
                if (returnCode == null)
                {
                    modified = false;
                }
                else
                {
                    for (int i=0;i<returnCode.length;i++)
                    {
                        if (returnCode[i] > 0)
                        {
                            modified = true;
                        }
                    }
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
            throw new NucleusDataStoreException(msg, (Throwable[])exceptions.toArray(new Throwable[exceptions.size()]), op.getObject());
        }

        return modified;
    }

    /**
     * Removes the association to one element
     * @param op ObjectProvider for the container
     * @param element Element to remove
     * @param size Current size
     * @param allowDependentField Whether to allow any cascade deletes caused by this removal
     * @return Whether it was successful 
     */
    public boolean remove(ObjectProvider op, Object element, int size, boolean allowDependentField)
    {
        if (!validateElementForReading(op, element))
        {
            NucleusLogger.DATASTORE.debug("Attempt to remove element=" + StringUtils.toJVMIDString(element) + " but doesn't exist in this Set.");
            return false;
        }

        Object elementToRemove = element;
        ExecutionContext ec = op.getExecutionContext();
        if (ec.getApiAdapter().isDetached(element))
        {
            // Element passed in is detached so find attached version (DON'T attach this object)
            elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false, element.getClass().getName());
        }

        boolean modified = remove(op, elementToRemove, size);
        CollectionMetaData collmd = ownerMemberMetaData.getCollection();
        boolean dependent = collmd.isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (allowDependentField && dependent && !collmd.isEmbeddedElement())
        {
            // Delete the element if it is dependent
            op.getExecutionContext().deleteObjectInternal(elementToRemove);
        }

        return modified;
    }

    /**
     * Remove all elements from a collection from the association owner vs elements.
     * This implementation iterates around the remove() method doing each element 1 at a time. 
     * Please refer to the JoinSetStore and FKSetStore for the variations used there. 
     * This is used for Map key and value stores.
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

        boolean modified = false;
        List exceptions = new ArrayList();
        boolean batched = (elements.size() > 1);

        // Validate all elements exist
        Iterator iter = elements.iterator();
        while (iter.hasNext())
        {
            Object element = iter.next();
            if (!validateElementForReading(op, element))
            {
                NucleusLogger.DATASTORE.debug("AbstractSetStore::removeAll element=" + element + " doesn't exist in this Set.");
                return false;
            }
        }

        try
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                // Process all waiting batched statements before we start our work
                SQLController sqlControl = storeMgr.getSQLController();
                try
                {
                    sqlControl.processStatementsForConnection(mconn);
                }
                catch (SQLException e)
                {
                    throw new MappedDatastoreException("SQLException", e);
                }

                iter = elements.iterator();
                while (iter.hasNext())
                {
                    Object element = iter.next();
 
                    try
                    {
                        // Process the remove
                        int[] rc = internalRemove(op, mconn, batched, element, !batched || (batched && !iter.hasNext()));
                        if (rc != null)
                        {
                            for (int i=0;i<rc.length;i++)
                            {
                                if (rc[i] > 0)
                                {
                                    // At least one record was inserted
                                    modified = true;
                                }
                            }
                        }
                    }
                    catch (MappedDatastoreException mde)
                    {
                        mde.printStackTrace();
                        exceptions.add(mde);
                        NucleusLogger.DATASTORE.error(mde);
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
            e.printStackTrace();
            exceptions.add(e);
            NucleusLogger.DATASTORE.error(e);
        }

        if (!exceptions.isEmpty())
        {
            // Throw all exceptions received as the cause of a NucleusDataStoreException so the user can see which record(s) didn't remove
            String msg = Localiser.msg("056012", ((Exception) exceptions.get(0)).getMessage());
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusDataStoreException(msg, (Throwable[])exceptions.toArray(new Throwable[exceptions.size()]), op.getObject());
        }

        return modified;
    }

    /**
     * Method to process an "add" statement.
     * @param op ObjectProvider for the owner
     * @param conn The connection
     * @param batched Whether we are batching it
     * @param element The element
     * @param processNow Whether to flush to the datastore now
     * @return Number of datastore records changed (always 0 if batch since nothing yet changed)
     * @throws MappedDatastoreException Thrown if an error occurs
     */
    public int[] internalAdd(ObjectProvider op, ManagedConnection conn, boolean batched, Object element, boolean processNow)
    throws MappedDatastoreException
    {
        ExecutionContext ec = op.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        String addStmt = getAddStmt();
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, addStmt, batched);
            try
            {
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, element, jdbcPosition, elementMapping);
                if (relationDiscriminatorMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                }

                // Execute the statement
                return sqlControl.executeStatementUpdate(ec, conn, addStmt, ps, processNow);
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
        catch (SQLException e)
        {
            throw new MappedDatastoreException(getAddStmt(), e);
        }
    }

    public boolean remove(ObjectProvider op, Object element, int size)
    {
        boolean modified = false;
        ExecutionContext ec = op.getExecutionContext();
        String removeStmt = getRemoveStmt(element);
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, removeStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, jdbcPosition, elementMapping);
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    int[] rowsDeleted = sqlControl.executeStatementUpdate(ec, mconn, removeStmt, ps, true);
                    modified = (rowsDeleted[0] == 1);
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
            String msg = Localiser.msg("056012",removeStmt);
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusDataStoreException(msg, e);
        }
        return modified;
    }

    public int[] internalRemove(ObjectProvider op, ManagedConnection conn, boolean batched, Object element, boolean executeNow) 
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
        catch (SQLException e)
        {
            throw new MappedDatastoreException("SQLException", e);
        }
    }
}