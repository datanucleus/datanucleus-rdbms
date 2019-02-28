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
2003 Erik Bengtson - fixed bug [833915] implements interface Queryable.
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - added resultClass
2008 Andy Jefferson - Removed optimistic restriction
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.query;

import java.io.ObjectStreamException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.query.AbstractQueryResultIterator;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Lazy collection results from a Query with the ResultSet in a forward direction.
 * In general the actual result elements are only loaded when accessed with the exception of 
 * non-transactional or optimistic contexts that load the elements at initialisation.
 * <p>
 * If the query had a range defined and this was not handled in the datastore query then
 * this QueryResult will skip the unrequired records and just return the range requested.
 * </p>
 */
public final class ForwardQueryResult<E> extends AbstractRDBMSQueryResult<E> implements java.io.Serializable
{
    /** Whether there are still more rows to be processed in the ResultSet. */
    protected boolean moreResultSetRows;

    /** The Result Objects. */
    protected List<E> resultObjs = new ArrayList();

    protected List resultIds = null;

    /** The candidate list restriction (optional). */
    private Collection candidates;

    /**
     * Constructor of the result from a Query.
     * @param query The Query
     * @param rof The factory to retrieve results from
     * @param rs The ResultSet from the Query Statement
     * @param fp FetchPlan
     * @param candidates Candidate elements
     */
    public ForwardQueryResult(Query query, ResultObjectFactory<E> rof, ResultSet rs, FetchPlan fp, Collection candidates)
    {
        super(query, rof, rs, fp);

        if (query.useResultsCaching())
        {
            resultIds = new ArrayList();
        }

        if (candidates != null)
        {
            this.candidates = new ArrayList(candidates);
        }
    }

    public void initialise()
    throws SQLException
    {
        // Move to first row
        moreResultSetRows = rs.next();

        if (applyRangeChecks)
        {
            // Move to first row of the required range
            for (int i=0;i<query.getRangeFromIncl();i++)
            {
                moreResultSetRows = rs.next();
                if (!moreResultSetRows)
                {
                    break;
                }
            }
        }

        int fetchSize = query.getFetchPlan().getFetchSize();
        if (!moreResultSetRows)
        {
            // ResultSet is empty, so just close it
            closeResults();
        }
        else if (fetchSize == FetchPlan.FETCH_SIZE_GREEDY)
        {
            // "greedy" mode, so load all results now
            advanceToEndOfResultSet();
        }
        else if (fetchSize > 0)
        {
            // Load "fetchSize" results now
            processNumberOfResults(fetchSize);
        }
    }

    /**
     * Method to advance through the results, processing the specified number of results.
     * @param number Number of results (-1 means process all)
     */
    private void processNumberOfResults(int number)
    {
        Iterator iter = iterator();
        if (number < 0)
        {
            while (iter.hasNext())
            {
                iter.next();
            }   
        }
        else
        {
            for (int i=0;i<number;i++)
            {
                if (iter.hasNext())
                {
                    iter.next();
                }
            }
        }
    }

    /**
     * Internal method to advance to the end of the ResultSet, populating
     * the resultObjs, and close the ResultSet when complete.
     */
    private void advanceToEndOfResultSet()
    {
        if (rs == null)
        {
            // Results already closed, so all rows loaded
            return;
        }

        // Process all remaining rows of the results
        processNumberOfResults(-1);
    }

    /**
     * Accessor for the next object from the ResultSet.
     * @return The next element from the ResultSet. 
     */
    protected E nextResultSetElement()
    {
        if (rof == null)
        {
            // Already disconnected
            return null;
        }

        // Convert this row into its associated object and save it
        E nextElement = rof.getObject();
        JDBCUtils.logWarnings(rs);
        resultObjs.add(nextElement);
        if (resultIds != null)
        {
            resultIds.add(api.getIdForObject(nextElement));
        }

        // Process any bulk loaded members
        if (bulkLoadedValueByMemberNumber != null)
        {
            Map<Integer, Object> memberValues = bulkLoadedValueByMemberNumber.get(api.getIdForObject(nextElement));
            if (memberValues != null)
            {
                ObjectProvider op = query.getExecutionContext().findObjectProvider(nextElement);
                Iterator<Map.Entry<Integer, Object>> memberValIter = memberValues.entrySet().iterator();
                while (memberValIter.hasNext())
                {
                    Map.Entry<Integer, Object> memberValueEntry = memberValIter.next();
                    op.replaceField(memberValueEntry.getKey(), memberValueEntry.getValue());
                }
                op.replaceAllLoadedSCOFieldsWithWrappers();
            }
        }

        // Update the status of whether there are more results outstanding
        if (rs == null)
        {
            throw new NucleusUserException("Results for query have already been closed. Perhaps you called flush(), closed the query, or ended a transaction");
        }
        try
        {
            moreResultSetRows = rs.next();
            if (applyRangeChecks)
            {
                // Check if we have reached the end of the range
                int maxElements = (int)(query.getRangeToExcl() - query.getRangeFromIncl());
                if (resultObjs.size() == maxElements)
                {
                    moreResultSetRows = false;
                }
            }

            if (!moreResultSetRows)
            {
                closeResults();
            }
        }
        catch (SQLException e)
        {
            throw api.getDataStoreExceptionForException(Localiser.msg("052601",e.getMessage()), e);
        }

        return nextElement;
    }

    /**
     * Internal method to close the ResultSet.
     */
    protected void closeResults()
    {
        if (rs == null)
        {
            // Results already closed
            return;
        }

        // Close ResultSet
        super.closeResults();

        if (resultIds != null)
        {
            // Cache the results with the QueryManager
            query.getQueryManager().addQueryResult(query, query.getInputParameters(), resultIds);
            resultIds = null;
        }

        // Disable range check since we have now loaded all results
        applyRangeChecks = false;
    }

    /**
     * Method to close the results, making the results unusable thereafter.
     */
    public synchronized void close()
    {
        moreResultSetRows = false;
        resultObjs.clear();
        if (resultIds != null)
        {
            resultIds.clear();
        }

        super.close();
    }

    /**
     * Method called to inform the query result that the connection is being closed so perform
     * any required operations now, or rest in peace.
     */
    protected void closingConnection()
    {
        if (loadResultsAtCommit && isOpen() && moreResultSetRows)
        {
            // Query connection closing message
            NucleusLogger.QUERY.debug(Localiser.msg("052606", query.toString()));

            try
            {
                // If we are still open navigate to the end of the ResultSet before it gets closed
                advanceToEndOfResultSet();
            }
            catch (RuntimeException re)
            {
                if (re instanceof NucleusUserException)
                {
                    // TODO Localise this message
                    // Log any exception - can get exceptions when maybe the user has specified an invalid result class etc
                    NucleusLogger.QUERY.warn("Exception thrown while loading remaining rows of query : " + re.getMessage());
                }
                else
                {
                    throw api.getUserExceptionForException("Exception thrown while loading remaining rows of query", re);
                }
            }
        }
    }

    // ---------------------- Implementation of List methods -------------------------

    /**
     * Accessor for an iterator for the results.
     * @return The iterator
     */
    public Iterator<E> iterator()
    {
        return new QueryResultIterator();
    }

    /**
     * Accessor for an iterator for the results.
     * @return The iterator
     */
    public ListIterator<E> listIterator()
    {
        return new QueryResultIterator();
    }

    /**
     * An Iterator results of a pm.query.execute().iterator()
     */
    private class QueryResultIterator extends AbstractQueryResultIterator<E>
    {
        private int nextRowNum = 0;

        /** hold the last element **/
        E currentElement = null;

        public boolean hasNext()
        {
            synchronized (ForwardQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasNext() on closed Query will return false
                    return false;
                }

                if (applyRangeChecks)
                {
                    int maxElements = (int)(query.getRangeToExcl() - query.getRangeFromIncl());
                    if (nextRowNum == maxElements)
                    {
                        moreResultSetRows = false;
                        closeResults();
                        return false;
                    }
                }

                if (nextRowNum < resultObjs.size())
                {
                    return true;
                }

                if (candidates != null && currentElement != null && !moreResultSetRows)
                {
                    return candidates.contains(currentElement);
                }

                return moreResultSetRows;
            }
        }

        public boolean hasPrevious()
        {
            // We only navigate in forward direction, but maybe could provide this method
            throw new UnsupportedOperationException("Not yet implemented");
        }

        public E next()
        {
            synchronized (ForwardQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling next() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(Localiser.msg("052600"));
                }
                if (candidates != null && currentElement != null)
                {
                    // Allow for candidate collections with dups, so we can return the same element multiple times
                    // This assumes that there is a single object returned of candidate type
                    if (candidates.remove(currentElement))
                    {
                        // Returning candidate type and candidates has dup elements, so return it til we exhaust candidates
                        resultObjs.add(currentElement);
                        return currentElement;
                    }
                }

                if (nextRowNum < resultObjs.size())
                {
                    currentElement = resultObjs.get(nextRowNum);
                    ++nextRowNum;
                    return currentElement;
                }
                else if (moreResultSetRows)
                {
                    currentElement = nextResultSetElement();
                    ++nextRowNum;
                    if (candidates != null)
                    {
                        // Remove from the candidates since we have processed it now
                        // This assumes that there is a single object returned of candidate type
                        candidates.remove(currentElement);
                    }

                    return currentElement;
                }
                throw new NoSuchElementException(Localiser.msg("052602"));
            }
        }

        public int nextIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        public E previous()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        public int previousIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    /**
     * Method to check if the specified object is contained in this result.
     * @param o The object
     * @return Whether it is contained here.
     */
    public synchronized boolean contains(Object o)
    {
        assertIsOpen();
        advanceToEndOfResultSet();

        return resultObjs.contains(o);
    }

    /**
     * Method to check if all of the specified objects are contained here.
     * @param c The collection of objects
     * @return Whether they are all contained here.
     */
    public synchronized boolean containsAll(Collection c)
    {
        assertIsOpen();
        advanceToEndOfResultSet();

        return resultObjs.containsAll(c);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof ForwardQueryResult))
        {
            return false;
        }
        return super.equals(o);
    }

    public int hashCode()
    {
        return super.hashCode();
    }

    /**
     * Method to retrieve a particular element from the list.
     * @param index The index of the element
     * @return The element at index
     */
    public synchronized E get(int index)
    {
        assertIsOpen();

        // Load rest of results (is this necessary, if we already have the element we don't need more)
        advanceToEndOfResultSet();
        if (index < 0 || index >= resultObjs.size())
        {
            throw new IndexOutOfBoundsException();
        }

        return resultObjs.get(index);
    }

    /**
     * Accessor for whether there are any results.
     * @return <tt>true</tt> if these results are empty.
     */
    public synchronized boolean isEmpty()
    {
        assertIsOpen();

        return resultObjs.isEmpty() && !moreResultSetRows;
    }

    /**
     * Method to get the size using the "resultSizeMethod".
     * This implementation supports "LAST" method. Override this in subclasses to implement other methods.
     * @return The size
     */
    protected int getSizeUsingMethod()
    {
        if (resultSizeMethod.equalsIgnoreCase("LAST"))
        {
            advanceToEndOfResultSet();
            return resultObjs.size();
        }
        return super.getSizeUsingMethod();
    }

    /**
     * Method to return the results as an array.
     * @return The array.
     */
    public synchronized Object[] toArray()
    {
        assertIsOpen();
        advanceToEndOfResultSet();

        return resultObjs.toArray();
    }

    /**
     * Method to return the results as an array.
     * @param a The array to copy into. 
     * @return The array.
     */
    public synchronized Object[] toArray(Object[] a)
    {
        assertIsOpen();
        advanceToEndOfResultSet();

        return resultObjs.toArray(a);
    }

    /**
     * Handle serialisation by returning a java.util.ArrayList of all of the results for this query
     * after disconnecting the query which has the consequence of enforcing the load of all objects.
     * @return The object to serialise
     * @throws ObjectStreamException Thrown if an error occurs
     */
    protected Object writeReplace() throws ObjectStreamException
    {
        disconnect();
        return new java.util.ArrayList(resultObjs);
    }
}