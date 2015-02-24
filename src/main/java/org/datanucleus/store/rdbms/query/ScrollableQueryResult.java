/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
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
2005 Andy Jefferson - added support for bidirectional iterator
2008 Andy Jefferson - added resultCacheType. Removed optimistic restriction
2008 Marco Schulze - implemented toArray() and toArray(Object[])
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.query;

import java.io.ObjectStreamException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.query.AbstractQueryResultIterator;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.SoftValueMap;
import org.datanucleus.util.WeakValueMap;

/**
 * Lazy collection results from a Query with the ResultSet scrollable.
 * Supports the following query extensions (in addition to those supported by superclasses) :-
 * <ul>
 * <li><b>datanucleus.query.resultCacheType</b> Type of caching of result objects.
 * Supports strong, weak, soft, none</li>
 * </ul>
 * If there is no transaction present, or if the FetchPlan is in "greedy" mode, and where caching is being used
 * will load all results at startup. Otherwise results are only loaded when accessed.
 */
public final class ScrollableQueryResult extends AbstractRDBMSQueryResult implements java.io.Serializable
{
    /** Map of ResultSet object values, keyed by the list index ("0", "1", etc). */
    private Map<Integer, Object> resultsObjsByIndex = null;

    protected Map<Integer, Object> resultIds = null;

    /** Position of first result (origin=0). */
    int startIndex = 0;
    /** Position of last result (origin=0, set when known). */
    int endIndex = -1;

    boolean applyRangeChecks = false;

    /**
     * Constructor of the result from a Query.
     * @param query The Query
     * @param rof The factory to retrieve results from
     * @param rs The ResultSet from the Query Statement
     * @param candidates the Candidates collection. Pass this argument only when distinct = false
     */
    public ScrollableQueryResult(Query query, ResultObjectFactory rof, ResultSet rs, Collection candidates)
    {
        super(query, rof, rs);

        if (candidates != null)
        {
            //TODO support this feature
            throw new NucleusException("Unsupported Feature: Candidate Collection is only allowed using ForwardQueryResult").setFatal();
        }

        if (query.useResultsCaching())
        {
            resultIds = new HashMap();
        }

        // Process any supported extensions
        String ext = (String)query.getExtension(Query.EXTENSION_RESULT_CACHE_TYPE);
        if (ext != null)
        {
            if (ext.equalsIgnoreCase("soft"))
            {
                resultsObjsByIndex = new SoftValueMap();
            }
            else if (ext.equalsIgnoreCase("weak"))
            {
                resultsObjsByIndex = new WeakValueMap();
            }
            else if (ext.equalsIgnoreCase("strong"))
            {
                resultsObjsByIndex = new HashMap();
            }
            else if (ext.equalsIgnoreCase("none"))
            {
                resultsObjsByIndex = null;
            }
            else
            {
                resultsObjsByIndex = new WeakValueMap();
            }
        }
        else
        {
            resultsObjsByIndex = new WeakValueMap();
        }

        applyRangeChecks = !query.processesRangeInDatastoreQuery();
        if (applyRangeChecks)
        {
            startIndex = (int) query.getRangeFromIncl();
        }
    }

    public void initialise()
    {
        if (resultsObjsByIndex != null)
        {
            // Caching results so load up any result objects needed right now
            int fetchSize = query.getFetchPlan().getFetchSize();
            if (fetchSize == FetchPlan.FETCH_SIZE_GREEDY)
            {
                // "greedy" mode, so load all results now
                loadObjects(startIndex, -1);

                // Cache the query results
                cacheQueryResults();
            }
            else if (fetchSize > 0)
            {
                // Load up the first "fetchSize" objects now
                loadObjects(startIndex, fetchSize);
            }
        }
    }

    /**
     * Convenience method to load up rows starting at the specified position.
     * Optionally takes a maximum number of rows to process.
     * @param start Start row
     * @param maxNumber Max number to process (-1 means no maximum)
     */
    protected void loadObjects(int start, int maxNumber)
    {
        int index = start;
        boolean hasMoreResults = true;
        while (hasMoreResults)
        {
            if (maxNumber >= 0 && index == (maxNumber+start))
            {
                // Maximum specified, and already loaded the required number of results
                hasMoreResults = false;
            }
            else if (applyRangeChecks && index >= query.getRangeToExcl())
            {
                // Reached end of allowed range
                size = (int) (query.getRangeToExcl()-query.getRangeFromIncl());
                hasMoreResults = false;
            }
            else
            {
                try
                {
                    boolean rowExists = rs.absolute(index+1);
                    if (!rowExists)
                    {
                        hasMoreResults = false;
                        size = index; // We know the size now so store for later use
                        if (applyRangeChecks && index < query.getRangeToExcl())
                        {
                            size = (int) (index - query.getRangeFromIncl());
                        }
                        endIndex = index-1;
                    }
                    else
                    {
                        getObjectForIndex(index);
                        index++;
                    }
                }
                catch (SQLException sqle)
                {
                    // TODO Handle this
                }
            }
        }
    }

    /**
     * Accessor for the result object at an index.
     * If the object has already been processed will return that object,
     * otherwise will retrieve the object using the factory.
     * @param index The list index position
     * @return The result object
     */
    protected Object getObjectForIndex(int index)
    {
        if (resultsObjsByIndex != null)
        {
            // Caching objects, so check the cache for this index
            Object obj = resultsObjsByIndex.get(index);
            if (obj != null)
            {
                // Already retrieved so return it
                return obj;
            }
        }

        if (rs == null)
        {
            throw new NucleusUserException("Results for query have already been closed. Perhaps you called flush(), closed the query, or ended a transaction");
        }
        try
        {
            // ResultSet is numbered 1, 2, ... N
            // List is indexed 0, 1, 2, ... N-1
            rs.absolute(index+1);
            Object obj = rof.getObject(query.getExecutionContext(), rs);
            JDBCUtils.logWarnings(rs);

            // Process any bulk loaded members
            if (bulkLoadedValueByMemberNumber != null)
            {
                ExecutionContext ec = query.getExecutionContext();
                Map<Integer, Object> memberValues = bulkLoadedValueByMemberNumber.get(ec.getApiAdapter().getIdForObject(obj));
                if (memberValues != null)
                {
                    ObjectProvider op = ec.findObjectProvider(obj);
                    Iterator<Map.Entry<Integer, Object>> memberValIter = memberValues.entrySet().iterator();
                    while (memberValIter.hasNext())
                    {
                        Map.Entry<Integer, Object> memberValueEntry = memberValIter.next();
                        op.replaceField(memberValueEntry.getKey(), memberValueEntry.getValue());
                    }
                    op.replaceAllLoadedSCOFieldsWithWrappers();
                }
            }

            if (resultsObjsByIndex != null)
            {
                // Put it in our cache, keyed by the list index
                resultsObjsByIndex.put(index, obj);
                if (resultIds != null)
                {
                    resultIds.put(index, query.getExecutionContext().getApiAdapter().getIdForObject(obj));
                }
            }

            return obj;
        }
        catch (SQLException sqe)
        {
            throw query.getExecutionContext().getApiAdapter().getDataStoreExceptionForException(
                Localiser.msg("052601", sqe.getMessage()), sqe);
        }
    }

    /**
     * Method to close the results, making the results unusable thereafter.
     */
    public synchronized void close()
    {
        if (resultsObjsByIndex != null)
        {
            resultsObjsByIndex.clear();
        }

        super.close();
    }

    /**
     * Inform the query result that the connection is being closed so perform
     * any operations now, or rest in peace.
     */
    protected void closingConnection()
    {
        // Make sure all rows are loaded.
        if (loadResultsAtCommit && isOpen())
        {
            // Query connection closing message
            NucleusLogger.QUERY.info(Localiser.msg("052606", query.toString()));

            if (endIndex < 0)
            {
                endIndex = size()-1;
                if (applyRangeChecks)
                {
                    endIndex = (int) query.getRangeToExcl()-1;
                }
            }
            for (int i=startIndex;i<endIndex+1;i++)
            {
                getObjectForIndex(i);
            }

            // Cache the query results
            cacheQueryResults();
        }
    }

    protected void cacheQueryResults()
    {
        if (resultIds != null)
        {
            List ids = new ArrayList();
            Iterator<Integer> resultIdPositionIter = resultIds.keySet().iterator();
            while (resultIdPositionIter.hasNext())
            {
                Integer position = resultIdPositionIter.next();
                Object resultId = resultIds.get(position);
                ids.add(resultId);
            }
            query.getQueryManager().addQueryResult(query, query.getInputParameters(), ids);
        }
        resultIds = null;
    }

    // ------------------------- Implementation of List methods ----------------------

    /**
     * Accessor for an iterator for the results.
     * @return The iterator
     */
    public Iterator iterator()
    {
        return new QueryResultIterator();
    }

    /**
     * Accessor for an iterator for the results.
     * @return The iterator
     */
    public ListIterator listIterator()
    {
        return new QueryResultIterator();
    }

    /**
     * An Iterator results of a pm.query.execute().iterator()
     */
    private class QueryResultIterator extends AbstractQueryResultIterator
    {
        private int iterRowNum = 0; // The index of the next object

        public QueryResultIterator()
        {
            super();
            if (applyRangeChecks)
            {
                // Skip the first x rows as per any range specification
                iterRowNum = (int) query.getRangeFromIncl();
            }
        }

        public boolean hasNext()
        {
            synchronized (ScrollableQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasNext() on closed Query will return false
                    return false;
                }

                int theSize = size();
                if (applyRangeChecks)
                {
                    if (theSize < query.getRangeToExcl()-query.getRangeFromIncl())
                    {
                        // Size reached before upper limit of range
                        return iterRowNum <= (query.getRangeFromIncl() + theSize - 1);
                    }

                    if (iterRowNum == query.getRangeToExcl()-1)
                    {
                        endIndex = iterRowNum;
                    }
                    // When we are at "query.getRangeToExcl()-1" we have 1 more element
                    return (iterRowNum <= (query.getRangeToExcl()-1));
                }

                // When we are at at "size()-1" we have one more element
                return (iterRowNum <= (theSize - 1));
            }
        }

        public boolean hasPrevious()
        {
            synchronized (ScrollableQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasPrevious() on closed Query will return false
                    return false;
                }

                if (applyRangeChecks)
                {
                    // row at first of range means no earlier
                    return (iterRowNum > query.getRangeFromIncl());
                }

                // row 0 means no earlier
                return (iterRowNum > 0);
            }
        }

        public Object next()
        {
            synchronized (ScrollableQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling next() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(Localiser.msg("052600"));
                }

                if (!hasNext())
                {
                    throw new NoSuchElementException("No next element");
                }

                Object obj = getObjectForIndex(iterRowNum);
                iterRowNum++;

                return obj;
            }
        }

        public int nextIndex()
        {
            if (hasNext())
            {
                if (applyRangeChecks)
                {
                    return iterRowNum - (int) query.getRangeFromIncl();
                }
                return iterRowNum;
            }
            return size();
        }

        public Object previous()
        {
            synchronized (ScrollableQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling previous() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(Localiser.msg("052600"));
                }

                if (!hasPrevious())
                {
                    throw new NoSuchElementException("No previous element");
                }

                iterRowNum--;
                return getObjectForIndex(iterRowNum);
            }
        }

        public int previousIndex()
        {
            if (applyRangeChecks)
            {
                return (int) (iterRowNum == query.getRangeFromIncl() ? -1 : iterRowNum-query.getRangeFromIncl()-1);
            }
            return (iterRowNum == 0 ? -1 : iterRowNum-1);
        }
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof ScrollableQueryResult))
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
    public synchronized Object get(int index)
    {
        assertIsOpen();
        if (index < 0 || index >= size())
        {
            throw new IndexOutOfBoundsException();
        }
        return getObjectForIndex(index + startIndex);
    }

    /**
     * Method to get the size using the "resultSizeMethod".
     * This implementation supports "LAST" method. Override this in subclasses to implement
     * other methods.
     * @return The size
     */
    protected int getSizeUsingMethod()
    {
        int theSize = 0;
        if (resultSizeMethod.equalsIgnoreCase("LAST"))
        {
            if (rs == null)
            {
                throw new NucleusUserException("Results for query have already been closed. Perhaps you called flush(), closed the query, or ended a transaction");
            }
            try
            {
                boolean hasLast = rs.last();
                if (!hasLast)
                {
                    // No results in ResultSet
                    theSize = 0;
                }
                else
                {
                    // ResultSet has results so return the row number as the list size
                    theSize = rs.getRow();
                }
            }
            catch (SQLException sqle)
            {
                throw query.getExecutionContext().getApiAdapter().getDataStoreExceptionForException(Localiser.msg("052601", sqle.getMessage()), sqle);
            }

            if (applyRangeChecks)
            {
                // Adjust the calculated size for any range specification
                if (theSize > query.getRangeToExcl())
                {
                    endIndex = (int) (query.getRangeToExcl()-1);
                    theSize = (int) (query.getRangeToExcl() - query.getRangeFromIncl());
                }
                else
                {
                    endIndex = theSize-1;
                    theSize = (int) (theSize - query.getRangeFromIncl());
                }
            }
        }
        else
        {
            theSize = super.getSizeUsingMethod();
        }

        return theSize;
    }

    public Object[] toArray()
    {
        return toArrayInternal(null);
    }

    public Object[] toArray(Object[] a)
    {
        if (a == null)
        {
            // ArrayList.toArray(Object[]) does not allow null arguments, so we don't do this either (according to javadoc, a NPE is thrown).
            throw new NullPointerException("null argument is illegal!");
        }

        return toArrayInternal(a);
    }

    private Object[] toArrayInternal(Object[] a)
    {
        Object[] result = a;
        ArrayList resultList = null;

        int size = -1;
        try
        {
            size = size();
        }
        catch (Exception x)
        {
            // silently (except for debugging) ignore and work with unknown size
            size = -1;
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug("toArray: Could not determine size.", x);
            }
        }

        if (size >= 0)
        {
            if (result == null || result.length < size)
            {
                // if the size is known and exceeds the array length, we use a list instead of populating the array directly
                result = null;
                resultList = new ArrayList(size);
            }
        }

        Iterator iterator = null;

        // trying to use the existing result array before creating sth. new
        if (result != null)
        {
            iterator = this.iterator();
            int idx = -1;
            while (iterator.hasNext())
            {
                ++idx;
                if (idx < result.length)
                {
                    result[idx] = iterator.next();
                }
                else
                {
                    // exceeding array size => switch to resultList
                    int capacity = (result.length * 3) / 2 + 1;
                    if (capacity < result.length)
                    {
                        // this could only happen, if the above calculation exceeds the integer range - but safer is better
                        capacity = result.length;
                    }

                    resultList = new ArrayList(capacity);
                    for (int i = 0; i < result.length; i++)
                    {
                        resultList.add(result[i]);
                    }
                    result = null;
                    break;
                }
            }
            ++idx;
            if (result != null && idx < result.length)
            {
                // it's a convention that the first element in the array after the real data is null (ArrayList.toArray(Object[]) does the same)
                result[idx] = null;
            }
        }

        // if the result is null, it means that using the existing array was not possible or there was none passed.
        if (result == null)
        {
            if (resultList == null)
            {
                resultList = new ArrayList();
            }

            if (iterator == null)
            {
                // the iterator might already exist (if we tried to use the result array and saw that it is not long enough)
                iterator = this.iterator();
            }

            while (iterator.hasNext())
            {
                resultList.add(iterator.next());
            }

            result = (a == null) ? resultList.toArray() : resultList.toArray(a);
        }

        return result;
    }

    /**
     * Handle serialisation by returning a java.util.ArrayList of all of the results for this query
     * after disconnecting the query which has the consequence of enforcing the load of all objects.
     * @return The object to serialise
     * @throws ObjectStreamException thrown if an error occurs
     */
    protected Object writeReplace() throws ObjectStreamException
    {
        disconnect();
        List results = new java.util.ArrayList();
        for (int i=0;i<resultsObjsByIndex.size();i++)
        {
            results.add(resultsObjsByIndex.get(i));
        }
        return results;
    }
}