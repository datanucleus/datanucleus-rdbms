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
package org.datanucleus.store.rdbms.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.scostore.ElementContainerStore;
import org.datanucleus.store.rdbms.scostore.IteratorStatement;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Abstract representation of a QueryResult for RDBMS queries.
 * Based on the assumption that we have a JDBC ResultSet, and we are extracting the results using a ResultObjectFactory.
 */
public abstract class AbstractRDBMSQueryResult<E> extends AbstractQueryResult<E>
{
    private static final long serialVersionUID = 7264180157109169910L;

    /** The ResultSet containing the results. */
    protected ResultSet rs;

    /** ResultObjectFactory for converting the result set into objects. */
    protected ResultObjectFactory<E> rof;

    /** Map of field values, keyed by the "id" of the object. The value is a "Map&lt;fieldNumber, fieldValue&gt;". */
    protected Map<Object, Map<Integer, Object>> bulkLoadedValueByMemberNumber;

    /** Default to closing the statement when closing the resultSet, but allow override. */
    protected boolean closeStatementWithResultSet = true;

    boolean applyRangeChecks = false;

    /**
     * Constructor of the result from a Query.
     * @param query The Query
     * @param rof The factory to retrieve results from
     * @param rs The ResultSet from the Query Statement
     */
    public AbstractRDBMSQueryResult(Query query, ResultObjectFactory<E> rof, ResultSet rs)
    {
        super(query);
        this.rof = rof;
        this.rs = rs;

        this.applyRangeChecks = !query.processesRangeInDatastoreQuery();
    }

    public void setCloseStatementWithResultSet(boolean flag)
    {
        this.closeStatementWithResultSet = flag;
    }

    public void registerMemberBulkResultSet(IteratorStatement iterStmt, ResultSet rs)
    {
        if (bulkLoadedValueByMemberNumber == null)
        {
            bulkLoadedValueByMemberNumber = new HashMap<>();
        }

        try
        {
            ExecutionContext ec = query.getExecutionContext();
            AbstractMemberMetaData mmd = iterStmt.getBackingStore().getOwnerMemberMetaData();
            if (mmd.hasCollection() || mmd.hasArray())
            {
                ElementContainerStore backingStore = (ElementContainerStore) iterStmt.getBackingStore();
                if (backingStore.isElementsAreEmbedded() || backingStore.isElementsAreSerialised())
                {
                    int param[] = new int[backingStore.getElementMapping().getNumberOfDatastoreMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }

                    if (backingStore.getElementMapping() instanceof SerialisedPCMapping ||
                        backingStore.getElementMapping() instanceof SerialisedReferenceMapping ||
                        backingStore.getElementMapping() instanceof EmbeddedElementPCMapping)
                    {
                        // Element = Serialised
                        while (rs.next())
                        {
                            Object owner = iterStmt.getOwnerMapIndex().getMapping().getObject(ec, rs, iterStmt.getOwnerMapIndex().getColumnPositions());
                            Object element = backingStore.getElementMapping().getObject(ec, rs, param, ec.findObjectProvider(owner), 
                                backingStore.getOwnerMemberMetaData().getAbsoluteFieldNumber());
                            addOwnerMemberValue(mmd, owner, element);
                        }
                    }
                    else
                    {
                        // Element = Non-PC
                        while (rs.next())
                        {
                            Object owner = iterStmt.getOwnerMapIndex().getMapping().getObject(ec, rs, iterStmt.getOwnerMapIndex().getColumnPositions());
                            Object element = backingStore.getElementMapping().getObject(ec, rs, param);
                            addOwnerMemberValue(mmd, owner, element);
                        }
                    }
                }
                else if (backingStore.getElementMapping() instanceof ReferenceMapping)
                {
                    // Element is Reference (interface/Object) so just use elementMapping
                    int param[] = new int[backingStore.getElementMapping().getNumberOfDatastoreMappings()];
                    for (int i = 0; i < param.length; ++i)
                    {
                        param[i] = i + 1;
                    }
                    while (rs.next())
                    {
                        Object owner = iterStmt.getOwnerMapIndex().getMapping().getObject(ec, rs, iterStmt.getOwnerMapIndex().getColumnPositions());
                        Object element = backingStore.getElementMapping().getObject(ec, rs, param);
                        addOwnerMemberValue(mmd, owner, element);
                    }
                }
                else
                {
                    String elementType = mmd.hasCollection() ? 
                            backingStore.getOwnerMemberMetaData().getCollection().getElementType() : backingStore.getOwnerMemberMetaData().getArray().getElementType();
                    ResultObjectFactory<E> scoROF = new PersistentClassROF(ec, rs, query.getIgnoreCache(),
                        iterStmt.getStatementClassMapping(), backingStore.getElementClassMetaData(), ec.getClassLoaderResolver().classForName(elementType));
                    while (rs.next())
                    {
                        Object owner = iterStmt.getOwnerMapIndex().getMapping().getObject(ec, rs, iterStmt.getOwnerMapIndex().getColumnPositions());
                        Object element = scoROF.getObject();
                        addOwnerMemberValue(mmd, owner, element);
                    }
                }
            }
            else if (mmd.hasMap())
            {
                // TODO Cater for maps
            }
        }
        catch (SQLException sqle)
        {
            NucleusLogger.DATASTORE.error("Exception thrown processing bulk loaded field " + iterStmt.getBackingStore().getOwnerMemberMetaData().getFullFieldName(), sqle);
        }
        finally
        {
            // Close the ResultSet (and its Statement)
            try
            {
                Statement stmt = null;
                try
                {
                    stmt = rs.getStatement();

                    // Close the result set
                    rs.close();
                }
                catch (SQLException e)
                {
                    NucleusLogger.DATASTORE.error(Localiser.msg("052605",e));
                }
                finally
                {
                    try
                    {
                        if (stmt != null)
                        {
                            // Close the original statement
                            stmt.close();
                        }
                    }
                    catch (SQLException e)
                    {
                        // Do nothing
                    }
                }
            }
            finally
            {
                rs = null;
            }
        }
    }

    public abstract void initialise()
    throws SQLException;

    private void addOwnerMemberValue(AbstractMemberMetaData mmd, Object owner, Object element)
    {
        Object ownerId = api.getIdForObject(owner);
        Map<Integer, Object> fieldValuesForOwner = bulkLoadedValueByMemberNumber.get(ownerId);
        if (fieldValuesForOwner == null)
        {
            fieldValuesForOwner = new HashMap<>();
            bulkLoadedValueByMemberNumber.put(ownerId, fieldValuesForOwner);
        }
        Collection coll = (Collection) fieldValuesForOwner.get(mmd.getAbsoluteFieldNumber());
        if (coll == null)
        {
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.newInstance();
                fieldValuesForOwner.put(mmd.getAbsoluteFieldNumber(), coll);
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
        }
        coll.add(element);
    }

    /**
     * Method to disconnect the results from the ExecutionContext, meaning that thereafter it just behaves
     * like a List. All remaining results are read in at this point (unless selected not to be).
     */
    public void disconnect()
    {
        if (query == null)
        {
            // Already disconnected
            return;
        }

        super.disconnect();
        rof = null;
        rs = null;
    }

    /**
     * Method to close the results, meaning that they are inaccessible after this point.
     */
    public synchronized void close()
    {
        super.close();
        rof = null;
        rs = null;
    }

    /**
     * Internal method to close the ResultSet.
     */
    protected void closeResults()
    {
        if (rs != null)
        {
            try
            {
                Statement stmt = null;
                try
                {
                    stmt = rs.getStatement();

                    // Close the result set
                    rs.close();
                }
                catch (SQLException e)
                {
                    NucleusLogger.DATASTORE.error(Localiser.msg("052605",e));
                }
                finally
                {
                    try
                    {
                        if (closeStatementWithResultSet && stmt != null)
                        {
                            // Close the original statement
                            stmt.close();
                        }
                    }
                    catch (SQLException e)
                    {
                        // Do nothing
                    }
                }
            }
            finally
            {
                rs = null;
            }
        }
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof AbstractRDBMSQueryResult))
        {
            return false;
        }

        AbstractRDBMSQueryResult other = (AbstractRDBMSQueryResult)o;
        if (rs != null)
        {
            return other.rs == rs;
        }
        else if (query != null)
        {
            return other.query == query;
        }
        return StringUtils.toJVMIDString(other).equals(StringUtils.toJVMIDString(this));
    }

    public int hashCode()
    {
        if (rs != null)
        {
            return rs.hashCode();
        }
        else if (query != null)
        {
            return query.hashCode();
        }
        return StringUtils.toJVMIDString(this).hashCode();
    }
}