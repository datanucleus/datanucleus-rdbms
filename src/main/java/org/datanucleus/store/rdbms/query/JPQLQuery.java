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
package org.datanucleus.store.rdbms.query;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlanForClass;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.query.evaluator.JPQLEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.AbstractContainerMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.store.query.CandidateIdsQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.QueryInterruptedException;
import org.datanucleus.store.query.QueryManager;
import org.datanucleus.store.query.QueryResult;
import org.datanucleus.store.query.QueryTimeoutException;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.scostore.IteratorStatement;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * RDBMS representation of a JPQL query for use by DataNucleus.
 * The query can be specified via method calls, or via a single-string form.
 * This implementation uses the generic query compilation in "org.datanucleus.query".
 * There are the following main ways of running a query here
 * <ul>
 * <li>Totally in the datastore (no candidate collection specified, and no in-memory eval).</li>
 * <li>Totally in-memory (candidate collection specified, and in-memory eval)</li>
 * <li>Retrieve candidates from datastore (no candidate collection), and evaluate in-memory</li>
 * </ul>
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    private static final long serialVersionUID = -3735379324740714088L;

    /** Extension for whether to convert "== ?" with null parameter to "IS NULL". Defaults to false to comply with JPA spec 4.11. */
    public static final String EXTENSION_USE_IS_NULL_WHEN_EQUALS_NULL_PARAM = "datanucleus.useIsNullWhenEqualsNullParameter";

    /** Extension to add NOWAIT when using FOR UPDATE (when supported). */
    public static final String EXTENSION_FOR_UPDATE_NOWAIT = "datanucleus.forUpdateNowait";

    /** The compilation of the query for this datastore. Not applicable if totally in-memory. */
    protected transient RDBMSQueryCompilation datastoreCompilation;

    /**
     * Constructs a new query instance that uses the given object manager.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec The ExecutionContext
     * @param query The single-string query form
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    @Override
    public void setImplicitParameter(int position, Object value)
    {
        if (datastoreCompilation != null && !datastoreCompilation.isPrecompilable())
        {
            // Force recompile since parameter value set and not compilable without parameter values
            datastoreCompilation = null;
        }
        super.setImplicitParameter(position, value);
    }

    @Override
    public void setImplicitParameter(String name, Object value)
    {
        if (datastoreCompilation != null && !datastoreCompilation.isPrecompilable())
        {
            // Force recompile since parameter value set and not compilable without parameter values
            datastoreCompilation = null;
        }
        super.setImplicitParameter(name, value);
    }

    /**
     * Utility to remove any previous compilation of this Query.
     */
    protected void discardCompiled()
    {
        super.discardCompiled();

        datastoreCompilation = null;
    }

    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    protected boolean isCompiled()
    {
        if (candidateCollection != null)
        {
            // Don't need datastore compilation here since evaluating in-memory
            return compilation != null;
        }

        // Need both to be present to say "compiled"
        if (compilation == null || datastoreCompilation == null)
        {
            return false;
        }
        if (!datastoreCompilation.isPrecompilable())
        {
            NucleusLogger.GENERAL.info("Query compiled but not precompilable so ditching datastore compilation");
            datastoreCompilation = null;
            return false;
        }
        return true;
    }

    /**
     * Method to get key for query cache
     * @return The cache key
     */
    protected String getQueryCacheKey()
    {
        if (getSerializeRead() != null && getSerializeRead())
        {
            return super.getQueryCacheKey() + " FOR UPDATE";
        }
        return super.getQueryCacheKey();
    }

    /**
     * Method to compile the JPQL query.
     * Uses the superclass to compile the generic query populating the "compilation", and then generates
     * the datastore-specific "datastoreCompilation".
     * @param parameterValues Map of param values keyed by param name (if available at compile time)
     */
    protected synchronized void compileInternal(Map parameterValues)
    {
        if (isCompiled())
        {
            return;
        }

        // Compile the generic query expressions
        super.compileInternal(parameterValues);

        boolean inMemory = evaluateInMemory();
        if (candidateCollection != null)
        {
            // Everything done in-memory so just return now (don't need datastore compilation)
            // TODO Maybe apply the result class checks ?
            return;
        }

        if (candidateClass == null || candidateClassName == null)
        {
            candidateClass = compilation.getCandidateClass();
            candidateClassName = candidateClass.getName();
        }

        // Create the SQL statement, and its result/parameter definitions
        RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
        QueryManager qm = getQueryManager();
        String datastoreKey = storeMgr.getQueryCacheKey();
        String queryCacheKey = getQueryCacheKey();

        if (useCaching() && queryCacheKey != null)
        {
            // Check if we have any parameters set to null, since this can invalidate a datastore compilation
            // e.g " field == :val" can be "COL IS NULL" or "COL = <val>"
            boolean nullParameter = false;
            if (parameterValues != null)
            {
                Iterator iter = parameterValues.values().iterator();
                while (iter.hasNext())
                {
                    Object val = iter.next();
                    if (val == null)
                    {
                        nullParameter = true;
                        break;
                    }
                }
            }

            if (!nullParameter)
            {
                // Allowing caching so try to find compiled (datastore) query
                datastoreCompilation = (RDBMSQueryCompilation)qm.getDatastoreQueryCompilation(datastoreKey, getLanguage(), queryCacheKey);
                if (datastoreCompilation != null)
                {
                    // Cached compilation exists for this datastore so reuse it
                    return;
                }
            }
        }

        // No cached compilation for this query in this datastore so compile it
        AbstractClassMetaData acmd = getCandidateClassMetaData();
        if (type == Query.BULK_UPDATE)
        {
            datastoreCompilation = new RDBMSQueryCompilation();
            compileQueryUpdate(parameterValues, acmd);
        }
        else if (type == Query.BULK_DELETE)
        {
            datastoreCompilation = new RDBMSQueryCompilation();
            compileQueryDelete(parameterValues, acmd);
        }
        else
        {
            datastoreCompilation = new RDBMSQueryCompilation();
            if (inMemory)
            {
                // Generate statement to just retrieve all candidate objects for later processing
                compileQueryToRetrieveCandidates(parameterValues, acmd);
            }
            else
            {
                // Generate statement to perform the full query in the datastore
                compileQueryFull(parameterValues, acmd);

                if (result != null)
                {
                    StatementResultMapping resultMapping = datastoreCompilation.getResultDefinition();
                    for (int i=0;i<resultMapping.getNumberOfResultExpressions();i++)
                    {
                        Object stmtMap = resultMapping.getMappingForResultExpression(i);
                        if (stmtMap instanceof StatementMappingIndex)
                        {
                            StatementMappingIndex idx = (StatementMappingIndex)stmtMap;
                            AbstractMemberMetaData mmd = idx.getMapping().getMemberMetaData();
                            if (mmd != null)
                            {
                                if (idx.getMapping() instanceof AbstractContainerMapping && idx.getMapping().getNumberOfDatastoreMappings() != 1)
                                {
                                    throw new NucleusUserException(Localiser.msg("021213"));
                                }
                            }
                        }
                    }
                }
            }

            if (resultClass != null && result != null)
            {
                // Do as PrivilegedAction since uses reflection
                AccessController.doPrivileged(new PrivilegedAction()
                {
                    public Object run()
                    {
                        // Check that this class has the necessary constructor/setters/fields to be used
                        StatementResultMapping resultMapping = datastoreCompilation.getResultDefinition();
                        if (QueryUtils.resultClassIsSimple(resultClass.getName()))
                        {
                            if (resultMapping.getNumberOfResultExpressions() > 1)
                            {
                                // Invalid number of result expressions
                                throw new NucleusUserException(Localiser.msg("021201", resultClass.getName()));
                            }

                            Object stmtMap = resultMapping.getMappingForResultExpression(0);
                            // TODO Handle StatementNewObjectMapping
                            StatementMappingIndex idx = (StatementMappingIndex)stmtMap;
                            Class exprType = idx.getMapping().getJavaType();
                            boolean typeConsistent = false;
                            if (exprType == resultClass)
                            {
                                typeConsistent = true;
                            }
                            else if (exprType.isPrimitive())
                            {
                                Class resultClassPrimitive = ClassUtils.getPrimitiveTypeForType(resultClass);
                                if (resultClassPrimitive == exprType)
                                {
                                    typeConsistent = true;
                                }
                            }
                            if (!typeConsistent)
                            {
                                // Inconsistent expression type not matching the result class type
                                throw new NucleusUserException(Localiser.msg("021202", resultClass.getName(), exprType));
                            }
                        }
                        else if (QueryUtils.resultClassIsUserType(resultClass.getName()))
                        {
                            // Check for valid constructor (either using param types, or using default ctr)
                            Class[] ctrTypes = new Class[resultMapping.getNumberOfResultExpressions()];
                            for (int i=0;i<ctrTypes.length;i++)
                            {
                                Object stmtMap = resultMapping.getMappingForResultExpression(i);
                                if (stmtMap instanceof StatementMappingIndex)
                                {
                                    ctrTypes[i] = ((StatementMappingIndex)stmtMap).getMapping().getJavaType();
                                }
                                else if (stmtMap instanceof StatementNewObjectMapping)
                                {
                                    // TODO Handle this
                                }
                            }
                            Constructor ctr = ClassUtils.getConstructorWithArguments(resultClass, ctrTypes);
                            if (ctr == null && !ClassUtils.hasDefaultConstructor(resultClass))
                            {
                                // No valid constructor found!
                                throw new NucleusUserException(Localiser.msg("021205", resultClass.getName()));
                            }
                            else if (ctr == null)
                            {
                                // We are using default constructor, so check the types of the result expressions for means of input
                                for (int i=0;i<resultMapping.getNumberOfResultExpressions();i++)
                                {
                                    Object stmtMap = resultMapping.getMappingForResultExpression(i);
                                    if (stmtMap instanceof StatementMappingIndex)
                                    {
                                        StatementMappingIndex mapIdx = (StatementMappingIndex)stmtMap;
                                        AbstractMemberMetaData mmd = mapIdx.getMapping().getMemberMetaData();
                                        String fieldName = mapIdx.getColumnAlias();
                                        Class fieldType = mapIdx.getMapping().getJavaType();
                                        if (fieldName == null && mmd != null)
                                        {
                                            fieldName = mmd.getName();
                                        }

                                        if (fieldName != null)
                                        {
                                            // Check for the field of that name in the result class
                                            Class resultFieldType = null;
                                            boolean publicField = true;
                                            try
                                            {
                                                Field fld = resultClass.getDeclaredField(fieldName);
                                                resultFieldType = fld.getType();

                                                // Check the type of the field
                                                if (!ClassUtils.typesAreCompatible(fieldType, resultFieldType) && !ClassUtils.typesAreCompatible(resultFieldType, fieldType))
                                                {
                                                    throw new NucleusUserException(Localiser.msg("021211", fieldName, fieldType.getName(), resultFieldType.getName()));
                                                }
                                                if (!Modifier.isPublic(fld.getModifiers()))
                                                {
                                                    publicField = false;
                                                }
                                            }
                                            catch (NoSuchFieldException nsfe)
                                            {
                                                publicField = false;
                                            }

                                            // Check for a public set method
                                            if (!publicField)
                                            {
                                                Method setMethod = QueryUtils.getPublicSetMethodForFieldOfResultClass(resultClass, fieldName, resultFieldType);
                                                if (setMethod == null)
                                                {
                                                    // No setter, so check for a public put(Object, Object) method
                                                    Method putMethod = QueryUtils.getPublicPutMethodForResultClass(resultClass);
                                                    if (putMethod == null)
                                                    {
                                                        throw new NucleusUserException(Localiser.msg("021212", resultClass.getName(), fieldName));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    else if (stmtMap instanceof StatementNewObjectMapping)
                                    {
                                        // TODO Handle this
                                    }
                                }
                            }
                        }

                        return null;
                    }
                });
            }

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021085", this, datastoreCompilation.getSQL()));
            }

            boolean hasParams = false;
            if (explicitParameters != null)
            {
                hasParams = true;
            }
            else if (parameterValues != null && parameterValues.size() > 0)
            {
                hasParams = true;
            }
            if (!datastoreCompilation.isPrecompilable() || (datastoreCompilation.getSQL().indexOf('?') < 0 && hasParams))
            {
                // Some parameters had their clauses evaluated during compilation so the query
                // didn't gain any parameters, so don't cache it
                NucleusLogger.QUERY.debug(Localiser.msg("021075"));
            }
            else
            {
                if (useCaching() && queryCacheKey != null)
                {
                	qm.addDatastoreQueryCompilation(datastoreKey, getLanguage(), queryCacheKey, datastoreCompilation);
                }
            }
        }
    }

    /**
     * Convenience accessor for the SQL to invoke in the datastore for this query.
     * @return The SQL.
     */
    public String getSQL()
    {
        if (datastoreCompilation != null)
        {
            return datastoreCompilation.getSQL();
        }
        return null;
    }

    protected Object performExecute(Map parameters)
    {
        if (candidateCollection != null)
        {
            // Supplied collection of instances, so evaluate in-memory
            if (candidateCollection.isEmpty())
            {
                return Collections.EMPTY_LIST;
            }

            List candidates = new ArrayList(candidateCollection);
            return new JPQLEvaluator(this, candidates, compilation, parameters, clr).execute(true, true, true, true, true);
        }
        else if (type == Query.SELECT)
        {
            // Query results are cached, so return those
            List<Object> cachedResults = getQueryManager().getQueryResult(this, parameters);
            if (cachedResults != null)
            {
                return new CandidateIdsQueryResult(this, cachedResults);
            }
        }

        Object results = null;
        ManagedConnection mconn = getStoreManager().getConnection(ec);
        try
        {
            // Execute the query
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", getLanguage(), getSingleStringQuery(), null));
            }

            RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
            AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
            SQLController sqlControl = storeMgr.getSQLController();
            PreparedStatement ps = null;
            try
            {
                if (type == Query.SELECT)
                {
                    // Create PreparedStatement and apply parameters, result settings etc
                    ps = RDBMSQueryUtils.getPreparedStatementForQuery(mconn, datastoreCompilation.getSQL(), this);
                    SQLStatementHelper.applyParametersToStatement(ps, ec, datastoreCompilation.getStatementParameters(), null, parameters);
                    RDBMSQueryUtils.prepareStatementForExecution(ps, this, false);

                    registerTask(ps);
                    ResultSet rs = null;
                    try
                    {
                        rs = sqlControl.executeStatementQuery(ec, mconn, toString(), ps);
                    }
                    finally
                    {
                        deregisterTask();
                    }

                    AbstractRDBMSQueryResult qr = null;
                    try
                    {
                        if (evaluateInMemory())
                        {
                            // IN-MEMORY EVALUATION
                            ResultObjectFactory rof = new PersistentClassROF(storeMgr, acmd, datastoreCompilation.getResultDefinitionForClass(), ignoreCache, getFetchPlan(), candidateClass);

                            // Just instantiate the candidates for later in-memory processing
                            // TODO Use a queryResult rather than an ArrayList so we load when required
                            List candidates = new ArrayList();
                            while (rs.next())
                            {
                                candidates.add(rof.getObject(ec, rs));
                            }

                            // Perform in-memory filter/result/order etc
                            results = new JPQLEvaluator(this, candidates, compilation, parameters, clr).execute(true, true, true, true, true);
                        }
                        else
                        {
                            // IN-DATASTORE EVALUATION
                            ResultObjectFactory rof = null;
                            if (result != null)
                            {
                                // Each result row is of a result type
                                rof = new ResultClassROF(storeMgr, resultClass, datastoreCompilation.getResultDefinition());
                            }
                            else if (resultClass != null && resultClass != candidateClass)
                            {
                                rof = new ResultClassROF(storeMgr, resultClass, datastoreCompilation.getResultDefinitionForClass());
                            }
                            else
                            {
                                // Each result row is a candidate object
                                rof = new PersistentClassROF(storeMgr, acmd, datastoreCompilation.getResultDefinitionForClass(), ignoreCache, getFetchPlan(), candidateClass);
                            }

                            // Create the required type of QueryResult
                            String resultSetType = RDBMSQueryUtils.getResultSetTypeForQuery(this);
                            if (resultSetType.equals("scroll-insensitive") ||
                                    resultSetType.equals("scroll-sensitive"))
                            {
                                qr = new ScrollableQueryResult(this, rof, rs, getResultDistinct() ? null : candidateCollection);
                            }
                            else
                            {
                                qr = new ForwardQueryResult(this, rof, rs, getResultDistinct() ? null : candidateCollection);
                            }

                            // Register any bulk loaded member resultSets that need loading
                            Map<String, IteratorStatement> scoIterStmts = datastoreCompilation.getSCOIteratorStatements();
                            if (scoIterStmts != null)
                            {
                                Iterator<Map.Entry<String, IteratorStatement>> scoStmtIter = scoIterStmts.entrySet().iterator();
                                while (scoStmtIter.hasNext())
                                {
                                    Map.Entry<String, IteratorStatement> stmtIterEntry = scoStmtIter.next();
                                    IteratorStatement iterStmt = stmtIterEntry.getValue();
                                    String iterStmtSQL = iterStmt.getSQLStatement().getSelectStatement().toSQL();
                                    NucleusLogger.DATASTORE_RETRIEVE.debug(">> JPQL Bulk-Fetch of " + iterStmt.getBackingStore().getOwnerMemberMetaData().getFullFieldName());
                                    try
                                    {
                                        PreparedStatement psSco = sqlControl.getStatementForQuery(mconn, iterStmtSQL);
                                        if (datastoreCompilation.getStatementParameters() != null)
                                        {
                                            BulkFetchExistsHelper helper = new BulkFetchExistsHelper(this);
                                            helper.applyParametersToStatement(psSco, datastoreCompilation, iterStmt.getSQLStatement(), parameters);
                                        }
                                        ResultSet rsSCO = sqlControl.executeStatementQuery(ec, mconn, iterStmtSQL, psSco);
                                        qr.registerMemberBulkResultSet(iterStmt, rsSCO);
                                    }
                                    catch (SQLException e)
                                    {
                                        throw new NucleusDataStoreException(Localiser.msg("056006", iterStmtSQL), e);
                                    }
                                }
                            }

                            qr.initialise();

                            final QueryResult qr1 = qr;
                            final ManagedConnection mconn1 = mconn;
                            ManagedConnectionResourceListener listener = new ManagedConnectionResourceListener()
                            {
                                public void transactionFlushed(){}
                                public void transactionPreClose()
                                {
                                    // Tx : disconnect query from ManagedConnection (read in unread rows etc)
                                    qr1.disconnect();
                                }
                                public void managedConnectionPreClose()
                                {
                                    if (!ec.getTransaction().isActive())
                                    {
                                        // Non-Tx : disconnect query from ManagedConnection (read in unread rows etc)
                                        qr1.disconnect();
                                    }
                                }
                                public void managedConnectionPostClose(){}
                                public void resourcePostClose()
                                {
                                    mconn1.removeListener(this);
                                }
                            };
                            mconn.addListener(listener);
                            qr.addConnectionListener(listener);
                            results = qr;
                        }
                    }
                    finally
                    {
                        if (qr == null)
                        {
                            rs.close();
                        }
                    }
                }
                else if (type == Query.BULK_UPDATE || type == Query.BULK_DELETE)
                {
                    long bulkResult = 0;
                    if (datastoreCompilation.getSQL() != null)
                    {
                        // Process single bulk statement
                        ps = sqlControl.getStatementForUpdate(mconn, datastoreCompilation.getSQL(), false);
                        SQLStatementHelper.applyParametersToStatement(ps, ec, datastoreCompilation.getStatementParameters(), null, parameters);
                        RDBMSQueryUtils.prepareStatementForExecution(ps, this, false);

                        int[] execResult = sqlControl.executeStatementUpdate(ec, mconn, toString(), ps, true);
                        bulkResult = execResult[0];
                    }
                    else
                    {
                        // Process multiple bulk statements and derive return value
                        List<String> sqls = datastoreCompilation.getSQLs();
                        List<Boolean> sqlUseInCountFlags = datastoreCompilation.getSQLUseInCountFlags();
                        Iterator<Boolean> sqlUseInCountIter = sqlUseInCountFlags.iterator();
                        for (String sql : sqls)
                        {
                            Boolean useInCount = sqlUseInCountIter.next();
                            ps = sqlControl.getStatementForUpdate(mconn, sql, false);
                            SQLStatementHelper.applyParametersToStatement(ps, ec, datastoreCompilation.getStatementParameters(), null, parameters);
                            RDBMSQueryUtils.prepareStatementForExecution(ps, this, false);

                            int[] execResults = sqlControl.executeStatementUpdate(ec, mconn, toString(), ps, true);
                            if (useInCount)
                            {
                                bulkResult += execResults[0];
                            }
                        }
                        
                    }

                    try
                    {
                        // Evict all objects of this type from the cache
                        ec.getNucleusContext().getLevel2Cache().evictAll(candidateClass, subclasses);
                    }
                    catch (UnsupportedOperationException uoe)
                    {
                        // Do nothing
                    }

                    results = bulkResult;
                }
            }
            catch (SQLException sqle)
            {
                if (storeMgr.getDatastoreAdapter().isStatementCancel(sqle))
                {
                    throw new QueryInterruptedException("Query has been interrupted", sqle);
                }
                else if (storeMgr.getDatastoreAdapter().isStatementTimeout(sqle))
                {
                    throw new QueryTimeoutException("Query has been timed out", sqle);
                }
                throw new NucleusException(Localiser.msg("021042", datastoreCompilation.getSQL()), sqle);
            }

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", getLanguage(), "" + (System.currentTimeMillis() - startTime)));
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method that will throw an {@link UnsupportedOperationException} if the query implementation doesn't
     * support cancelling queries.
     */
    protected void assertSupportsCancel()
    {
        // We support cancel via JDBC PreparedStatement.cancel();
    }

    protected boolean cancelTaskObject(Object obj)
    {
        Statement ps = (Statement)obj;
        try
        {
            ps.cancel();
            return true;
        }
        catch (SQLException sqle)
        {
            NucleusLogger.DATASTORE_RETRIEVE.warn("Error cancelling query", sqle);
            return false;
        }
    }

    /**
     * Convenience method for whether this query supports timeouts.
     * @return Whether timeouts are supported.
     */
    protected boolean supportsTimeout()
    {
        return true;
    }

    /**
     * Method to set the (native) query statement for the compiled query as a whole.
     * The "table groups" in the resultant SQLStatement will be named as per the candidate alias,
     * and thereafter "{alias}.{fieldName}". 
     * @param parameters Input parameters (if known)
     * @param candidateCmd Metadata for the candidate class
     */
    private void compileQueryFull(Map parameters, AbstractClassMetaData candidateCmd)
    {
        if (type != Query.SELECT)
        {
            return;
        }
        if (candidateCollection != null)
        {
            return;
        }

        long startTime = 0;
        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            startTime = System.currentTimeMillis();
            NucleusLogger.QUERY.debug(Localiser.msg("021083", getLanguage(), toString()));
        }

        if (result != null)
        {
            datastoreCompilation.setResultDefinition(new StatementResultMapping());
        }
        else
        {
            datastoreCompilation.setResultDefinitionForClass(new StatementClassMapping());
        }

        // Generate statement for candidate(s)
        SQLStatement stmt = RDBMSQueryUtils.getStatementForCandidates((RDBMSStoreManager) getStoreManager(), null, candidateCmd,
            datastoreCompilation.getResultDefinitionForClass(), ec, candidateClass, subclasses, result, 
            compilation.getCandidateAlias(), compilation.getCandidateAlias());

        // Update the SQLStatement with filter, ordering, result etc
        Set<String> options = new HashSet<String>();
        options.add(QueryToSQLMapper.OPTION_CASE_INSENSITIVE);
        options.add(QueryToSQLMapper.OPTION_EXPLICIT_JOINS);
        if (getBooleanExtensionProperty(EXTENSION_USE_IS_NULL_WHEN_EQUALS_NULL_PARAM, false)) // Default to false for "IS NULL" with null param
        {
            options.add(QueryToSQLMapper.OPTION_NULL_PARAM_USE_IS_NULL);
        }
        QueryToSQLMapper sqlMapper = new QueryToSQLMapper(stmt, compilation, parameters,
            datastoreCompilation.getResultDefinitionForClass(), datastoreCompilation.getResultDefinition(),
            candidateCmd, subclasses, getFetchPlan(), ec, null, options, extensions);
        sqlMapper.setDefaultJoinType(JoinType.INNER_JOIN);
        sqlMapper.compile();
        datastoreCompilation.setParameterNameByPosition(sqlMapper.getParameterNameByPosition());
        datastoreCompilation.setPrecompilable(sqlMapper.isPrecompilable());

        // Apply any range
        if (range != null)
        {
            long lower = fromInclNo;
            long upper = toExclNo;
            if (fromInclParam != null)
            {
                lower = ((Number)parameters.get(fromInclParam)).longValue();
            }
            if (toExclParam != null)
            {
                upper = ((Number)parameters.get(toExclParam)).longValue();
            }
            long count = upper - lower;
            if (upper == Long.MAX_VALUE)
            {
                count = -1;
            }
            stmt.setRange(lower, count);
        }

        // Set any extensions
        boolean useUpdateLock = RDBMSQueryUtils.useUpdateLockForQuery(this);
        stmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, Boolean.valueOf(useUpdateLock));
        if (getBooleanExtensionProperty(EXTENSION_FOR_UPDATE_NOWAIT, false))
        {
            stmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE_NOWAIT, Boolean.TRUE);
        }

        datastoreCompilation.setSQL(stmt.getSelectStatement().toString());
        datastoreCompilation.setStatementParameters(stmt.getSelectStatement().getParametersForStatement());

        if (result == null && !(resultClass != null && resultClass != candidateClass))
        {
            // Select of candidates, so check for any immediate multi-valued fields that are marked for fetching
            FetchPlanForClass fpc = getFetchPlan().getFetchPlanForClass(candidateCmd);
            int[] fpMembers = fpc.getMemberNumbers();
            for (int i=0;i<fpMembers.length;i++)
            {
                AbstractMemberMetaData fpMmd = candidateCmd.getMetaDataForManagedMemberAtAbsolutePosition(fpMembers[i]);
                RelationType fpRelType = fpMmd.getRelationType(clr);
                if (RelationType.isRelationMultiValued(fpRelType))
                {
                    String multifetchType = getStringExtensionProperty(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_MULTIVALUED_FETCH, null);
                    if (multifetchType == null)
                    {
                        // Default to bulk-fetch, so advise the user of why this is happening and how to turn it off
                        NucleusLogger.QUERY.debug("You have selected field " + fpMmd.getFullFieldName() + " for fetching by this query. We will fetch it using 'EXISTS'." +
                            " To disable this set the query extension/hint '" + RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_MULTIVALUED_FETCH + "' as 'none' or remove the field" +
                            " from the query FetchPlan. If this bulk-fetch generates an invalid or unoptimised query, please report it with a way of reproducing it");
                        multifetchType = "exists";
                    }
                    if (multifetchType.equalsIgnoreCase("exists"))
                    {
                        if (fpMmd.hasCollection() && SCOUtils.collectionHasSerialisedElements(fpMmd))
                        {
                            // Ignore collections serialised into the owner (retrieved in main query)
                        }
                        else if (fpMmd.hasMap() && SCOUtils.mapHasSerialisedKeysAndValues(fpMmd))
                        {
                            // Ignore maps serialised into the owner (retrieved in main query)
                        }
                        else
                        {
                            // Fetch container contents for all candidate owners
                            BulkFetchExistsHelper helper = new BulkFetchExistsHelper(this);
                            IteratorStatement iterStmt = helper.getSQLStatementForContainerField(candidateCmd, parameters, fpMmd, datastoreCompilation, options);
                            if (iterStmt != null)
                            {
                                datastoreCompilation.setSCOIteratorStatement(fpMmd.getFullFieldName(), iterStmt);
                            }
                            else
                            {
                                NucleusLogger.GENERAL.debug("Note that query has field " + fpMmd.getFullFieldName() + " marked in the FetchPlan, yet this is currently not fetched by this query");
                            }
                        }
                    }
                    else
                    {
                        NucleusLogger.GENERAL.debug("Note that query has field " + fpMmd.getFullFieldName() + " marked in the FetchPlan, yet this is not fetched by this query.");
                    }
                }
                // TODO Continue this bulk fetch process to fields of fields that are fetched
            }
        }

        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(Localiser.msg("021084", getLanguage(), System.currentTimeMillis()-startTime));
        }
    }

    /**
     * Method to set the statement (and parameter/results definitions) to retrieve all candidates.
     * This is used when we want to evaluate in-memory and so just retrieve all possible candidates
     * first.
     * @param parameters Input parameters (if known)
     * @param candidateCmd Metadata for the candidate class
     */
    private void compileQueryToRetrieveCandidates(Map parameters, AbstractClassMetaData candidateCmd)
    {
        if (type != Query.SELECT)
        {
            return;
        }
        if (candidateCollection != null)
        {
            return;
        }

        StatementClassMapping resultsDef = new StatementClassMapping();
        datastoreCompilation.setResultDefinitionForClass(resultsDef);

        // Generate statement for candidate(s)
        SQLStatement stmt = RDBMSQueryUtils.getStatementForCandidates((RDBMSStoreManager) getStoreManager(), null, candidateCmd,
            datastoreCompilation.getResultDefinitionForClass(), ec, candidateClass, subclasses, result, null, null);

        if (stmt.allUnionsForSamePrimaryTable())
        {
            // Select fetch-plan fields of candidate class
            SQLStatementHelper.selectFetchPlanOfCandidateInStatement(stmt, datastoreCompilation.getResultDefinitionForClass(), candidateCmd, getFetchPlan(), 1);
        }
        else
        {
            // Select id only since tables don't have same mappings or column names
            // TODO complete-table will come through here but maybe ought to be treated differently
            SQLStatementHelper.selectIdentityOfCandidateInStatement(stmt, datastoreCompilation.getResultDefinitionForClass(), candidateCmd);
        }

        datastoreCompilation.setSQL(stmt.getSelectStatement().toString());
        datastoreCompilation.setStatementParameters(stmt.getSelectStatement().getParametersForStatement());
    }

    /**
     * Method to compile the query for RDBMS for a bulk update.
     * @param parameterValues The parameter values (if any)
     * @param candidateCmd Meta-data for the candidate class
     */
    protected void compileQueryUpdate(Map parameterValues, AbstractClassMetaData candidateCmd)
    {
        Expression[] updateExprs = compilation.getExprUpdate();
        if (updateExprs == null || updateExprs.length == 0)
        {
            // Nothing to update
            return;
        }

        // Generate statement for candidate and related classes in this inheritance tree
        RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
        DatastoreClass candidateTbl = storeMgr.getDatastoreClass(candidateCmd.getFullClassName(), clr);
        if (candidateTbl == null)
        {
            // TODO Using subclass-table, so find the table(s) it can be persisted into
            throw new NucleusDataStoreException("Bulk update of " + candidateCmd.getFullClassName() + " not supported since candidate has no table of its own");
        }

        InheritanceStrategy inhStr = candidateCmd.getBaseAbstractClassMetaData().getInheritanceMetaData().getStrategy();

        List<BulkTable> tables = new ArrayList<BulkTable>();
        tables.add(new BulkTable(candidateTbl, true));
        if (inhStr != InheritanceStrategy.COMPLETE_TABLE)
        {
            // Add deletion from superclass tables since we will have an entry there
            while (candidateTbl.getSuperDatastoreClass() != null)
            {
                candidateTbl = candidateTbl.getSuperDatastoreClass();
                tables.add(new BulkTable(candidateTbl, false));
            }
        }

        Collection<String> subclassNames = storeMgr.getSubClassesForClass(candidateCmd.getFullClassName(), true, clr);
        if (subclassNames != null && !subclassNames.isEmpty())
        {
            // Check for subclasses having their own tables and hence needing multiple DELETEs
            Iterator<String> iter = subclassNames.iterator();
            while (iter.hasNext())
            {
                String subclassName = iter.next();
                DatastoreClass subclassTbl = storeMgr.getDatastoreClass(subclassName, clr);
                if (candidateTbl != subclassTbl)
                {
                    // Only include BulkTable in count if using COMPLETE_TABLE strategy
                    tables.add(0, new BulkTable(subclassTbl, inhStr == InheritanceStrategy.COMPLETE_TABLE));
                }
            }
        }

        List<String> sqls = new ArrayList<String>();
        List<Boolean> sqlCountFlags = new ArrayList<Boolean>();
        for (BulkTable bulkTable : tables)
        {
            // Generate statement for candidate
            DatastoreClass table = bulkTable.table;
            Map<String, Object> extensions = null;
            if (!storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.UPDATE_DELETE_STATEMENT_ALLOW_TABLE_ALIAS_IN_WHERE_CLAUSE))
            {
                extensions = new HashMap<String, Object>();
                extensions.put(SQLStatement.EXTENSION_SQL_TABLE_NAMING_STRATEGY, "table-name");
            }
            SQLStatement stmt = new SQLStatement(storeMgr, table, null, null, extensions);
            stmt.setClassLoaderResolver(clr);
            stmt.setCandidateClassName(candidateCmd.getFullClassName());

            if (table.getMultitenancyMapping() != null)
            {
                // Multi-tenancy restriction
                JavaTypeMapping tenantMapping = table.getMultitenancyMapping();
                SQLTable tenantSqlTbl = stmt.getPrimaryTable();
                SQLExpression tenantExpr = stmt.getSQLExpressionFactory().newExpression(stmt, tenantSqlTbl, tenantMapping);
                SQLExpression tenantVal = stmt.getSQLExpressionFactory().newLiteral(stmt, tenantMapping, storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID));
                stmt.whereAnd(tenantExpr.eq(tenantVal), true);
            }
            // TODO Discriminator restriction?

            Set<String> options = new HashSet<String>();
            options.add(QueryToSQLMapper.OPTION_CASE_INSENSITIVE);
            options.add(QueryToSQLMapper.OPTION_EXPLICIT_JOINS);
            if (getBooleanExtensionProperty(EXTENSION_USE_IS_NULL_WHEN_EQUALS_NULL_PARAM, false)) // Default to false for "IS NULL" with null param
            {
                options.add(QueryToSQLMapper.OPTION_NULL_PARAM_USE_IS_NULL);
            }
            QueryToSQLMapper sqlMapper = new QueryToSQLMapper(stmt, compilation, parameterValues, null, null, candidateCmd, subclasses, getFetchPlan(), ec, null, options, extensions);
            sqlMapper.setDefaultJoinType(JoinType.INNER_JOIN);
            sqlMapper.compile();

            if (stmt.hasUpdates())
            {
                sqls.add(stmt.getUpdateStatement().toString());
                sqlCountFlags.add(bulkTable.useInCount);

                datastoreCompilation.setStatementParameters(stmt.getSelectStatement().getParametersForStatement());
            }
        }

        if (sqls.size() == 1)
        {
            datastoreCompilation.setSQL(sqls.get(0));
        }
        else
        {
            datastoreCompilation.setSQL(sqls, sqlCountFlags);
        }
    }

    private class BulkTable
    {
        DatastoreClass table;
        boolean useInCount;
        public BulkTable(DatastoreClass tbl, boolean useInCount)
        {
            this.table = tbl;
            this.useInCount = useInCount;
        }
        public String toString() { return table.toString(); }
    }

    /**
     * Method to compile the query for RDBMS for a bulk delete.
     * @param parameterValues The parameter values (if any)
     * @param candidateCmd Meta-data for the candidate class
     */
    protected void compileQueryDelete(Map parameterValues, AbstractClassMetaData candidateCmd)
    {
        RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
        DatastoreClass candidateTbl = storeMgr.getDatastoreClass(candidateCmd.getFullClassName(), clr);
        if (candidateTbl == null)
        {
            // TODO Using subclass-table, so find the table(s) it can be persisted into
            throw new NucleusDataStoreException("Bulk delete of " + candidateCmd.getFullClassName() + " not supported since candidate has no table of its own");
        }

        InheritanceStrategy inhStr = candidateCmd.getBaseAbstractClassMetaData().getInheritanceMetaData().getStrategy();

        List<BulkTable> tables = new ArrayList<BulkTable>();
        tables.add(new BulkTable(candidateTbl, true));
        if (inhStr != InheritanceStrategy.COMPLETE_TABLE)
        {
            // Add deletion from superclass tables since we will have an entry there
            while (candidateTbl.getSuperDatastoreClass() != null)
            {
                candidateTbl = candidateTbl.getSuperDatastoreClass();
                tables.add(new BulkTable(candidateTbl, false));
            }
        }

        Collection<String> subclassNames = storeMgr.getSubClassesForClass(candidateCmd.getFullClassName(), true, clr);
        if (subclassNames != null && !subclassNames.isEmpty())
        {
            // Check for subclasses having their own tables and hence needing multiple DELETEs
            Iterator<String> iter = subclassNames.iterator();
            while (iter.hasNext())
            {
                String subclassName = iter.next();
                DatastoreClass subclassTbl = storeMgr.getDatastoreClass(subclassName, clr);
                if (candidateTbl != subclassTbl)
                {
                    // Only include BulkTable in count if using COMPLETE_TABLE strategy
                    tables.add(0, new BulkTable(subclassTbl, inhStr == InheritanceStrategy.COMPLETE_TABLE));
                }
            }
        }

        List<String> sqls = new ArrayList<String>();
        List<Boolean> sqlCountFlags = new ArrayList<Boolean>();
        for (BulkTable bulkTable : tables)
        {
            // Generate statement for candidate
            DatastoreClass table = bulkTable.table;
            Map<String, Object> extensions = null;
            if (!storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.UPDATE_DELETE_STATEMENT_ALLOW_TABLE_ALIAS_IN_WHERE_CLAUSE))
            {
                extensions = new HashMap<String, Object>();
                extensions.put(SQLStatement.EXTENSION_SQL_TABLE_NAMING_STRATEGY, "table-name");
            }
            SQLStatement stmt = new SQLStatement(storeMgr, table, null, null, extensions);
            stmt.setClassLoaderResolver(clr);
            stmt.setCandidateClassName(candidateCmd.getFullClassName());

            if (table.getMultitenancyMapping() != null)
            {
                // Multi-tenancy restriction
                JavaTypeMapping tenantMapping = table.getMultitenancyMapping();
                SQLTable tenantSqlTbl = stmt.getPrimaryTable();
                SQLExpression tenantExpr = stmt.getSQLExpressionFactory().newExpression(stmt, tenantSqlTbl, tenantMapping);
                SQLExpression tenantVal = stmt.getSQLExpressionFactory().newLiteral(stmt, tenantMapping, storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID));
                stmt.whereAnd(tenantExpr.eq(tenantVal), true);
            }
            // TODO Discriminator restriction?

            Set<String> options = new HashSet<String>();
            options.add(QueryToSQLMapper.OPTION_CASE_INSENSITIVE);
            options.add(QueryToSQLMapper.OPTION_EXPLICIT_JOINS);
            if (getBooleanExtensionProperty(EXTENSION_USE_IS_NULL_WHEN_EQUALS_NULL_PARAM, false)) // Default to false for "IS NULL" with null param
            {
                options.add(QueryToSQLMapper.OPTION_NULL_PARAM_USE_IS_NULL);
            }
            options.add(QueryToSQLMapper.OPTION_BULK_DELETE_NO_RESULT);
            QueryToSQLMapper sqlMapper = new QueryToSQLMapper(stmt, compilation, parameterValues, null, null, candidateCmd, subclasses, getFetchPlan(), ec, null, options, extensions);
            sqlMapper.setDefaultJoinType(JoinType.INNER_JOIN);
            sqlMapper.compile();

            sqls.add(stmt.getDeleteStatement().toString());
            sqlCountFlags.add(bulkTable.useInCount);
            datastoreCompilation.setStatementParameters(stmt.getDeleteStatement().getParametersForStatement());
        }

        if (sqls.size() == 1)
        {
            datastoreCompilation.setSQL(sqls.get(0));
        }
        else
        {
            datastoreCompilation.setSQL(sqls, sqlCountFlags);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.Query#processesRangeInDatastoreQuery()
     */
    @Override
    public boolean processesRangeInDatastoreQuery()
    {
        if (range == null)
        {
            // No range specified so makes no difference
            return true;
        }

        RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
        DatastoreAdapter dba = storeMgr.getDatastoreAdapter();
        boolean using_limit_where_clause = (dba.getRangeByLimitEndOfStatementClause(fromInclNo, toExclNo).length() > 0);
        boolean using_rownum = (dba.getRangeByRowNumberColumn().length() > 0) || (dba.getRangeByRowNumberColumn2().length() > 0);

        return using_limit_where_clause || using_rownum;
    }

    /**
     * Method to return the names of the extensions supported by this query.
     * To be overridden by subclasses where they support additional extensions.
     * @return The supported extension names
     */
    public Set<String> getSupportedExtensions()
    {
        Set<String> supported = super.getSupportedExtensions();
        supported.add(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_TYPE);
        supported.add(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_CONCURRENCY);
        supported.add(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_FETCH_DIRECTION);
        return supported;
    }

    /**
     * Add a vendor-specific extension this query.
     * Intercepts any setting of in-memory evaluation, so we can throw away any datastore compilation.
     * @param key the extension key
     * @param value the extension value
     */
    public void addExtension(String key, Object value)
    {
        if (key != null && key.equals(EXTENSION_EVALUATE_IN_MEMORY))
        {
            datastoreCompilation = null;
            getQueryManager().deleteDatastoreQueryCompilation(getStoreManager().getQueryCacheKey(), getLanguage(), toString());
        }
        super.addExtension(key, value);
    }

    /**
     * Set multiple extensions, or use null to clear extensions.
     * Intercepts any setting of in-memory evaluation, so we can throw away any datastore compilation.
     * @param extensions Query extensions
     */
    public void setExtensions(Map extensions)
    {
        if (extensions != null && extensions.containsKey(EXTENSION_EVALUATE_IN_MEMORY))
        {
            datastoreCompilation = null;
            getQueryManager().deleteDatastoreQueryCompilation(getStoreManager().getQueryCacheKey(), getLanguage(), toString());
        }
        super.setExtensions(extensions);
    }

    public RDBMSQueryCompilation getDatastoreCompilation()
    {
        return datastoreCompilation;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.Query#getNativeQuery()
     */
    @Override
    public Object getNativeQuery()
    {
        if (datastoreCompilation != null)
        {
            return datastoreCompilation.getSQL();
        }
        return super.getNativeQuery();
    }
}