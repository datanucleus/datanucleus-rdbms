/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved. 
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
2004 Andy Jefferson - commented and localised
2004 Andy Jefferson - changed to allow for null candidate class
2005 Andy Jefferson - changed to use JDBC parameters
2005 Andy Jefferson - added timeout support
2005 Andy Jefferson - changed to use persistable candidate class
2005 Andy Jefferson - added checks on missing columns to compile step
2006 Andy Jefferson - implemented executeWithMap taking Integer args
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.ClassNotPersistableException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.QueryResultMetaData;
import org.datanucleus.store.Extent;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.QueryInterruptedException;
import org.datanucleus.store.query.QueryResult;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * A Query using SQL.
 * The majority of this has to be specified in the query filter itself.
 * There are no variables/imports. Ordering/grouping is explicit in the query.
 * <h3>Results</h3>
 * Results from this type of query will be :-
 * <ul>
 * <li><b>resultClass</b> - each row of the ResultSet is converted into an instance of the result class</li>
 * <li><b>candidateClass</b> - each row of the ResultSet is converted into an instance of the candidate</li>
 * <li><b>Object[]</b> - when no candidate or result class specified</li>
 * <li><b>Long</b> - when the query is an INSERT/UPDATE/DELETE/MERGE</li>
 * </ul>
 * <h3>Parameters</h3>
 * Parameters to this type of query can be :-
* <ul>
 * <li><b>Positional</b> : The SQL statement includes "?" and the parameters are positional starting at 1 (just like in JDBC).</li>
 * <li><b>Numbered</b> : The SQL statement includes "?1", "?2" etc (with numbers starting at 1) and the users input of parameters at execute is of the numbered parameters.</li>
 * <li><b>Named</b> : The SQL statement includes ":myParam1", ":myOtherParam" etc and the users input of parameters via "executeWithMap" includes values for all specified names.</li>
 * </ul>
 */
public final class SQLQuery extends Query
{
    private static final long serialVersionUID = -6820729188666657398L;

    /** The statement that the user specified to the Query. */
    protected transient final String inputSQL;

    /** The actual SQL issued at execution time. */
    protected transient String compiledSQL = null;

    /** MetaData defining the results of the query. */
    protected QueryResultMetaData resultMetaData = null;

    /** State variable for the compilation state */
    protected transient boolean isCompiled = false;

    /** Mappings for the result of the query. */
    protected transient StatementMappingIndex[] stmtMappings;

    /**
     * Constructor for a new query using the existing query as start point.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The existing query
     */
    public SQLQuery(StoreManager storeMgr, ExecutionContext ec, SQLQuery query)
    {
        this(storeMgr, ec, query.inputSQL);
    }

    /**
     * Constructs a new query instance.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public SQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (String)null);
    }

    /**
     * Constructs a new query instance for the provided SQL query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param queryString The SQL query string
     */
    public SQLQuery(StoreManager storeMgr, ExecutionContext ec, String queryString)
    {
        super(storeMgr, ec);

        candidateClass = null;
        filter = null;
        imports = null;
        explicitVariables = null;
        explicitParameters = null;
        ordering = null;

        if (queryString == null)
        {
            throw new NucleusUserException(Localiser.msg("059001"));
        }

        // Remove any end-of-line/tab chars for when user dumped the query in a text file with one word per line!
        this.inputSQL = queryString.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').trim();

        // Detect type of SQL statement
        String firstToken = inputSQL.trim().substring(0,6).toUpperCase(); 
        if (firstToken.equals("SELECT"))
        {
            type = QueryType.SELECT;
        }
        else if (firstToken.equals("DELETE"))
        {
            type = QueryType.BULK_DELETE;
            unique = true;
        }
        else if (firstToken.equals("UPDATE") || firstToken.equals("INSERT") || firstToken.startsWith("MERGE"))
        {
            type = QueryType.BULK_UPDATE;
            unique = true;
        }
        else
        {
            // Stored procedures, others
            type = QueryType.OTHER;
            unique = true;
        }

        if (ec.getApiAdapter().getName().equalsIgnoreCase("JDO"))
        {
            // Check for strict SQL where the API restricts the usage
            boolean allowAllSyntax = ec.getNucleusContext().getConfiguration().getBooleanProperty(PropertyNames.PROPERTY_QUERY_SQL_ALLOWALL);
            if (ec.getProperty(PropertyNames.PROPERTY_QUERY_SQL_ALLOWALL) != null)
            {
                allowAllSyntax = ec.getBooleanProperty(PropertyNames.PROPERTY_QUERY_SQL_ALLOWALL);
            }
            if (!allowAllSyntax)
            {
                // JDO spec [14.7] : SQL queries must start with SELECT
                if (!firstToken.equals("SELECT"))
                {
                    throw new NucleusUserException(Localiser.msg("059002", inputSQL));
                }
            }
        }
    }

    public String getLanguage()
    {
        return "SQL";
    }

    /**
     * Set the candidate Extent to query.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param pcs the Candidate Extent.
     * @throws NucleusUserException Always thrown since method not applicable
     */
    public void setCandidates(Extent pcs)
    {
        throw new NucleusUserException(Localiser.msg("059004"));
    }

    /**
     * Set the candidate Collection to query.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param pcs the Candidate collection.
     * @throws NucleusUserException Always thrown since method not applicable
     */
    public void setCandidates(Collection pcs)
    {
        throw new NucleusUserException(Localiser.msg("059005"));
    }

    /**
     * Set the result for the results. The application might want to get results
     * from a query that are not instances of the candidate class. The results
     * might be fields of persistent instances, instances of classes other than
     * the candidate class, or aggregates of fields.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param result The result parameter consists of the optional keyword
     * distinct followed by a commaseparated list of named result expressions or
     * a result class specification.
     * @throws NucleusUserException Always thrown.
     */
    public void setResult(String result)
    {
        throw new NucleusUserException(Localiser.msg("059006"));
    }

    /**
     * Method to set the MetaData defining the result.
     * Setting this will unset the resultClass.
     * @param qrmd Query Result MetaData
     */
    public void setResultMetaData(QueryResultMetaData qrmd)
    {
        this.resultMetaData = qrmd;
        super.setResultClass(null);
    }

    /**
     * Set the result class for the results.
     * Setting this will unset the resultMetaData.
     * @param result_cls The result class
     */
    public void setResultClass(Class result_cls)
    {
        super.setResultClass(result_cls);
        this.resultMetaData = null;
    }

    /**
     * Set the range of the results. Not applicable for SQL.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param fromIncl From element no (inclusive) to return
     * @param toExcl To element no (exclusive) to return
     * @throws NucleusUserException Always thrown.
     */
    public void setRange(int fromIncl, int toExcl)
    {
        throw new NucleusUserException(Localiser.msg("059007"));
    }

    /**
     * Method to set whether to use subclasses. Not applicable for SQL.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param subclasses Whether to use subclasses
     * @throws NucleusUserException Always thrown.
     */
    public void setSubclasses(boolean subclasses)
    {
        throw new NucleusUserException(Localiser.msg("059004"));
    }

    /**
     * Set the filter for the query. Not applicable for SQL.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param filter the query filter.
     * @throws NucleusUserException Always thrown since method not applicable
     */
    public void setFilter(String filter)
    {
        throw new NucleusUserException(Localiser.msg("059008"));
    }

    /**
     * Declare the unbound variables to be used in the query.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param variables the variables separated by semicolons.
     * @throws NucleusUserException Always thrown since method not applicable
     */
    public void declareExplicitVariables(String variables)
    {
        throw new NucleusUserException(Localiser.msg("059009"));
    }

    /**
     * Declare the explicit parameters to be used in the query.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param parameters the parameters separated by semicolons.
     * @exception NucleusUserException Always thrown.
     */
    public void declareExplicitParameters(String parameters)
    {
        throw new NucleusUserException(Localiser.msg("059016"));
    }

    /**
     * Set the import statements to be used to identify the fully qualified name of variables or parameters.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param imports import statements separated by semicolons.
     * @exception NucleusUserException Always thrown.
     */
    public void declareImports(String imports)
    {
        throw new NucleusUserException(Localiser.msg("059026"));
    }

    /**
     * Set the grouping specification for the result Collection.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param grouping the grouping specification.
     * @throws NucleusUserException  Always thrown.
     */
    public void setGrouping(String grouping)
    {
        throw new NucleusUserException(Localiser.msg("059010"));
    }

    /**
     * Set the ordering specification for the result Collection.
     * This implementation always throws a NucleusUserException since this concept doesn't apply to SQL queries.
     * @param ordering  the ordering specification.
     * @throws NucleusUserException  Always thrown.
     */
    public void setOrdering(String ordering)
    {
        throw new NucleusUserException(Localiser.msg("059011"));
    }

    /**
     * Execute the query to delete persistent objects.
     * @param parameters the Map containing all of the parameters.
     * @return the filtered QueryResult of the deleted objects.
     */
    protected long performDeletePersistentAll(Map parameters)
    {
        throw new NucleusUserException(Localiser.msg("059000"));
    }

    /**
     * Convenience method to return whether the query should return a single row.
     * @return Whether it represents a unique row
     */
    protected boolean shouldReturnSingleRow()
    {
        if (unique)
        {
            return true;
        }
        // An SQL query returns what it returns
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.Query#processesRangeInDatastoreQuery()
     */
    @Override
    public boolean processesRangeInDatastoreQuery()
    {
        return true;
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof SQLQuery) || !super.equals(obj))
        {
            return false;
        }
        return inputSQL.equals(((SQLQuery)obj).inputSQL);
    }

    public int hashCode()
    {
        return super.hashCode() ^ inputSQL.hashCode();
    }

    /**
     * Utility to remove any previous compilation of this Query.
     */
    protected void discardCompiled()
    {
        super.discardCompiled();

        compiledSQL = null;
        isCompiled = false;
        stmtMappings = null;
    }

    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    protected boolean isCompiled()
    {
        return isCompiled;
    }

    /**
     * Verify the elements of the query and provide a hint to the query to prepare and optimize an execution plan.
     */
    public void compileInternal(Map parameterValues)
    {
        if (isCompiled)
        {
            return;
        }

        // Default to using the users SQL direct with no substitution of params etc
        compiledSQL = inputSQL;

        if (candidateClass != null && getType() == QueryType.SELECT)
        {
            // Perform any sanity checking of input for SELECT queries
            RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
            if (cmd == null)
            {
                throw new ClassNotPersistableException(candidateClass.getName());
            }
            if (cmd.getPersistableSuperclass() != null)
            {
               // throw new PersistentSuperclassNotAllowedException(candidateClass.getName());
            }

            if (getResultClass() == null)
            {
                // Check the presence of the required columns (id, version, discriminator) in the candidate class
                String selections = stripComments(compiledSQL.trim()).substring(7); // Skip "SELECT "
                int fromStart = selections.indexOf("FROM");
                if (fromStart == -1)
                {
                    fromStart = selections.indexOf("from");
                }
                selections = selections.substring(0, fromStart).trim();

                String[] selectedColumns = StringUtils.split(selections, ",");
                if (selectedColumns == null || selectedColumns.length == 0)
                {
                    throw new NucleusUserException(Localiser.msg("059003", compiledSQL));
                }

                if (selectedColumns.length == 1 && selectedColumns[0].trim().equals("*"))
                {
                    // SQL Query using * so just end the checking since all possible columns will be selected
                }
                else
                {
                    // Generate id column field information for later checking the id is present
                    DatastoreClass table = storeMgr.getDatastoreClass(candidateClass.getName(), clr);
                    PersistableMapping idMapping = (PersistableMapping)table.getIdMapping();
                    String[] idColNames = new String[idMapping.getNumberOfDatastoreMappings()];
                    boolean[] idColMissing = new boolean[idMapping.getNumberOfDatastoreMappings()];
                    for (int i=0;i<idMapping.getNumberOfDatastoreMappings();i++)
                    {
                        idColNames[i] = idMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString();
                        idColMissing[i] = true;
                    }

                    // Generate discriminator/version information for later checking they are present
                    JavaTypeMapping discrimMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
                    String discriminatorColName = (discrimMapping != null) ? discrimMapping.getDatastoreMapping(0).getColumn().getIdentifier().toString() : null;
                    JavaTypeMapping versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
                    String versionColName = (versionMapping != null) ? versionMapping.getDatastoreMapping(0).getColumn().getIdentifier().toString() : null;
                    boolean discrimMissing = (discriminatorColName != null);
                    boolean versionMissing = (versionColName != null);

                    // Go through the selected fields and check the existence of id, version, discriminator cols
                    DatastoreAdapter dba = storeMgr.getDatastoreAdapter();
                    final AbstractClassMetaData candidateCmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
                    for (int i = 0; i < selectedColumns.length; i++)
                    {
                        String colName = selectedColumns[i].trim();
                        if (colName.indexOf(" AS ") > 0)
                        {
                            // Allow for user specification of "XX.YY AS ZZ"
                            colName = colName.substring(colName.indexOf(" AS ")+4).trim();
                        }
                        else if (colName.indexOf(" as ") > 0)
                        {
                            // Allow for user specification of "XX.YY as ZZ"
                            colName = colName.substring(colName.indexOf(" as ")+4).trim();
                        }

                        if (candidateCmd.getIdentityType() == IdentityType.DATASTORE)
                        {
                            // Check for existence of id column, allowing for any RDBMS using quoted identifiers
                            if (SQLQuery.columnNamesAreTheSame(dba, idColNames[0], colName))
                            {
                                idColMissing[0] = false;
                            }
                        }
                        else if (candidateCmd.getIdentityType() == IdentityType.APPLICATION)
                        {
                            for (int j=0; j<idColNames.length; j++)
                            {
                                // Check for existence of id column, allowing for any RDBMS using quoted identifiers
                                if (SQLQuery.columnNamesAreTheSame(dba, idColNames[j], colName))
                                {
                                    idColMissing[j] = false;
                                }
                            }
                        }

                        if (discrimMissing && SQLQuery.columnNamesAreTheSame(dba, discriminatorColName, colName))
                        {
                            discrimMissing = false;
                        }
                        else if (versionMissing && SQLQuery.columnNamesAreTheSame(dba, versionColName, colName))
                        {
                            versionMissing = false;
                        }
                    }

                    if (discrimMissing)
                    {
                        throw new NucleusUserException(Localiser.msg("059014", compiledSQL, candidateClass.getName(), discriminatorColName));
                    }
                    if (versionMissing)
                    {
                        throw new NucleusUserException(Localiser.msg("059015", compiledSQL, candidateClass.getName(), versionColName));
                    }
                    for (int i = 0; i < idColMissing.length; i++)
                    {
                        if (idColMissing[i])
                        {
                            throw new NucleusUserException(Localiser.msg("059013", compiledSQL, candidateClass.getName(), idColNames[i]));
                        }
                    }
                }
            }
        }

        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(Localiser.msg("059012", compiledSQL));
        }

        isCompiled = true;
    }

    /**
     * Execute the query and return the result.
     * For a SELECT query this will be the QueryResult. 
     * For an UPDATE/DELETE it will be the row count for the update statement.
     * @param parameters the Map containing all of the parameters (positional parameters) (not null)
     * @return the result of the query
     */
    protected Object performExecute(Map parameters)
    {
        if (parameters.size() != (parameterNames != null ? parameterNames.length : 0))
        {
            throw new NucleusUserException(Localiser.msg("059019", (parameterNames!=null) ? "" + parameterNames.length : 0,"" + parameters.size()));
        }

        if (type == QueryType.BULK_DELETE || type == QueryType.BULK_UPDATE)
        {
            // Update/Delete statement (INSERT/UPDATE/DELETE/MERGE)
            try
            {
                RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
                ManagedConnection mconn = storeMgr.getConnection(ec);
                SQLController sqlControl = storeMgr.getSQLController();

                try
                {
                    PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, compiledSQL, false);
                    try
                    {
                        // Set the values of any parameters
                        for (int i=0;i<parameters.size();i++)
                        {
                            ps.setObject((i+1), parameters.get(Integer.valueOf(i+1)));
                        }

                        // Execute the update statement
                        int[] rcs = sqlControl.executeStatementUpdate(ec, mconn, compiledSQL, ps, true);
                        return Long.valueOf(rcs[0]); // Return a single Long with the number of records updated
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
                throw new NucleusDataStoreException(Localiser.msg("059025", compiledSQL), e);
            }
        }
        else if (type == QueryType.SELECT)
        {
            // Query statement (SELECT, stored-procedure)
            AbstractRDBMSQueryResult qr = null;
            try
            {
                RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
                ManagedConnection mconn = storeMgr.getConnection(ec);
                SQLController sqlControl = storeMgr.getSQLController();

                try
                {
                    PreparedStatement ps = RDBMSQueryUtils.getPreparedStatementForQuery(mconn, compiledSQL, this);
                    try
                    {
                        // Set the values of any parameters
                        for (int i=0;i<parameters.size();i++)
                        {
                            ps.setObject((i+1), parameters.get(Integer.valueOf(i+1)));
                        }

                        // Apply any user-specified constraints over timeouts and ResultSet
                        RDBMSQueryUtils.prepareStatementForExecution(ps, this, true);

                        ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, compiledSQL, ps);
                        try
                        {
                            // Generate a ResultObjectFactory
                            ResultObjectFactory rof = null;
                            if (resultMetaData != null)
                            {
                                // Each row of the ResultSet is defined by MetaData
                                rof = new ResultMetaDataROF(ec, rs, resultMetaData);
                            }
                            else if (resultClass != null || candidateClass == null)
                            {
                                // Each row of the ResultSet is either an instance of resultClass, or Object[]
                                rof = RDBMSQueryUtils.getResultObjectFactoryForNoCandidateClass(ec, rs, resultClass);
                            }
                            else
                            {
                                // Each row of the ResultSet is an instance of the candidate class
                                rof = getResultObjectFactoryForCandidateClass(rs);
                            }

                            // Return the associated type of results depending on whether scrollable or not
                            qr = RDBMSQueryUtils.getQueryResultForQuery(this, rof, rs, null);
                            qr.initialise();

                            final QueryResult qr1 = qr;
                            final ManagedConnection mconn1 = mconn;
                            mconn.addListener(new ManagedConnectionResourceListener()
                            {
                                public void transactionFlushed(){}
                                public void transactionPreClose()
                                {
                                    // Disconnect the query from this ManagedConnection (read in unread rows etc)
                                    qr1.disconnect();                        
                                }
                                public void managedConnectionPreClose()
                                {
                                    if (!ec.getTransaction().isActive())
                                    {
                                        // Disconnect the query from this ManagedConnection (read in unread rows etc)
                                        qr1.disconnect();
                                    }
                                }
                                public void managedConnectionPostClose(){}
                                public void resourcePostClose()
                                {
                                    mconn1.removeListener(this);
                                }
                            });                            
                        }
                        finally
                        {
                            if (qr == null)
                            {
                                rs.close();
                            }
                        }
                    }
                    catch (QueryInterruptedException qie)
                    {
                        // Execution was cancelled so cancel the PreparedStatement
                        ps.cancel();
                        throw qie;
                    }
                    finally
                    {
                        if (qr == null)
                        {
                            sqlControl.closeStatement(mconn, ps);
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
                throw new NucleusDataStoreException(Localiser.msg("059025", compiledSQL), e);
            }
            return qr;
        }
        else
        {
            // 'Other' statement (manually invoked stored-procedure?, CREATE?, DROP?, or similar)
            try
            {
                RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
                ManagedConnection mconn = storeMgr.getConnection(ec);
                SQLController sqlControl = storeMgr.getSQLController();

                try
                {
                    PreparedStatement ps = RDBMSQueryUtils.getPreparedStatementForQuery(mconn, compiledSQL, this);
                    try
                    {
                        // Set the values of any parameters
                        for (int i=0;i<parameters.size();i++)
                        {
                            ps.setObject((i+1), parameters.get(Integer.valueOf(i+1)));
                        }

                        // Apply any user-specified constraints over timeouts etc
                        RDBMSQueryUtils.prepareStatementForExecution(ps, this, false);

                        sqlControl.executeStatement(ec, mconn, compiledSQL, ps);
                    }
                    catch (QueryInterruptedException qie)
                    {
                        // Execution was cancelled so cancel the PreparedStatement
                        ps.cancel();
                        throw qie;
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
                throw new NucleusDataStoreException(Localiser.msg("059025", compiledSQL), e);
            }
            return true;
        }
    }

    /**
     * Execute the query and return the filtered List.
     * Override the method in Query since we want the parameter names to be Integer based here.
     * @param parameters the Object array with all of the parameters.
     * @return The query results
     */
    public Object executeWithArray(Object[] parameters)
    {
        // Convert the input array into a Map with Integer keys 1, 2, etc
        Map parameterMap = new HashMap();
        if (parameters != null)
        {
            for (int i = 0; i < parameters.length; ++i)
            {
                parameterMap.put(Integer.valueOf(i+1), parameters[i]);
            }
        }

        // Prepare for execution
        Map executionMap = prepareForExecution(parameterMap);

        // Execute using superclass method
        return super.executeQuery(executionMap);
    }

    /**
     * Execute the query using the input Map of parameters.
     * @param executeParameters the Map of the parameters passed in to execute().
     * @return The query results
     */
    public Object executeWithMap(Map executeParameters)
    {
        // Prepare for execution
        Map executionMap = prepareForExecution(executeParameters);

        // Execute using superclass method
        return super.executeQuery(executionMap);
    }

    /**
     * Method to process the input parameters preparing the statement and parameters for execution.
     * The parameters returned are ready for execution. Compiles the query, and updates the 
     * "compiledSQL" and "parameterNames".
     * Supports positional parameters, numbered parameters (?1, ?2), and named parameters (:p1, :p3).
     * If using named parameters then the keys of the Map must align to the names in the SQL.
     * If using numbered/positional parameters then the keys of the Map must be Integer and align with the
     * parameter numbers/positions.
     * @param executeParameters The input parameters map
     * @return Map of parameters for execution
     */
    protected Map prepareForExecution(Map executeParameters)
    {
        Map params = new HashMap();
        if (implicitParameters != null)
        {
            // Add any implicit parameters defined via the API
            params.putAll(implicitParameters);
        }
        if (executeParameters != null)
        {
            // Add any parameters defined at execute()
            params.putAll(executeParameters);
        }

        compileInternal(executeParameters);

        // Clear the parameterNames that are set in compile since we assign ours using the parameterMap passed in
        List paramNames = new ArrayList();

        // Build up list of expected parameters (in the order the query needs them)
        // Allow for positional parameters ('?'), numbered parameters ("?1") or named parameters (":myParam")
        Collection expectedParams = new ArrayList();
        boolean complete = false;
        int charPos = 0;
        char[] statement = compiledSQL.toCharArray();
        StringBuilder paramName = null;
        int paramPos = 0;
        boolean colonParam = true;
        StringBuilder runtimeJdbcText = new StringBuilder();
        while (!complete)
        {
            char c = statement[charPos];
            boolean endOfParam = false;
            if (c == '?')
            {
                // New positional/numbered parameter
                colonParam = false;
                paramPos++;
                paramName = new StringBuilder();
            }
            else if (c == ':')
            {
                // New named parameter
                if (charPos > 0)
                {
                    char prev = statement[charPos-1];
                    if (Character.isLetterOrDigit(prev))
                    {
                        // Some valid SQL can include colon, so ignore if the part just before is alphanumeric
                    }
                    else
                    {
                        colonParam = true;
                        paramPos++;
                        paramName = new StringBuilder();
                    }
                }
                else
                {
                    colonParam = true;
                    paramPos++;
                    paramName = new StringBuilder();
                }
            }
            else
            {
                if (paramName != null)
                {
                    if (Character.isLetterOrDigit(c))
                    {
                        // Allow param names to include alphnumeric
                        paramName.append(c);
                    }
                    else
                    {
                        endOfParam = true;
                    }
                }
            }
            if (paramName != null)
            {
                if (endOfParam)
                {
                    // Replace the param by "?" in the runtime SQL
                    runtimeJdbcText.append('?');
                    runtimeJdbcText.append(c);
                }
            }
            else
            {
                runtimeJdbcText.append(c);
            }

            charPos++;

            complete = (charPos == compiledSQL.length());
            if (complete && paramName != null && !endOfParam)
            {
                runtimeJdbcText.append('?');
            }

            if (paramName != null && (complete || endOfParam))
            {
                // Process the parameter
                if (paramName.length() > 0)
                {
                    // Named/Numbered parameter
                    if (colonParam)
                    {
                        expectedParams.add(paramName.toString());
                    }
                    else
                    {
                        try
                        {
                            Integer num = Integer.valueOf(paramName.toString());
                            expectedParams.add(num);
                        }
                        catch (NumberFormatException nfe)
                        {
                            throw new NucleusUserException("SQL query " + inputSQL + " contains an invalid parameter specification " + paramName.toString());
                        }
                    }
                }
                else
                {
                    if (!colonParam)
                    {
                        // Positional parameter
                        expectedParams.add(Integer.valueOf(paramPos));
                    }
                    else
                    {
                        // Just a colon so ignore it
                    }
                }
                paramName = null;
            }
        }
        compiledSQL = runtimeJdbcText.toString(); // Update the SQL that JDBC will receive to just have ? for a parameter

        if (expectedParams.size() > 0 && params.isEmpty())
        {
            // We expect some parameters yet the user gives us none!
            throw new NucleusUserException(Localiser.msg("059028", inputSQL, "" + expectedParams.size()));
        }

        // Build a Map of params with keys 1, 2, 3, etc representing the position in the runtime JDBC SQL
        Map executeMap = new HashMap();

        // Cycle through the expected params
        paramPos = 1;
        for (Object expectedParam : expectedParams)
        {
            if (!params.containsKey(expectedParam))
            {
                // Expected parameter is not provided
                throw new NucleusUserException(Localiser.msg("059031", "" + expectedParam, inputSQL));
            }

            executeMap.put(Integer.valueOf(paramPos), params.get(expectedParam));
            paramNames.add("" + paramPos);
            paramPos++;
        }
        parameterNames = (String[])paramNames.toArray(new String[paramNames.size()]);

        return executeMap;
    }

    /**
     * Method that will throw an {@link UnsupportedOperationException} if the query implementation doesn't
     * support cancelling queries.
     */
    protected void assertSupportsCancel()
    {
        // We support cancel via JDBC PreparedStatement.cancel();
    }

    protected boolean supportsTimeout()
    {
        return true;
    }

    /**
     * Method to generate a ResultObjectFactory for converting rows of the provided ResultSet into instances of the candidate class. 
     * Populates "stmtMappings".
     * @param rs The ResultSet
     * @return The ResultObjectFactory
     * @throws SQLException Thrown if an error occurs processing the ResultSet
     */
    protected ResultObjectFactory getResultObjectFactoryForCandidateClass(ResultSet rs)
    throws SQLException
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RDBMSStoreManager storeMgr = (RDBMSStoreManager) getStoreManager();
        DatastoreAdapter dba = storeMgr.getDatastoreAdapter();

        // Create an index listing for ALL (fetchable) fields in the result class.
        final AbstractClassMetaData candidateCmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
        int fieldCount = candidateCmd.getNoOfManagedMembers() + candidateCmd.getNoOfInheritedManagedMembers();
        Map columnFieldNumberMap = new HashMap(); // Map of field numbers keyed by the column name
        stmtMappings = new StatementMappingIndex[fieldCount];
        DatastoreClass tbl = storeMgr.getDatastoreClass(candidateClass.getName(), clr);
        for (int fieldNumber = 0; fieldNumber < fieldCount; ++fieldNumber)
        {
            AbstractMemberMetaData mmd = candidateCmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String fieldName = mmd.getName();
            Class fieldType = mmd.getType();

            JavaTypeMapping m = null;
            if (mmd.getPersistenceModifier() != FieldPersistenceModifier.NONE)
            {
                if (tbl != null)
                {
                    // Get the field mapping from the candidate table
                    m = tbl.getMemberMapping(mmd);
                }
                else
                {
                    // Fall back to generating a mapping for this type - does this ever happen?
                    m = storeMgr.getMappingManager().getMappingWithDatastoreMapping(fieldType, false, false, clr);
                }
                if (m.includeInFetchStatement())
                {
                    // Set mapping for this field since it can potentially be returned from a fetch
                    String columnName = null;
                    if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                    {
                        for (int colNum = 0;colNum<mmd.getColumnMetaData().length;colNum++)
                        {
                            columnName = mmd.getColumnMetaData()[colNum].getName();
                            columnFieldNumberMap.put(columnName, Integer.valueOf(fieldNumber));
                        }
                    }
                    else
                    {
                        columnName = storeMgr.getIdentifierFactory().newColumnIdentifier(
                            fieldName, ec.getNucleusContext().getTypeManager().isDefaultEmbeddedType(fieldType), FieldRole.ROLE_NONE, false).getName();
                        columnFieldNumberMap.put(columnName, Integer.valueOf(fieldNumber));
                    }
                }
                else
                {
                    // Don't put anything in this position (field has no column in the result set)
                }
            }
            else
            {
                // Don't put anything in this position (field has no column in the result set)
            }
            stmtMappings[fieldNumber] = new StatementMappingIndex(m);
        }
        if (columnFieldNumberMap.size() == 0)
        {
            // None of the fields in the class have columns in the datastore table!
            throw new NucleusUserException(Localiser.msg("059030", candidateClass.getName())).setFatal();
        }

        // Generate id column field information for later checking the id is present
        DatastoreClass table = storeMgr.getDatastoreClass(candidateClass.getName(), clr);
        if (table == null)
        {
            AbstractClassMetaData[] cmds = storeMgr.getClassesManagingTableForClass(candidateCmd, clr);
            if (cmds != null && cmds.length == 1)
            {
                table = storeMgr.getDatastoreClass(cmds[0].getFullClassName(), clr);
            }
            else
            {
                throw new NucleusUserException("SQL query specified with class " + candidateClass.getName() +
                    " but this doesn't have its own table, or is mapped to multiple tables. Unsupported");
            }
        }

        PersistableMapping idMapping = (PersistableMapping)table.getIdMapping();
        String[] idColNames = new String[idMapping.getNumberOfDatastoreMappings()];
        for (int i=0;i<idMapping.getNumberOfDatastoreMappings();i++)
        {
            idColNames[i] = idMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString();
        }

        // Generate discriminator information for later checking it is present
        JavaTypeMapping discrimMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
        String discrimColName = discrimMapping != null ? discrimMapping.getDatastoreMapping(0).getColumn().getIdentifier().toString() : null;

        // Generate version information for later checking it is present
        JavaTypeMapping versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
        String versionColName = versionMapping != null ? versionMapping.getDatastoreMapping(0).getColumn().getIdentifier().toString() : null;

        // Go through the fields of the ResultSet and map to the required fields in the candidate
        ResultSetMetaData rsmd = rs.getMetaData();
        HashSet remainingColumnNames = new HashSet(columnFieldNumberMap.size()); // TODO We put nothing in this, so what is it for?!
        int colCount = rsmd.getColumnCount();
        int[] datastoreIndex = null;
        int[] versionIndex = null;
        int[] discrimIndex = null;

        int[] matchedFieldNumbers = new int[colCount];
        int fieldNumberPosition = 0;
        for (int colNum=1; colNum<=colCount; ++colNum)
        {
            String colName = rsmd.getColumnName(colNum);

            // Find the field for this column
            int fieldNumber = -1;
            Integer fieldNum = (Integer)columnFieldNumberMap.get(colName);
            if (fieldNum == null)
            {
                // Try column name in lowercase
                fieldNum = (Integer)columnFieldNumberMap.get(colName.toLowerCase());
                if (fieldNum == null)
                {
                    // Try column name in UPPERCASE
                    fieldNum = (Integer)columnFieldNumberMap.get(colName.toUpperCase());
                }
            }

            if (fieldNum != null)
            {
                fieldNumber = fieldNum.intValue();
            }
            if (fieldNumber >= 0)
            {
                int[] exprIndices = null;
                if (stmtMappings[fieldNumber].getColumnPositions() != null)
                {
                    exprIndices = new int[stmtMappings[fieldNumber].getColumnPositions().length+1];
                    for (int i=0;i<stmtMappings[fieldNumber].getColumnPositions().length;i++)
                    {
                        exprIndices[i] = stmtMappings[fieldNumber].getColumnPositions()[i];
                    }
                    exprIndices[exprIndices.length-1] = colNum;
                }
                else
                {
                    exprIndices = new int[] {colNum};
                }
                stmtMappings[fieldNumber].setColumnPositions(exprIndices);
                remainingColumnNames.remove(colName);
                matchedFieldNumbers[fieldNumberPosition++] = fieldNumber;
            }

            if (discrimColName != null && colName.equals(discrimColName))
            {
                // Identify the location of the discriminator column
                discrimIndex = new int[1];
                discrimIndex[0] = colNum;
            }

            if (versionColName != null && colName.equals(versionColName))
            {
                // Identify the location of the version column
                versionIndex = new int[1];
                versionIndex[0] = colNum;
            }

            if (candidateCmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Check for existence of id column, allowing for any RDBMS using quoted identifiers
                if (columnNamesAreTheSame(dba, idColNames[0], colName))
                {
                    datastoreIndex = new int[1];
                    datastoreIndex[0] = colNum;
                }
            }
        }

        // Set the field numbers found to match what we really have
        int[] fieldNumbers = new int[fieldNumberPosition];
        for (int i=0;i<fieldNumberPosition;i++)
        {
            fieldNumbers[i] = matchedFieldNumbers[i];
        }

        StatementClassMapping mappingDefinition = new StatementClassMapping();
        for (int i=0;i<fieldNumbers.length;i++)
        {
            mappingDefinition.addMappingForMember(fieldNumbers[i], stmtMappings[fieldNumbers[i]]);
        }
        if (datastoreIndex != null)
        {
            StatementMappingIndex datastoreMappingIdx = new StatementMappingIndex(table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false));
            datastoreMappingIdx.setColumnPositions(datastoreIndex);
            mappingDefinition.addMappingForMember(SurrogateColumnType.DATASTORE_ID.getFieldNumber(), datastoreMappingIdx);
        }
        if (discrimIndex != null)
        {
            StatementMappingIndex discrimMappingIdx = new StatementMappingIndex(table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, true));
            discrimMappingIdx.setColumnPositions(discrimIndex);
            mappingDefinition.addMappingForMember(SurrogateColumnType.DISCRIMINATOR.getFieldNumber(), discrimMappingIdx);
        }
        if (versionIndex != null)
        {
            StatementMappingIndex versionMappingIdx = new StatementMappingIndex(table.getSurrogateMapping(SurrogateColumnType.VERSION, true));
            versionMappingIdx.setColumnPositions(versionIndex);
            mappingDefinition.addMappingForMember(SurrogateColumnType.VERSION.getFieldNumber(), versionMappingIdx);
        }

        return new PersistentClassROF(ec, rs, mappingDefinition, candidateCmd, ignoreCache, getCandidateClass());
    }

    /**
     * Convenience method to compare two column names.
     * Allows for case sensitivity issues, and for database added quoting.
     * @param dba Datastore adapter
     * @param name1 The first name (from the datastore)
     * @param name2 The second name (from the user SQL statement)
     * @return Whether they are the same
     */
    public static boolean columnNamesAreTheSame(DatastoreAdapter dba, String name1, String name2)
    {
        if (name1.equalsIgnoreCase(name2) || name1.equalsIgnoreCase(dba.getIdentifierQuoteString() + name2 + dba.getIdentifierQuoteString()))
        {
            return true;
        }
        return false;
    }

    /**
     * Strips any slash-star comments from the given sql string.
     * @param sql sql string
     * @return sql without comments
     */
    private static String stripComments(String sql)
    {
        return sql.replaceAll("(?:/\\*(?:[^*]|(?:\\*+[^*/]))*\\*+/)|(?://.*)", "");
    }
}