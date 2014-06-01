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
package org.datanucleus.store.rdbms.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.Configuration;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.StatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Utilities for use in queries specific to RDBMS.
 */
public class RDBMSQueryUtils extends QueryUtils
{
    /**
     * Convenience method that takes a result set that contains a discriminator column and returns
     * the class name that it represents.
     * @param discrimMapping Mapping for the discriminator column
     * @param dismd Metadata for the discriminator
     * @param rs The result set
     * @param ec execution context
     * @return The class name for the object represented in the current row
     */
    public static String getClassNameFromDiscriminatorResultSetRow(JavaTypeMapping discrimMapping,
            DiscriminatorMetaData dismd, ResultSet rs, ExecutionContext ec)
    {
        String rowClassName = null;
        
        if (discrimMapping != null && dismd.getStrategy() != DiscriminatorStrategy.NONE)
        {
            try
            {
                String discriminatorColName = discrimMapping.getDatastoreMapping(0).getColumn().getIdentifier().getName();
                String discriminatorValue = rs.getString(discriminatorColName);
                rowClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discriminatorValue, dismd);
            }
            catch (SQLException e)
            {
                // discriminator column doesn't exist with this name
            }
        }
        return rowClassName;
    }

    /**
     * Accessor for the result set type for the specified query.
     * Uses the persistence property "datanucleus.rdbms.query.resultSetType" and allows it to be
     * overridden by the query extension of the same name.
     * Checks both the PMF, and also the query extensions.
     * @param query The query
     * @return The result set type string
     */
    public static String getResultSetTypeForQuery(Query query)
    {
        String rsTypeString = 
            query.getExecutionContext().getNucleusContext().getConfiguration().getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_TYPE);
        Object rsTypeExt = query.getExtension(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_TYPE);
        if (rsTypeExt != null)
        {
            rsTypeString = (String)rsTypeExt;
        }
        return rsTypeString;
    }

    /**
     * Accessor for the result set concurrency for the specified query.
     * Uses the persistence property "datanucleus.rdbms.query.resultSetConcurrency" and allows it to be
     * overridden by the query extension of the same name.
     * Checks both the PMF, and also the query extensions.
     * @param query The query
     * @return The result set concurrency string
     */
    public static String getResultSetConcurrencyForQuery(Query query)
    {
        String rsConcurrencyString = 
            query.getExecutionContext().getNucleusContext().getConfiguration().getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_CONCURRENCY);
        Object rsConcurrencyExt = query.getExtension(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_CONCURRENCY);
        if (rsConcurrencyExt != null)
        {
            rsConcurrencyString = (String)rsConcurrencyExt;
        }
        return rsConcurrencyString;
    }

    /**
     * Convenience method to return if the specified query should use an "UPDATE" lock on returned objects.
     * First checks whether serializeRead is set on the query and, if not, falls back to the setting
     * for the class.
     * @param query The query
     * @return Whether to use an "UPDATE" lock
     */
    public static boolean useUpdateLockForQuery(Query query)
    {
    	if (query.getSerializeRead() != null)
    	{
    	    if (!query.getExecutionContext().getTransaction().isActive())
    	    {
    	        // Only applies in a transaction
    	        return false;
    	    }

    	    // Query value takes top priority
            return query.getSerializeRead();
    	}
    	else
        {
    	    // Fallback to transaction or class itself
            return query.getExecutionContext().getSerializeReadForClass(query.getCandidateClassName());
        }
    }

    /**
     * Method to create a PreparedStatement for use with the query.
     * @param conn the Connection
     * @param queryStmt The statement text for the query
     * @param query The query
     * @return the PreparedStatement
     * @throws SQLException Thrown if an error occurs creating the statement
     */
    public static PreparedStatement getPreparedStatementForQuery(ManagedConnection conn, String queryStmt, 
            Query query)
    throws SQLException
    {
        // Apply any non-standard result set definition if required (either from the PMF, or via query extensions)
        String rsTypeString = RDBMSQueryUtils.getResultSetTypeForQuery(query);
        if (rsTypeString != null &&
            (!rsTypeString.equals("scroll-sensitive") && !rsTypeString.equals("forward-only") &&
             !rsTypeString.equals("scroll-insensitive")))
        {
            throw new NucleusUserException(Localiser.msg("052510"));
        }
        if (rsTypeString != null)
        {
            DatastoreAdapter dba = ((RDBMSStoreManager)query.getStoreManager()).getDatastoreAdapter();

            // Add checks on what the DatastoreAdapter supports
            if (rsTypeString.equals("scroll-sensitive") && !dba.supportsOption(DatastoreAdapter.RESULTSET_TYPE_SCROLL_SENSITIVE))
            {
                NucleusLogger.DATASTORE_RETRIEVE.info("Query requested to run with result-set type of " + rsTypeString + " yet not supported by adapter. Using forward-only");
                rsTypeString = "forward-only";
            }
            else if (rsTypeString.equals("scroll-insensitive") && !dba.supportsOption(DatastoreAdapter.RESULTSET_TYPE_SCROLL_INSENSITIVE))
            {
                NucleusLogger.DATASTORE_RETRIEVE.info("Query requested to run with result-set type of " + rsTypeString + " yet not supported by adapter. Using forward-only");
                rsTypeString = "forward-only";
            }
            else if (rsTypeString.equals("forward-only") && !dba.supportsOption(DatastoreAdapter.RESULTSET_TYPE_FORWARD_ONLY))
            {
                NucleusLogger.DATASTORE_RETRIEVE.info("Query requested to run with result-set type of " + rsTypeString + " yet not supported by adapter. Using scroll-sensitive");
                rsTypeString = "scroll-sensitive";
            }
        }

        String rsConcurrencyString = RDBMSQueryUtils.getResultSetConcurrencyForQuery(query);
        if (rsConcurrencyString != null &&
            (!rsConcurrencyString.equals("read-only") && !rsConcurrencyString.equals("updateable")))
        {
            throw new NucleusUserException(Localiser.msg("052511"));
        }

        SQLController sqlControl = ((RDBMSStoreManager)query.getStoreManager()).getSQLController();
        PreparedStatement ps = sqlControl.getStatementForQuery(conn, queryStmt, rsTypeString, rsConcurrencyString);

        return ps;
    }

    /**
     * Method to apply any restrictions to the created ResultSet.
     * @param ps The PreparedStatement
     * @param query The query
     * @param applyTimeout Whether to apply the query timeout (if any) direct to the PreparedStatement
     * @throws SQLException Thrown when an error occurs applying the constraints
     */
    public static void prepareStatementForExecution(PreparedStatement ps, Query query, boolean applyTimeout)
    throws SQLException
    {
        NucleusContext nucleusCtx = query.getExecutionContext().getNucleusContext();
        RDBMSStoreManager storeMgr = (RDBMSStoreManager) query.getStoreManager();
        Configuration conf = nucleusCtx.getConfiguration();

        if (applyTimeout)
        {
            Integer timeout = query.getDatastoreReadTimeoutMillis();
            if (timeout != null && timeout > 0)
            {
                ps.setQueryTimeout(timeout/1000);
            }
        }

        // Apply any fetch size
        int fetchSize = 0;
        if (query.getFetchPlan().getFetchSize() > 0)
        {
            // FetchPlan has a size set so use that
            fetchSize = query.getFetchPlan().getFetchSize();
        }
        if (storeMgr.getDatastoreAdapter().supportsQueryFetchSize(fetchSize))
        {
            ps.setFetchSize(fetchSize);
        }

        // Apply any fetch direction
        String fetchDir = conf.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_FETCH_DIRECTION);
        Object fetchDirExt = query.getExtension(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_FETCH_DIRECTION);
        if (fetchDirExt != null)
        {
            fetchDir = (String)fetchDirExt;
            if (!fetchDir.equals("forward") && !fetchDir.equals("reverse") && !fetchDir.equals("unknown"))
            {
                throw new NucleusUserException(Localiser.msg("052512"));
            }
        }

        if (fetchDir.equals("reverse"))
        {
            ps.setFetchDirection(ResultSet.FETCH_REVERSE);
        }
        else if (fetchDir.equals("unknown"))
        {
            ps.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        }

        // Add a limit on the number of rows to include the maximum we may need
        long toExclNo = query.getRangeToExcl();
        if (toExclNo != 0 && toExclNo != Long.MAX_VALUE)
        {
            if (toExclNo > Integer.MAX_VALUE)
            {
                // setMaxRows takes an int as input so limit to the correct range
                ps.setMaxRows(Integer.MAX_VALUE);
            }
            else
            {
                ps.setMaxRows((int)toExclNo);
            }
        }
    }

    /**
     * Method to return a statement selecting the candidate table(s) required to cover all possible
     * types for this candidates inheritance strategy.
     * @param storeMgr RDBMS StoreManager
     * @param parentStmt Parent statement (if there is one)
     * @param cmd Metadata for the class
     * @param clsMapping Mapping for the results of the statement
     * @param ec ExecutionContext
     * @param candidateCls Candidate class
     * @param subclasses Whether to create a statement for subclasses of the candidate too
     * @param result The result clause
     * @param candidateAlias alias for the candidate (if any)
     * @param candidateTableGroupName TableGroup name for the candidate (if any)
     * @return The SQLStatement
     * @throws NucleusException if there are no tables for concrete classes in this query (hence would return null)
     */
    public static SQLStatement getStatementForCandidates(RDBMSStoreManager storeMgr, SQLStatement parentStmt, 
            AbstractClassMetaData cmd, StatementClassMapping clsMapping, ExecutionContext ec, Class candidateCls, 
            boolean subclasses, String result, String candidateAlias, String candidateTableGroupName)
    {
        SQLStatement stmt = null;

        DatastoreIdentifier candidateAliasId = null;
        if (candidateAlias != null)
        {
            candidateAliasId = storeMgr.getIdentifierFactory().newTableIdentifier(candidateAlias);
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        List<DatastoreClass> candidateTables = new ArrayList<DatastoreClass>();
        if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
        {
            DatastoreClass candidateTable = storeMgr.getDatastoreClass(cmd.getFullClassName(), clr);
            if (candidateTable != null)
            {
                candidateTables.add(candidateTable);
            }
            if (subclasses)
            {
                Collection<String> subclassNames = 
                    storeMgr.getSubClassesForClass(cmd.getFullClassName(), subclasses, clr);
                if (subclassNames != null)
                {
                    Iterator<String> subclassIter = subclassNames.iterator();
                    while (subclassIter.hasNext())
                    {
                        String subclassName = subclassIter.next();
                        DatastoreClass tbl = storeMgr.getDatastoreClass(subclassName, clr);
                        if (tbl != null)
                        {
                            candidateTables.add(tbl);
                        }
                    }
                }
            }

            Iterator<DatastoreClass> iter = candidateTables.iterator();
            int maxClassNameLength = cmd.getFullClassName().length();
            while (iter.hasNext())
            {
                DatastoreClass cls = iter.next();
                String className = cls.getType();
                if (className.length() > maxClassNameLength)
                {
                    maxClassNameLength = className.length();
                }
            }

            iter = candidateTables.iterator();
            while (iter.hasNext())
            {
                DatastoreClass cls = iter.next();

                SQLStatement tblStmt =
                    new SQLStatement(parentStmt, storeMgr, cls, candidateAliasId, candidateTableGroupName);
                tblStmt.setClassLoaderResolver(clr);
                tblStmt.setCandidateClassName(cls.getType());

                // Add SELECT of dummy column accessible as "NUCLEUS_TYPE" containing the classname
                JavaTypeMapping m = storeMgr.getMappingManager().getMapping(String.class);
                String nuctypeName = cls.getType();
                if (maxClassNameLength > nuctypeName.length())
                {
                    nuctypeName = StringUtils.leftAlignedPaddedString(nuctypeName, maxClassNameLength);
                }
                StringLiteral lit = new StringLiteral(tblStmt, m, nuctypeName, null);
                tblStmt.select(lit, UnionStatementGenerator.NUC_TYPE_COLUMN);

                if (stmt == null)
                {
                    stmt = tblStmt;
                }
                else
                {
                    stmt.union(tblStmt);
                }
            }
            if (clsMapping != null)
            {
                clsMapping.setNucleusTypeColumnName(UnionStatementGenerator.NUC_TYPE_COLUMN);
            }
        }
        else
        {
            // "new-table", "superclass-table", "subclass-table"
            List<Class> candidateClasses = new ArrayList<Class>();
            if (ClassUtils.isReferenceType(candidateCls))
            {
                // Persistent interface, so find all persistent implementations
                String[] clsNames =
                    storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(candidateCls.getName(), clr);
                for (int i=0;i<clsNames.length;i++)
                {
                    Class cls = clr.classForName(clsNames[i]);
                    DatastoreClass table = storeMgr.getDatastoreClass(clsNames[i], clr);
                    candidateClasses.add(cls);
                    candidateTables.add(table);
                    AbstractClassMetaData implCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(cls, clr);
                    if (implCmd.getIdentityType() != cmd.getIdentityType())
                    {
                        throw new NucleusUserException("You are querying an interface (" + cmd.getFullClassName() + ") " +
                            "yet one of its implementations (" + implCmd.getFullClassName() + ") " +
                            " uses a different identity type!");
                    }
                    else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        if (cmd.getPKMemberPositions().length != implCmd.getPKMemberPositions().length)
                        {
                            throw new NucleusUserException("You are querying an interface (" + cmd.getFullClassName() + ") " +
                                "yet one of its implementations (" + implCmd.getFullClassName() + ") " +
                                " has a different number of PK members!");
                        }
                    }
                }
            }
            else
            {
                DatastoreClass candidateTable = storeMgr.getDatastoreClass(cmd.getFullClassName(), clr);
                if (candidateTable != null)
                {
                    // Candidate has own table
                    candidateClasses.add(candidateCls);
                    candidateTables.add(candidateTable);
                }
                else
                {
                    // Candidate stored in subclass tables
                    AbstractClassMetaData[] cmds = storeMgr.getClassesManagingTableForClass(cmd, clr);
                    if (cmds != null && cmds.length > 0)
                    {
                        for (int i=0;i<cmds.length;i++)
                        {
                            DatastoreClass table = storeMgr.getDatastoreClass(cmds[i].getFullClassName(), clr);
                            Class cls = clr.classForName(cmds[i].getFullClassName());
                            candidateClasses.add(cls);
                            candidateTables.add(table);
                        }
                    }
                    else
                    {
                        throw new UnsupportedOperationException("No tables for query of " + cmd.getFullClassName());
                    }
                }
            }

            for (int i=0;i<candidateTables.size();i++)
            {
                DatastoreClass tbl = candidateTables.get(i);
                Class cls = candidateClasses.get(i);
                StatementGenerator stmtGen = null;
                if (tbl.getDiscriminatorMapping(true) != null || QueryUtils.resultHasOnlyAggregates(result))
                {
                    // Either has a discriminator, or only selecting aggregates so need single select
                    stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, cls, subclasses, 
                        candidateAliasId, candidateTableGroupName);
                    stmtGen.setOption(StatementGenerator.OPTION_RESTRICT_DISCRIM);
                }
                else
                {
                    stmtGen = new UnionStatementGenerator(storeMgr, clr, cls, subclasses, 
                        candidateAliasId, candidateTableGroupName);
                    if (result == null)
                    {
                        // Returning one row per candidate so include distinguisher column
                        stmtGen.setOption(StatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                        clsMapping.setNucleusTypeColumnName(UnionStatementGenerator.NUC_TYPE_COLUMN);
                    }
                }
                stmtGen.setParentStatement(parentStmt);
                SQLStatement tblStmt = stmtGen.getStatement();

                if (stmt == null)
                {
                    stmt = tblStmt;
                }
                else
                {
                    stmt.union(tblStmt);
                }
            }
        }

        return stmt;
    }

    /**
     * Utility to take a ResultSet and return a ResultObjectFactory for extracting the results,
     * assuming that no candidate class is supplied. The QueryResult will return either a
     * result class type, or Object/Object[] depending on whether a ResultClass has been defined.
     * @param storeMgr RDBMS StoreManager
     * @param rs The ResultSet
     * @param resultClass Result class if required (or null)
     * @return The query ResultObjectFactory
     */
    public static ResultObjectFactory getResultObjectFactoryForNoCandidateClass(RDBMSStoreManager storeMgr, ResultSet rs,
            Class resultClass)
    {
        // No candidate class, so use resultClass or Object/Object[]
        Class requiredResultClass = resultClass;
        int numberOfColumns = 0;
        String[] resultFieldNames = null;
        try
        {
            ResultSetMetaData rsmd = rs.getMetaData();
            numberOfColumns = rsmd.getColumnCount();
            if (requiredResultClass == null)
            {
                if (numberOfColumns == 1)
                {
                    requiredResultClass = Object.class;
                }
                else
                {
                    requiredResultClass = Object[].class;
                }
            }

            // Generate names to use for the fields based on the column names
            resultFieldNames = new String[numberOfColumns];
            for (int i=0;i<numberOfColumns;i++)
            {
                // Use "label" (i.e SQL alias) if specified, otherwise "name"
                String colName = rsmd.getColumnName(i+1);
                String colLabel = rsmd.getColumnLabel(i+1);
                if (StringUtils.isWhitespace(colLabel))
                {
                    resultFieldNames[i] = colName;
                }
                else
                {
                    resultFieldNames[i] = colLabel;
                }
            }
        }
        catch (SQLException sqe)
        {
            // Do nothing
        }

        return new ResultClassROF(storeMgr, requiredResultClass, resultFieldNames);
    }
}