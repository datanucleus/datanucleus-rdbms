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
package org.datanucleus.store.rdbms.request;

import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.LockManager;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.AbstractClassTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Class to retrieve the fields of an object of a specified class from the datastore.
 * If some of those fields are themselves persistent objects then this can optionally
 * retrieve fields of those objects in the same fetch.
 * <p>
 * Any surrogate version stored in this table will be fetched *if* the object being updated doesn't
 * already have a value for it. If the caller wants the surrogate version to be updated then
 * they should nullify the "transactional" version before calling.
 * </p>
 */
public class FetchRequest extends Request
{
    /** JDBC fetch statement without locking. */
    private String statementUnlocked;

    /** JDBC fetch statement with locking. */
    private String statementLocked;

    /** Absolute numbers of the fields/properties of the class to fetch. */
    private int[] memberNumbersToFetch = null;

    /** The mapping of the results of the SQL statement. */
    private StatementClassMapping mappingDefinition;

    /** Callbacks for postFetch() operations, to be called after the fetch itself (relation fields). */
    private final MappingCallbacks[] callbacks;

    private int numberOfFieldsToFetch = 0;

    /** Convenience string listing the fields to be fetched by this request. */
    private final String fieldsToFetch;

    /** Whether we are fetching a surrogate version in this fetch. */
    private boolean fetchingSurrogateVersion = false;

    /** Name of the version field. Only applies if the class has a version field (not surrogate). */
    private String versionFieldName = null;

    /**
     * Constructor, taking the table. Uses the structure of the datastore table to build a basic query.
     * @param classTable The Class Table representing the datastore table to retrieve
     * @param mmds MetaData of the fields/properties to retrieve
     * @param cmd ClassMetaData of objects being fetched
     * @param clr ClassLoader resolver
     */
    public FetchRequest(DatastoreClass classTable, AbstractMemberMetaData[] mmds, AbstractClassMetaData cmd, 
        ClassLoaderResolver clr)
    {
        super(classTable);

        RDBMSStoreManager storeMgr = classTable.getStoreManager();

        // Work out the real candidate table.
        // Instead of just taking the most derived table as the candidate we find the table closest to
        // the root table necessary to retrieve the requested fields
        boolean found = false;
        DatastoreClass candidateTable = classTable;
        if (mmds != null)
        {
            while (candidateTable != null)
            {
                for (int i=0;i<mmds.length;i++)
                {
                    JavaTypeMapping m = candidateTable.getMemberMappingInDatastoreClass(mmds[i]);
                    if (m != null)
                    {
                        found = true;
                        break;
                    }
                }
                if (found)
                {
                    break;
                }
                candidateTable = candidateTable.getSuperDatastoreClass();
            }
        }
        if (candidateTable == null)
        {
            candidateTable = classTable;
        }
        this.table = candidateTable;
        this.key = ((AbstractClassTable)table).getPrimaryKey();

        // Extract version information, from this table and any super-tables
        DatastoreClass currentTable = table;
        while (currentTable != null)
        {
            VersionMetaData currentVermd = currentTable.getVersionMetaData();
            if (currentVermd != null)
            {
                if (currentVermd.getFieldName() == null)
                {
                    // Surrogate version stored in this table
                    fetchingSurrogateVersion = true;
                }
                else
                {
                    // Version field
                    versionFieldName = currentVermd.getFieldName();
                }
            }

            currentTable = currentTable.getSuperDatastoreClass();
        }

        // TODO Can we skip the statement generation if we know there are no selectable fields?

        // Generate the statement for the requested members
        SQLStatement sqlStatement = new SQLStatement(storeMgr, table, null, null);
        mappingDefinition = new StatementClassMapping();
        Collection<MappingCallbacks> fetchCallbacks = new HashSet<MappingCallbacks>();
        numberOfFieldsToFetch = processMembersOfClass(sqlStatement, mmds, table, sqlStatement.getPrimaryTable(), mappingDefinition, fetchCallbacks, clr);
        callbacks = fetchCallbacks.toArray(new MappingCallbacks[fetchCallbacks.size()]);
        memberNumbersToFetch = mappingDefinition.getMemberNumbers();

        // Add WHERE clause restricting to an object of this type
        int inputParamNum = 1;
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Datastore identity value for input
            JavaTypeMapping datastoreIdMapping = table.getDatastoreIdMapping();
            SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), datastoreIdMapping);
            SQLExpression val = exprFactory.newLiteralParameter(sqlStatement, datastoreIdMapping, null, "ID");
            sqlStatement.whereAnd(expr.eq(val), true);

            StatementMappingIndex datastoreIdx = mappingDefinition.getMappingForMemberPosition(StatementClassMapping.MEMBER_DATASTORE_ID);
            if (datastoreIdx == null)
            {
                datastoreIdx = new StatementMappingIndex(datastoreIdMapping);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_DATASTORE_ID, datastoreIdx);
            }
            datastoreIdx.addParameterOccurrence(new int[] {inputParamNum});
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Application identity value(s) for input
            int[] pkNums = cmd.getPKMemberPositions();
            for (int i=0;i<pkNums.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkNums[i]);
                JavaTypeMapping pkMapping = table.getMemberMapping(mmd);
                SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), pkMapping);
                SQLExpression val = exprFactory.newLiteralParameter(sqlStatement, pkMapping, null, "PK" + i);
                sqlStatement.whereAnd(expr.eq(val), true);

                StatementMappingIndex pkIdx = mappingDefinition.getMappingForMemberPosition(pkNums[i]);
                if (pkIdx == null)
                {
                    pkIdx = new StatementMappingIndex(pkMapping);
                    mappingDefinition.addMappingForMember(pkNums[i], pkIdx);
                }
                int[] inputParams = new int[pkMapping.getNumberOfDatastoreMappings()];
                for (int j=0;j<pkMapping.getNumberOfDatastoreMappings();j++)
                {
                    inputParams[j] = inputParamNum++;
                }
                pkIdx.addParameterOccurrence(inputParams);
            }
        }

        if (table.getMultitenancyMapping() != null)
        {
            // Add restriction on multi-tenancy
            JavaTypeMapping tenantMapping = table.getMultitenancyMapping();
            SQLExpression tenantExpr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), tenantMapping);
            SQLExpression tenantVal = exprFactory.newLiteral(sqlStatement, tenantMapping, storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID));
            sqlStatement.whereAnd(tenantExpr.eq(tenantVal), true);
        }

        // Generate convenience string for logging
        StringBuilder str = new StringBuilder();
        if (mmds != null)
        {
            for (int i=0;i<mmds.length;i++)
            {
                if (!mmds[i].isPrimaryKey())
                {
                    if (str.length() > 0)
                    {
                        str.append(',');
                    }
                    str.append(mmds[i].getName());
                }
            }
        }
        if (fetchingSurrogateVersion)
        {
            // Add on surrogate version column
            if (str.length() > 0)
            {
                str.append(",");
            }
            str.append("[VERSION]");
        }
        if (!fetchingSurrogateVersion && numberOfFieldsToFetch == 0)
        {
            fieldsToFetch = null;
            sqlStatement = null;
            mappingDefinition = null;
        }
        else
        {
            fieldsToFetch = str.toString();

            // Generate the unlocked and locked JDBC statements
            statementUnlocked = sqlStatement.getSelectStatement().toSQL();
            sqlStatement.addExtension("lock-for-update", Boolean.TRUE);
            statementLocked = sqlStatement.getSelectStatement().toSQL();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.request.Request#execute(org.datanucleus.state.ObjectProvider)
     */
    public void execute(ObjectProvider op)
    {
        if (fieldsToFetch != null && NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("052218", op.getObjectAsPrintable(), fieldsToFetch, table));
        }

        if (isFetchingVersionOnly() && isVersionLoaded(op))
        {
            // Don't fetch if we have only the version to fetch and it is already loaded
            // Debug comment until we're sure this is a valid thing to do
        }
        else if (statementLocked != null)
        {
            ExecutionContext ec = op.getExecutionContext();
            RDBMSStoreManager storeMgr = table.getStoreManager();
            boolean locked = ec.getSerializeReadForClass(op.getClassMetaData().getFullClassName());
            short lockType = ec.getLockManager().getLockMode(op.getInternalObjectId());
            if (lockType != LockManager.LOCK_MODE_NONE)
            {
                if (lockType == LockManager.LOCK_MODE_PESSIMISTIC_READ || lockType == LockManager.LOCK_MODE_PESSIMISTIC_WRITE)
                {
                    // Override with pessimistic lock
                    locked = true;
                }
            }
            String statement = (locked ? statementLocked : statementUnlocked);

            StatementClassMapping mappingDef = mappingDefinition;
          /*if ((sm.isDeleting() || sm.isDetaching()) && mappingDefinition.hasChildMappingDefinitions())
            {
                // Don't fetch any children since the object is being deleted
                mappingDef = mappingDefinition.cloneStatementMappingWithoutChildren();
            }*/

            try
            {
                ManagedConnection mconn = storeMgr.getConnection(ec);
                SQLController sqlControl = storeMgr.getSQLController();

                try
                {
                    PreparedStatement ps = sqlControl.getStatementForQuery(mconn, statement);

                    AbstractClassMetaData cmd = op.getClassMetaData();
                    try
                    {
                        // Provide the primary key field(s) to the JDBC statement
                        if (cmd.getIdentityType() == IdentityType.DATASTORE)
                        {
                            StatementMappingIndex datastoreIdx = mappingDef.getMappingForMemberPosition(StatementClassMapping.MEMBER_DATASTORE_ID);
                            for (int i=0;i<datastoreIdx.getNumberOfParameterOccurrences();i++)
                            {
                                table.getDatastoreIdMapping().setObject(ec, ps, datastoreIdx.getParameterPositionsForOccurrence(i), op.getInternalObjectId());
                            }
                        }
                        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                        {
                            op.provideFields(cmd.getPKMemberPositions(), storeMgr.getFieldManagerForStatementGeneration(op, ps, mappingDef));
                        }

                        // Execute the statement
                        ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, statement, ps);
                        try
                        {
                            // Check for failure to find the object
                            if (!rs.next())
                            {
                                if (NucleusLogger.DATASTORE_RETRIEVE.isInfoEnabled())
                                {
                                    NucleusLogger.DATASTORE_RETRIEVE.info(Localiser.msg("050018", op.getInternalObjectId()));
                                }
                                throw new NucleusObjectNotFoundException("No such database row", op.getInternalObjectId());
                            }

                            // Copy the results into the object
                            op.replaceFields(memberNumbersToFetch, storeMgr.getFieldManagerForResultProcessing(op, rs, mappingDef));

                            if (op.getTransactionalVersion() == null)
                            {
                                // Object has no version set so update it from this fetch
                                Object datastoreVersion = null;
                                if (fetchingSurrogateVersion)
                                {
                                    // Surrogate version column - get from the result set using the version mapping
                                    StatementMappingIndex verIdx = mappingDef.getMappingForMemberPosition(StatementClassMapping.MEMBER_VERSION);
                                    datastoreVersion = table.getVersionMapping(true).getObject(ec, rs, verIdx.getColumnPositions());
                                }
                                else if (versionFieldName != null)
                                {
                                    // Version field - now populated in the field in the object from the results
                                    datastoreVersion = op.provideField(cmd.getAbsolutePositionOfMember(versionFieldName));
                                }
                                op.setVersion(datastoreVersion);
                            }
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
            catch (SQLException sqle)
            {
                String msg = Localiser.msg("052219", op.getObjectAsPrintable(), statement, sqle.getMessage());
                NucleusLogger.DATASTORE_RETRIEVE.warn(msg);
                List exceptions = new ArrayList();
                exceptions.add(sqle);
                while ((sqle = sqle.getNextException()) != null)
                {
                    exceptions.add(sqle);
                }
                throw new NucleusDataStoreException(msg, (Throwable[])exceptions.toArray(new Throwable[exceptions.size()]));
            }
        }

        // Execute any mapping actions now that we have fetched the fields
        for (int i = 0; i < callbacks.length; ++i)
        {
            callbacks[i].postFetch(op);
        }
    }

    /**
     * Convenience method to return if the version field of the managed object is loaded.
     * @param op ObjectProvider of the object
     * @return Whether the version is loaded
     */
    private boolean isVersionLoaded(ObjectProvider op)
    {
        return op.getObject() != null && op.getVersion() != null;
    }

    /**
     * Convenience method to return if just the version field is being fetched.
     * @return Whether we are just fetching the version field
     */
    private boolean isFetchingVersionOnly()
    {
        return ((fetchingSurrogateVersion || versionFieldName != null) && numberOfFieldsToFetch == 0);
    }

    /**
     * Method to process the supplied members of the class, adding to the SQLStatement as required.
     * Can recurse if some of the requested fields are persistent objects in their own right, so we
     * take the opportunity to retrieve some of their fields.
     * @param sqlStatement Statement being built
     * @param mmds Meta-data for the required fields/properties
     * @param table The table to look for member mappings
     * @param sqlTbl The table in the SQL statement to use for selects
     * @param mappingDef Mapping definition for the result
     * @param fetchCallbacks Any additional required callbacks are added here
     * @param clr ClassLoader resolver
     * @return Number of fields being fetched
     */
    protected int processMembersOfClass(SQLStatement sqlStatement, AbstractMemberMetaData[] mmds, 
            DatastoreClass table, SQLTable sqlTbl, StatementClassMapping mappingDef, Collection fetchCallbacks, ClassLoaderResolver clr)
    {
        int number = 0;
        if (mmds != null)
        {
            for (int i=0;i<mmds.length;i++)
            {
                // Get the mapping (in this table, or super-table)
                AbstractMemberMetaData mmd = mmds[i];
                JavaTypeMapping mapping = table.getMemberMapping(mmd);
                if (mapping != null)
                {
                    if (!mmd.isPrimaryKey() && mapping.includeInFetchStatement())
                    {
                        // The depth is the number of levels down to load in this statement.
                        // 0 is to load just this objects fields (as with JPOX, and DataNucleus up to 1.1.3)
                        int depth = 0;
                        if (mapping instanceof PersistableMapping)
                        {
                            depth = 1;
                            if (Modifier.isAbstract(mmd.getType().getModifiers()))
                            {
                                DatastoreClass relTable = table.getStoreManager().getDatastoreClass(mmd.getTypeName(), clr);
                                if (relTable != null && relTable.getDiscriminatorMapping(false) == null)
                                {
                                    // 1-1 relation to base class with no discriminator and has subclasses
                                    // hence no way of determining the exact type, hence no point in fetching it
                                    String[] subclasses = table.getStoreManager().getMetaDataManager().getSubclassesForClass(mmd.getTypeName(), false);
                                    if (subclasses != null && subclasses.length > 0)
                                    {
                                        depth = 0;
                                    }
                                }
                            }
                        }
                        SQLStatementHelper.selectMemberOfSourceInStatement(sqlStatement, mappingDef, null, sqlTbl, mmd, clr, depth);
                        number++;
                    }

                    if (mapping instanceof MappingCallbacks)
                    {
                        // TODO Need to add that this mapping is for base object or base.field1, etc
                        fetchCallbacks.add(mapping);
                    }
                }
            }
        }

        JavaTypeMapping verMapping = table.getVersionMapping(true);
        if (verMapping != null)
        {
            // Select version
            StatementMappingIndex verMapIdx = new StatementMappingIndex(verMapping);
            SQLTable verSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStatement, sqlTbl, verMapping);
            int[] cols = sqlStatement.select(verSqlTbl, verMapping, null);
            verMapIdx.setColumnPositions(cols);
            mappingDef.addMappingForMember(StatementClassMapping.MEMBER_VERSION, verMapIdx);
        }

        return number;
    }
}