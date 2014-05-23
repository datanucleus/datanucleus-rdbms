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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.LockManager;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Request to locate a record in the data store. Performs an SQL statement like
 * <pre>
 * SELECT 1 FROM CANDIDATE_TABLE WHERE ID = ?
 * </pre>
 * and checks if the ResultSet is empty
 */
public class LocateRequest extends Request
{
    /** JDBC locate statement without locking. */
    private String statementUnlocked;

    /** JDBC locate statement with locking. */
    private String statementLocked;

    /** Definition of mappings in the SQL statement. */
    private StatementClassMapping mappingDefinition;

    /**
     * Constructor, taking the table. Uses the structure of the datastore table to build a basic query.
     * @param table The Class Table representing the datastore table to retrieve
     */
    public LocateRequest(DatastoreClass table)
    {
        super(table);

        RDBMSStoreManager storeMgr = table.getStoreManager();
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        SQLStatement sqlStatement = new SQLStatement(storeMgr, table, null, null);
        mappingDefinition = new StatementClassMapping();
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        JavaTypeMapping m = storeMgr.getMappingManager().getMapping(Integer.class);
        sqlStatement.select(exprFactory.newLiteral(sqlStatement, m, 1), null);

        // Add WHERE clause restricting to the identity of an object
        AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(table.getType(), clr);

        int inputParamNum = 1;
        if (table.getIdentityType() == IdentityType.DATASTORE)
        {
            // Datastore identity value for input
            JavaTypeMapping datastoreIdMapping = table.getDatastoreObjectIdMapping();
            SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), 
                datastoreIdMapping);
            SQLExpression val = exprFactory.newLiteralParameter(sqlStatement, datastoreIdMapping, null, "ID");
            sqlStatement.whereAnd(expr.eq(val), true);

            StatementMappingIndex datastoreIdx =
                mappingDefinition.getMappingForMemberPosition(StatementClassMapping.MEMBER_DATASTORE_ID);
            if (datastoreIdx == null)
            {
                datastoreIdx = new StatementMappingIndex(datastoreIdMapping);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_DATASTORE_ID, datastoreIdx);
            }
            datastoreIdx.addParameterOccurrence(new int[] {inputParamNum});
        }
        else if (table.getIdentityType() == IdentityType.APPLICATION)
        {
            // Application identity value(s) for input
            int[] pkNums = cmd.getPKMemberPositions();
            for (int i=0;i<pkNums.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkNums[i]);
                JavaTypeMapping pkMapping = table.getMemberMappingInDatastoreClass(mmd);
                if (pkMapping == null)
                {
                    pkMapping = table.getMemberMapping(mmd);
                }
                SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(),
                    pkMapping);
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
            SQLExpression tenantExpr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), 
                tenantMapping);
            SQLExpression tenantVal = exprFactory.newLiteral(sqlStatement, tenantMapping,
                storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID));
            sqlStatement.whereAnd(tenantExpr.eq(tenantVal), true);
        }

        // Generate the unlocked and locked JDBC statements
        statementUnlocked = sqlStatement.getSelectStatement().toSQL();
        sqlStatement.addExtension("lock-for-update", Boolean.TRUE);
        statementLocked = sqlStatement.getSelectStatement().toSQL();
    }

    /**
     * Method performing the retrieval of the record from the datastore. 
     * Takes the constructed retrieval query and populates with the specific record information.
     * @param op ObjectProvider for the record to be retrieved
     */
    public void execute(ObjectProvider op)
    {
        if (statementLocked != null)
        {
            ExecutionContext ec = op.getExecutionContext();
            RDBMSStoreManager storeMgr = table.getStoreManager();
            boolean locked = ec.getSerializeReadForClass(op.getClassMetaData().getFullClassName());
            short lockType = ec.getLockManager().getLockMode(op.getInternalObjectId());
            if (lockType != LockManager.LOCK_MODE_NONE)
            {
                if (lockType == LockManager.LOCK_MODE_PESSIMISTIC_READ ||
                    lockType == LockManager.LOCK_MODE_PESSIMISTIC_WRITE)
                {
                    // Override with pessimistic lock
                    locked = true;
                }
            }
            String statement = (locked ? statementLocked : statementUnlocked);

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
                        // Provide the primary key field(s)
                        if (cmd.getIdentityType() == IdentityType.DATASTORE)
                        {
                            StatementMappingIndex datastoreIdx = mappingDefinition.getMappingForMemberPosition(
                                StatementClassMapping.MEMBER_DATASTORE_ID);
                            for (int i=0;i<datastoreIdx.getNumberOfParameterOccurrences();i++)
                            {
                                table.getDatastoreObjectIdMapping().setObject(ec, ps,
                                    datastoreIdx.getParameterPositionsForOccurrence(i), op.getInternalObjectId());
                            }
                        }
                        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                        {
                            op.provideFields(cmd.getPKMemberPositions(),
                                storeMgr.getFieldManagerForStatementGeneration(op, ps, mappingDefinition));
                        }

                        // Execute the statement
                        ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, statement, ps);
                        try
                        {
                            if (!rs.next())
                            {
                                NucleusLogger.DATASTORE_RETRIEVE.info(Localiser.msg("050018", op.getInternalObjectId()));
                                throw new NucleusObjectNotFoundException("No such database row", op.getInternalObjectId());
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
                String msg = Localiser.msg("052220", op.getObjectAsPrintable(), statement, sqle.getMessage());
                NucleusLogger.DATASTORE_RETRIEVE.warn(msg);
                List exceptions = new ArrayList();
                exceptions.add(sqle);
                while ((sqle = sqle.getNextException()) != null)
                {
                    exceptions.add(sqle);
                }
                throw new NucleusDataStoreException(msg, 
                    (Throwable[])exceptions.toArray(new Throwable[exceptions.size()]));
            }
        }
    }
}