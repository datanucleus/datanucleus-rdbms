/**********************************************************************
Copyright (c) 2012 Andy Jefferson and others. All rights reserved.
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
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.LockMode;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.fieldmanager.ParameterSetter;
import org.datanucleus.store.rdbms.fieldmanager.ResultSetGetter;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Request to locate a series of records in the data store (all present in the same table). 
 * Performs an SQL statement like
 * <pre>
 * SELECT ID [,FIELD1,FIELD2] FROM CANDIDATE_TABLE WHERE ID = ? OR ID = ? OR ID = ?
 * </pre>
 */
public class LocateBulkRequest extends BulkRequest
{
    AbstractClassMetaData cmd = null;

    /** Definition of input mappings in the SQL statement. */
    private StatementClassMapping[] mappingDefinitions;

    /** Result mapping for the SQL statement. */
    private StatementClassMapping resultMapping;

    /**
     * Constructor, taking the table. Uses the structure of the datastore table to build a basic query.
     * @param table The Class Table representing the datastore table to retrieve
     */
    public LocateBulkRequest(DatastoreClass table)
    {
        super(table);
    }

    protected String getStatement(DatastoreClass table, ObjectProvider[] ops, boolean lock)
    {
        RDBMSStoreManager storeMgr = table.getStoreManager();
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        cmd = storeMgr.getMetaDataManager().getMetaDataForClass(table.getType(), clr);
        ExecutionContext ec = ops[0].getExecutionContext();

        SelectStatement sqlStatement = new SelectStatement(storeMgr, table, null, null);

        // SELECT fields we require
        resultMapping = new StatementClassMapping();

        // a). PK fields
        if (table.getIdentityType() == IdentityType.DATASTORE)
        {
            JavaTypeMapping datastoreIdMapping = table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false);
            SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), datastoreIdMapping);
            int[] cols = sqlStatement.select(expr, null);
            StatementMappingIndex datastoreIdx = new StatementMappingIndex(datastoreIdMapping);
            datastoreIdx.setColumnPositions(cols);
            resultMapping.addMappingForMember(SurrogateColumnType.DATASTORE_ID.getFieldNumber(), datastoreIdx);
        }
        else if (table.getIdentityType() == IdentityType.APPLICATION)
        {
            int[] pkNums = cmd.getPKMemberPositions();
            for (int i=0;i<pkNums.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkNums[i]);
                JavaTypeMapping pkMapping = table.getMemberMappingInDatastoreClass(mmd);
                if (pkMapping == null)
                {
                    pkMapping = table.getMemberMapping(mmd);
                }
                SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), pkMapping);
                int[] cols = sqlStatement.select(expr, null);
                StatementMappingIndex pkIdx = new StatementMappingIndex(pkMapping);
                pkIdx.setColumnPositions(cols);
                resultMapping.addMappingForMember(mmd.getAbsoluteFieldNumber(), pkIdx);
            }
        }
        else
        {
            throw new NucleusUserException("Cannot locate objects using nondurable identity");
        }

        JavaTypeMapping verMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
        if (verMapping != null)
        {
            VersionMetaData currentVermd = table.getVersionMetaData();
            if (currentVermd != null && currentVermd.getFieldName() == null)
            {
                // Surrogate version column
                SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), verMapping);
                int[] cols = sqlStatement.select(expr, null);
                StatementMappingIndex mapIdx = new StatementMappingIndex(verMapping);
                mapIdx.setColumnPositions(cols);
                resultMapping.addMappingForMember(SurrogateColumnType.VERSION.getFieldNumber(), mapIdx);
            }
        }

        int[] nonPkFieldNums = cmd.getNonPKMemberPositions();
        if (nonPkFieldNums != null)
        {
            for (int i=0;i<nonPkFieldNums.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(nonPkFieldNums[i]);
                JavaTypeMapping mapping = table.getMemberMapping(mmd);
                if (mapping != null && mapping.includeInFetchStatement())
                {
                    if (mapping instanceof PersistableMapping)
                    {
                        // Ignore 1-1/N-1 for now
                        continue;
                    }

                    SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), mapping);
                    int[] cols = sqlStatement.select(expr, null);
                    StatementMappingIndex mapIdx = new StatementMappingIndex(mapping);
                    mapIdx.setColumnPositions(cols);
                    resultMapping.addMappingForMember(mmd.getAbsoluteFieldNumber(), mapIdx);
                }
            }
        }

        JavaTypeMapping multitenancyMapping = table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false);
        if (multitenancyMapping != null)
        {
            // Add WHERE clause restricting to tenant
            SQLExpression tenantExpr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), multitenancyMapping);
            SQLExpression tenantVal = exprFactory.newLiteral(sqlStatement, multitenancyMapping, ec.getNucleusContext().getMultiTenancyId(ec, cmd));
            sqlStatement.whereAnd(tenantExpr.eq(tenantVal), true);
        }

        JavaTypeMapping softDeleteMapping = table.getSurrogateMapping(SurrogateColumnType.SOFTDELETE, false);
        if (softDeleteMapping != null)
        {
            // Add WHERE clause restricting to soft-delete unset
            SQLExpression softDeleteExpr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), softDeleteMapping);
            SQLExpression softDeleteVal = exprFactory.newLiteral(sqlStatement, softDeleteMapping, Boolean.FALSE);
            sqlStatement.whereAnd(softDeleteExpr.eq(softDeleteVal), true);
        }

        // Add WHERE clause restricting to the identities of the objects
        mappingDefinitions = new StatementClassMapping[ops.length];
        int inputParamNum = 1;
        for (int i=0;i<ops.length;i++)
        {
            mappingDefinitions[i] = new StatementClassMapping();
            if (table.getIdentityType() == IdentityType.DATASTORE)
            {
                // Datastore identity value for input
                JavaTypeMapping datastoreIdMapping = table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false);
                SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), datastoreIdMapping);
                SQLExpression val = exprFactory.newLiteralParameter(sqlStatement, datastoreIdMapping, null, "ID");
                sqlStatement.whereOr(expr.eq(val), true);

                StatementMappingIndex datastoreIdx = new StatementMappingIndex(datastoreIdMapping);
                mappingDefinitions[i].addMappingForMember(SurrogateColumnType.DATASTORE_ID.getFieldNumber(), datastoreIdx);
                datastoreIdx.addParameterOccurrence(new int[] {inputParamNum++});
            }
            else if (table.getIdentityType() == IdentityType.APPLICATION)
            {
                // Application identity value(s) for input
                BooleanExpression pkExpr = null;
                int[] pkNums = cmd.getPKMemberPositions();
                for (int j=0;j<pkNums.length;j++)
                {
                    AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkNums[j]);
                    JavaTypeMapping pkMapping = table.getMemberMappingInDatastoreClass(mmd);
                    if (pkMapping == null)
                    {
                        pkMapping = table.getMemberMapping(mmd);
                    }
                    SQLExpression expr = exprFactory.newExpression(sqlStatement, sqlStatement.getPrimaryTable(), pkMapping);
                    SQLExpression val = exprFactory.newLiteralParameter(sqlStatement, pkMapping, null, "PK" + j);
                    BooleanExpression fieldEqExpr = expr.eq(val);
                    if (pkExpr == null)
                    {
                        pkExpr = fieldEqExpr;
                    }
                    else
                    {
                        pkExpr = pkExpr.and(fieldEqExpr);
                    }

                    StatementMappingIndex pkIdx = new StatementMappingIndex(pkMapping);
                    mappingDefinitions[i].addMappingForMember(mmd.getAbsoluteFieldNumber(), pkIdx);
                    int[] inputParams = new int[pkMapping.getNumberOfDatastoreMappings()];
                    for (int k=0;k<pkMapping.getNumberOfDatastoreMappings();k++)
                    {
                        inputParams[k] = inputParamNum++;
                    }
                    pkIdx.addParameterOccurrence(inputParams);
                }
                if (pkExpr == null)
                {
                    throw new NucleusException("Unable to generate PK expression for WHERE clause of locate statement");
                }

                pkExpr = (BooleanExpression)pkExpr.encloseInParentheses();
                sqlStatement.whereOr(pkExpr, true);
            }
        }

        // Generate the appropriate JDBC statement allowing for locking
        if (lock)
        {
            sqlStatement.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, Boolean.TRUE);
            return sqlStatement.getSQLText().toSQL();
        }
        return sqlStatement.getSQLText().toSQL();
    }

    /**
     * Method performing the location of the records in the datastore. 
     * @param ops ObjectProviders to be located
     * @throws NucleusObjectNotFoundException with nested exceptions for each of missing objects (if any)
     */
    public void execute(ObjectProvider[] ops)
    {
        if (ops == null || ops.length == 0)
        {
            return;
        }

        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuilder str = new StringBuilder();
            for (int i=0;i<ops.length;i++)
            {
                if (i > 0)
                {
                    str.append(", ");
                }
                str.append(ops[i].getInternalObjectId());
            }
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("052223", str.toString(), table));
        }

        ExecutionContext ec = ops[0].getExecutionContext();
        RDBMSStoreManager storeMgr = table.getStoreManager();
        AbstractClassMetaData cmd = ops[0].getClassMetaData();
        boolean locked = ec.getSerializeReadForClass(cmd.getFullClassName());
        LockMode lockType = ec.getLockManager().getLockMode(ops[0].getInternalObjectId());
        if (lockType != LockMode.LOCK_NONE)
        {
            if (lockType == LockMode.LOCK_PESSIMISTIC_READ || lockType == LockMode.LOCK_PESSIMISTIC_WRITE)
            {
                // Override with pessimistic lock
                locked = true;
            }
        }
        String statement = getStatement(table, ops, locked);

        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, statement);

                try
                {
                    // Provide the primary key field(s)
                    for (int i=0;i<ops.length;i++)
                    {
                        if (cmd.getIdentityType() == IdentityType.DATASTORE)
                        {
                            StatementMappingIndex datastoreIdx = mappingDefinitions[i].getMappingForMemberPosition(SurrogateColumnType.DATASTORE_ID.getFieldNumber());
                            for (int j=0;j<datastoreIdx.getNumberOfParameterOccurrences();j++)
                            {
                                table.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false).setObject(ec, ps, datastoreIdx.getParameterPositionsForOccurrence(j), 
                                    ops[i].getInternalObjectId());
                            }
                        }
                        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                        {
                            ops[i].provideFields(cmd.getPKMemberPositions(), new ParameterSetter(ops[i], ps, mappingDefinitions[i]));
                        }
                    }

                    // Execute the statement
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, statement, ps);
                    try
                    {
                        ObjectProvider[] missingOps = processResults(rs, ops);
                        if (missingOps != null && missingOps.length > 0)
                        {
                            NucleusObjectNotFoundException[] nfes = new NucleusObjectNotFoundException[missingOps.length];
                            for (int i=0;i<nfes.length;i++)
                            {
                                nfes[i] = new NucleusObjectNotFoundException("Object not found", missingOps[i].getInternalObjectId());
                            }
                            throw new NucleusObjectNotFoundException("Some objects were not found. Look at nested exceptions for details", nfes);
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
            String msg = Localiser.msg("052220", ops[0].getObjectAsPrintable(), statement, sqle.getMessage());
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

    private ObjectProvider[] processResults(ResultSet rs, ObjectProvider[] ops)
    throws SQLException
    {
        List<ObjectProvider> missingOps = new ArrayList<>();
        for (int i=0;i<ops.length;i++)
        {
            missingOps.add(ops[i]);
        }

        ExecutionContext ec = ops[0].getExecutionContext();
        while (rs.next())
        {
            FieldManager resultFM = new ResultSetGetter(ec, rs, resultMapping, cmd);
            Object id = null;
            Object key = null;
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                StatementMappingIndex idx = resultMapping.getMappingForMemberPosition(SurrogateColumnType.DATASTORE_ID.getFieldNumber());
                JavaTypeMapping idMapping = idx.getMapping();
                key = idMapping.getObject(ec, rs, idx.getColumnPositions());
                if (IdentityUtils.isDatastoreIdentity(key))
                {
                    // If mapping is OIDMapping then returns an OID rather than the column value
                    key = IdentityUtils.getTargetKeyForDatastoreIdentity(key);
                }
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                if (cmd.usesSingleFieldIdentityClass())
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions();
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[0]);
                    if (pkMmd.getType() == int.class)
                    {
                        key = resultFM.fetchIntField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == short.class)
                    {
                        key = resultFM.fetchShortField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == long.class)
                    {
                        key = resultFM.fetchLongField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == char.class)
                    {
                        key = resultFM.fetchCharField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == boolean.class)
                    {
                        key = resultFM.fetchBooleanField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == byte.class)
                    {
                        key = resultFM.fetchByteField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == double.class)
                    {
                        key = resultFM.fetchDoubleField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == float.class)
                    {
                        key = resultFM.fetchFloatField(pkFieldNums[0]);
                    }
                    else if (pkMmd.getType() == String.class)
                    {
                        key = resultFM.fetchStringField(pkFieldNums[0]);
                    }
                    else
                    {
                        key = resultFM.fetchObjectField(pkFieldNums[0]);
                    }
                }
                else
                {
                    id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, true, resultFM);
                }
            }

            // Find which ObjectProvider this row is for
            ObjectProvider op = null;
            for (ObjectProvider missingOp : missingOps)
            {
                Object opId = missingOp.getInternalObjectId();
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    Object opKey = IdentityUtils.getTargetKeyForDatastoreIdentity(opId);
                    if (key != null && opKey.getClass() != key.getClass())
                    {
                        opKey = TypeConversionHelper.convertTo(opKey, key.getClass());
                    }
                    if (opKey.equals(key))
                    {
                        op = missingOp;
                        break;
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    if (cmd.usesSingleFieldIdentityClass())
                    {
                        Object opKey = IdentityUtils.getTargetKeyForSingleFieldIdentity(opId);
                        if (opKey.equals(key))
                        {
                            op = missingOp;
                            break;
                        }
                    }
                    else
                    {
                        if (opId.equals(id))
                        {
                            op = missingOp;
                            break;
                        }
                    }
                }
            }
            if (op != null)
            {
                // Mark ObjectProvider as processed
                missingOps.remove(op);

                // Load up any unloaded fields that we have selected
                int[] selectedMemberNums = resultMapping.getMemberNumbers();
                int[] unloadedMemberNums = ClassUtils.getFlagsSetTo(op.getLoadedFields(), selectedMemberNums, false);
                if (unloadedMemberNums != null && unloadedMemberNums.length > 0)
                {
                    op.replaceFields(unloadedMemberNums, resultFM);
                }

                // Load version if present and not yet set
                JavaTypeMapping versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, false);
                if (op.getTransactionalVersion() == null && versionMapping != null)
                {
                    VersionMetaData currentVermd = table.getVersionMetaData();
                    Object datastoreVersion = null;
                    if (currentVermd != null)
                    {
                        if (currentVermd.getFieldName() == null)
                        {
                            // Surrogate version
                            versionMapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, true); // Why use true now?
                            StatementMappingIndex verIdx = resultMapping.getMappingForMemberPosition(SurrogateColumnType.VERSION.getFieldNumber());
                            datastoreVersion = versionMapping.getObject(ec, rs, verIdx.getColumnPositions());
                        }
                        else
                        {
                            datastoreVersion = op.provideField(cmd.getAbsolutePositionOfMember(currentVermd.getFieldName()));
                        }
                        op.setVersion(datastoreVersion);
                    }
                }
            }
        }

        if (!missingOps.isEmpty())
        {
            return missingOps.toArray(new ObjectProvider[missingOps.size()]);
        }
        return null;
    }
}