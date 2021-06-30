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
2004 Andy Jefferson - localised messages
2005 Andrew Hoffman - changed mapping parent class
2006 Andy Jefferson - add convenience method for updating CLOB
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.column;

import java.io.IOException;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.ColumnDefinitionException;
import org.datanucleus.store.rdbms.fieldmanager.ParameterSetter;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.adapter.OracleAdapter;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.Localiser;

/**
 * Mapping for an Oracle CLOB column.
 * Extends the standard JDBC handler so that we can insert an empty CLOB, and then update it (Oracle non-standard behaviour).
 */
public class OracleClobColumnMapping extends ClobColumnMapping implements ColumnMappingPostSet
{
    public OracleClobColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(mapping, storeMgr, col);
		column = col;
		initialize();
	}

    private void initialize()
    {
		initTypeInfo();
        if (column != null && !column.isUnlimitedLength())
        {
            throw new ColumnDefinitionException("Invalid length specified for CLOB column " + column + ", must be 'unlimited'");
        }
    }

    public String getInsertionInputParameter()
    {
        return "EMPTY_CLOB()";
    }
    
    public boolean includeInFetchStatement()
    {
        return true;
    }

    public String getUpdateInputParameter()
    {
        return "EMPTY_CLOB()";
    }

    /**
     * Accessor for whether this mapping requires values inserting on an INSERT.
     * @return Whether values are to be inserted into this mapping on an INSERT
     */
    public boolean insertValuesOnInsert()
    {
        // We will just insert "EMPTY_CLOB()" above so dont put value in
        return false;
    }

    public String getString(ResultSet rs, int param)
    {
        String value = null;

        try
        {
            char[] cbuf = null;
            java.sql.Clob clob = rs.getClob(param);

            if (clob != null)
            {
                // Note: Using clob.stringValue() results in StoreManagerTest
                // exception: "java.sql.SQLException: Conversion to String failed"

                StringBuilder sbuf = new StringBuilder();
                Reader reader = clob.getCharacterStream();
                try
                {
                    final int BUFF_SIZE = 4096;
                    cbuf = new char[BUFF_SIZE];
                    int charsRead = reader.read(cbuf);

                    while (-1 != charsRead)
                    {
                        sbuf.append(cbuf, 0, charsRead);

                        java.util.Arrays.fill(cbuf, (char)0);
                        charsRead = reader.read(cbuf);
                    }
                }
                catch (IOException e)
                {
                    throw new NucleusDataStoreException("Error reading Oracle CLOB object: param = " + param, e);
                }
                finally
                {
                    try
                    {
                        reader.close();
                    }
                    catch (IOException e)
                    {
                        throw new NucleusDataStoreException("Error reading Oracle CLOB object: param = " + param, e);
                    }
                }

                value = sbuf.toString();

                if (value.length() == 0)
                {
                    value = null;
                }
                else if (value.equals(getDatastoreAdapter().getSurrogateForEmptyStrings()))
                {
                    value = "";
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","String", "" + param), e);
        }

        return value;
    }

    public Object getObject(ResultSet rs, int param)
    {
        return getString(rs, param);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setPostProcessing(ObjectProvider op, Object value)
    {
        String stringValue = (String)value;
        ExecutionContext ec = op.getExecutionContext();
        Table table = column.getTable();
        RDBMSStoreManager storeMgr = table.getStoreManager();

        if (table instanceof DatastoreClass)
        {
            // CLOB within a primary table
            DatastoreClass classTable = (DatastoreClass)table;

            // Generate "SELECT {clobColumn} FROM TABLE WHERE ID=? FOR UPDATE" statement
            SelectStatement sqlStmt = new SelectStatement(storeMgr, table, null, null);
            sqlStmt.setClassLoaderResolver(ec.getClassLoaderResolver());
            sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
            SQLTable blobSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), mapping);
            sqlStmt.select(blobSqlTbl, column, null);
            StatementClassMapping mappingDefinition = new StatementClassMapping();
            AbstractClassMetaData cmd = op.getClassMetaData();
            SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();

            int inputParamNum = 1;
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Datastore identity value for input
                JavaTypeMapping datastoreIdMapping = classTable.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false);
                SQLExpression expr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), datastoreIdMapping);
                SQLExpression val = exprFactory.newLiteralParameter(sqlStmt, datastoreIdMapping, null, "ID");
                sqlStmt.whereAnd(expr.eq(val), true);

                StatementMappingIndex datastoreIdx = mappingDefinition.getMappingForMemberPosition(SurrogateColumnType.DATASTORE_ID.getFieldNumber());
                if (datastoreIdx == null)
                {
                    datastoreIdx = new StatementMappingIndex(datastoreIdMapping);
                    mappingDefinition.addMappingForMember(SurrogateColumnType.DATASTORE_ID.getFieldNumber(), datastoreIdx);
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
                    JavaTypeMapping pkMapping = classTable.getMemberMapping(mmd);
                    SQLExpression expr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), pkMapping);
                    SQLExpression val = exprFactory.newLiteralParameter(sqlStmt, pkMapping, null, "PK" + i);
                    sqlStmt.whereAnd(expr.eq(val), true);

                    StatementMappingIndex pkIdx = mappingDefinition.getMappingForMemberPosition(pkNums[i]);
                    if (pkIdx == null)
                    {
                        pkIdx = new StatementMappingIndex(pkMapping);
                        mappingDefinition.addMappingForMember(pkNums[i], pkIdx);
                    }
                    int[] inputParams = new int[pkMapping.getNumberOfColumnMappings()];
                    for (int j=0;j<pkMapping.getNumberOfColumnMappings();j++)
                    {
                        inputParams[j] = inputParamNum++;
                    }
                    pkIdx.addParameterOccurrence(inputParams);
                }
            }

            String textStmt = sqlStmt.getSQLText().toSQL();

            if (op.isEmbedded())
            {
                // This mapping is embedded, so navigate back to the real owner since that is the "id" in the table
                ObjectProvider[] embeddedOwners = ec.getOwnersForEmbeddedObjectProvider(op);
                if (embeddedOwners != null)
                {
                    // Just use the first owner
                    // TODO Should check if the owner is stored in this table
                    op = embeddedOwners[0];
                }
            }

            try
            {
                ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
                SQLController sqlControl = storeMgr.getSQLController();

                try
                {
                    PreparedStatement ps = sqlControl.getStatementForQuery(mconn, textStmt);

                    try
                    {
                        // Provide the primary key field(s) to the JDBC statement
                        if (cmd.getIdentityType() == IdentityType.DATASTORE)
                        {
                            StatementMappingIndex datastoreIdx = mappingDefinition.getMappingForMemberPosition(SurrogateColumnType.DATASTORE_ID.getFieldNumber());
                            for (int i=0;i<datastoreIdx.getNumberOfParameterOccurrences();i++)
                            {
                                classTable.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false).setObject(ec, ps,
                                    datastoreIdx.getParameterPositionsForOccurrence(i), op.getInternalObjectId());
                            }
                        }
                        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                        {
                            op.provideFields(cmd.getPKMemberPositions(), new ParameterSetter(op, ps, mappingDefinition));
                        }

                        ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, textStmt, ps);
                        try
                        {
                            if (!rs.next())
                            {
                                throw new NucleusObjectNotFoundException(Localiser.msg("050018", IdentityUtils.getPersistableIdentityForId(op.getInternalObjectId())));
                            }

                            DatastoreAdapter dba = storeMgr.getDatastoreAdapter();
                            int jdbcMajorVersion = dba.getDriverMajorVersion();
                            if (dba.getDatastoreDriverName().equalsIgnoreCase(OracleAdapter.OJDBC_DRIVER_NAME) && jdbcMajorVersion < 10)
                            {
                                // Oracle JDBC drivers version 9 and below use some sh*tty Oracle-specific CLOB type
                                oracle.sql.CLOB clob = (oracle.sql.CLOB)rs.getClob(1);
                                if (clob != null)
                                {
                                    clob.putString(1, stringValue); // Deprecated but what can you do
                                }
                            }
                            else
                            {
                                // Oracle JDBC drivers 10 and above supposedly use the JDBC standard class for Clobs
                                java.sql.Clob clob = rs.getClob(1);
                                if (clob != null)
                                {
                                    clob.setString(1, stringValue);
                                }
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
            catch (SQLException e)
            {
                throw new NucleusDataStoreException("Update of CLOB value failed: " + textStmt, e);
            }
        }
        else
        {
            // TODO Support join table
            throw new NucleusDataStoreException("We do not support INSERT/UPDATE CLOB post processing of non-primary table " + table);
        }
    }
}