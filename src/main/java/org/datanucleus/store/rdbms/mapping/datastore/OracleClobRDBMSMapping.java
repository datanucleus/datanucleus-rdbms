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
package org.datanucleus.store.rdbms.mapping.datastore;

import java.io.IOException;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.ColumnDefinitionException;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.adapter.OracleAdapter;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;

/**
 * Mapping for Oracle CLOB type.
 * Extends the standard JDBC handler so that we can insert an empty CLOB, and then update it (Oracle non-standard behaviour).
 */
public class OracleClobRDBMSMapping extends ClobRDBMSMapping
{
    public OracleClobRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
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

    public SQLTypeInfo getTypeInfo()
    {
        return storeMgr.getSQLTypeInfoForJDBCType(Types.CLOB);
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
    /**
     * Convenience method to update the contents of a CLOB column.
     * Oracle requires that a CLOB is initialised with EMPTY_CLOB() and then you retrieve
     * the column and update its CLOB value. Performs a statement
     * <pre>
     * SELECT {clobColumn} FROM TABLE WHERE ID=? FOR UPDATE
     * </pre>
     * and then updates the Clob value returned.
     * @param op ObjectProvider of the object
     * @param table Table storing the CLOB column
     * @param mapping Datastore mapping for the CLOB column
     * @param value The value to store in the CLOB
     * @throws NucleusObjectNotFoundException Thrown if an object is not found
     * @throws NucleusDataStoreException Thrown if an error occurs in datastore communication
     */
    @SuppressWarnings("deprecation")
    public static void updateClobColumn(ObjectProvider op, Table table, DatastoreMapping mapping, String value)
    {
        ExecutionContext ec = op.getExecutionContext();
        RDBMSStoreManager storeMgr = table.getStoreManager();
        DatastoreClass classTable = (DatastoreClass)table; // Don't support join tables yet
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();

        // Generate "SELECT {clobColumn} FROM TABLE WHERE ID=? FOR UPDATE" statement
        SQLStatement sqlStmt = new SQLStatement(storeMgr, table, null, null);
        sqlStmt.setClassLoaderResolver(ec.getClassLoaderResolver());
        sqlStmt.addExtension("lock-for-update", true);
        SQLTable blobSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), mapping.getJavaTypeMapping());
        sqlStmt.select(blobSqlTbl, mapping.getColumn(), null);
        StatementClassMapping mappingDefinition = new StatementClassMapping();
        AbstractClassMetaData cmd = op.getClassMetaData();
        int inputParamNum = 1;
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Datastore identity value for input
            JavaTypeMapping datastoreIdMapping = classTable.getDatastoreIdMapping();
            SQLExpression expr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), 
                datastoreIdMapping);
            SQLExpression val = exprFactory.newLiteralParameter(sqlStmt, datastoreIdMapping, null, "ID");
            sqlStmt.whereAnd(expr.eq(val), true);

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
                JavaTypeMapping pkMapping = classTable.getMemberMapping(mmd);
                SQLExpression expr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(),
                    pkMapping);
                SQLExpression val = exprFactory.newLiteralParameter(sqlStmt, pkMapping, null, "PK" + i);
                sqlStmt.whereAnd(expr.eq(val), true);

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

        String textStmt = sqlStmt.getSelectStatement().toSQL();

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
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();

            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, textStmt);

                try
                {
                    // Provide the primary key field(s) to the JDBC statement
                    if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        StatementMappingIndex datastoreIdx = mappingDefinition.getMappingForMemberPosition(
                            StatementClassMapping.MEMBER_DATASTORE_ID);
                        for (int i=0;i<datastoreIdx.getNumberOfParameterOccurrences();i++)
                        {
                            classTable.getDatastoreIdMapping().setObject(ec, ps,
                                datastoreIdx.getParameterPositionsForOccurrence(i), op.getInternalObjectId());
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        op.provideFields(cmd.getPKMemberPositions(),
                            storeMgr.getFieldManagerForStatementGeneration(op, ps, mappingDefinition));
                    }

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, textStmt, ps);
                    try
                    {
                        if (!rs.next())
                        {
                            throw new NucleusObjectNotFoundException("No such database row", op.getInternalObjectId());
                        }

                        DatastoreAdapter dba = storeMgr.getDatastoreAdapter();
                        int jdbcMajorVersion = dba.getDriverMajorVersion();
                        if (dba.getDatastoreDriverName().equalsIgnoreCase(OracleAdapter.OJDBC_DRIVER_NAME) && jdbcMajorVersion < 10)
                        {
                            // Oracle JDBC drivers version 9 and below use some sh*tty Oracle-specific CLOB type
                            // we have to cast to that, face west, pray whilst saying ommmmmmmmmmm
                            oracle.sql.CLOB clob = (oracle.sql.CLOB)rs.getClob(1);
                            if (clob != null)
                            {
                                clob.putString(1, value); // Deprecated but what can you do
                            }
                        }
                        else
                        {
                            // Oracle JDBC drivers 10 and above supposedly use the JDBC standard class for Clobs
                            java.sql.Clob clob = rs.getClob(1);
                            if (clob != null)
                            {
                                clob.setString(1, value);
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
}