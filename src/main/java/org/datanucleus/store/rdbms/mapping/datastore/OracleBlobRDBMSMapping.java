/**********************************************************************
Copyright (c) 2005 Brendan De Beer and others. All rights reserved.
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
2007 Thomas Marti - added handling for String->BLOB mapping
2009 Andy Jefferson - rewrite SQL to use SQLStatement API methods
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import oracle.jdbc.driver.OracleResultSet;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
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
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Maps a Field to an Oracle BLOB.
 * Extends the standard JDBC handler so that we can insert an empty BLOB, and then update it (Oracle non-standard behaviour).
 */
public class OracleBlobRDBMSMapping extends AbstractDatastoreMapping
{
    /**
     * Constructor.
     * @param mapping The Java mapping
     * @param storeMgr Store Manager in use
     * @param col Column
     */
    public OracleBlobRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(storeMgr, mapping);
        column = col;
        initialize();
    }

    /**
     * Creates a OracleBlobRDBMSMapping
     * @param storeMgr Store Manager
     * @param mapping The Java mapping
     */
    protected OracleBlobRDBMSMapping(RDBMSStoreManager storeMgr, JavaTypeMapping mapping)
    {
        super(storeMgr, mapping);
    }

    /**
     * Initialize the mapping.
     */
    private void initialize()
    {
        initTypeInfo();
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping#getInsertionInputParameter()
     */
    public String getInsertionInputParameter()
    {
        return "EMPTY_BLOB()";
    }

    /**
     * Accessor for whether this mapping requires values inserting on an INSERT.
     * @return Whether values are to be inserted into this mapping on an INSERT
     */
    public boolean insertValuesOnInsert()
    {
        // We will just insert "EMPTY_BLOB()" above so don't put value in
        return false;
    }

    /**
     * Returns the object to be loaded from the Orale BLOB.
     * @param rs the ResultSet from the query
     * @param param the index in the query
     * @return the object loaded as a byte[]
     * @throws NucleusDataStoreException Thrown if an error occurs in datastore communication
     */
    public Object getObject(ResultSet rs, int param)
    {
        Object obj = null;

        try
        {
            Blob blob = rs.getBlob(param);
            if (!rs.wasNull())
            {
                byte[] bytes = blob.getBytes(1,(int)blob.length());
                if (bytes.length < 1)
                {
                    return null;
                }
                try
                {
                    if (getJavaTypeMapping().isSerialised())
                    {
                        BlobImpl b = new BlobImpl(bytes);
                        obj = b.getObject();
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.BOOLEAN_ARRAY))
                    {
                        obj = TypeConversionHelper.getBooleanArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.BYTE_ARRAY))
                    {
                        obj = bytes;
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.CHAR_ARRAY))
                    {
                        obj = TypeConversionHelper.getCharArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_STRING))
                    {
                        obj = new String(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.DOUBLE_ARRAY))
                    {
                        obj = TypeConversionHelper.getDoubleArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.FLOAT_ARRAY))
                    {
                        obj = TypeConversionHelper.getFloatArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.INT_ARRAY))
                    {
                        obj = TypeConversionHelper.getIntArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.LONG_ARRAY))
                    {
                        obj = TypeConversionHelper.getLongArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.SHORT_ARRAY))
                    {
                        obj = TypeConversionHelper.getShortArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_BOOLEAN_ARRAY))
                    {
                        obj = TypeConversionHelper.getBooleanObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_BYTE_ARRAY))
                    {
                        obj = TypeConversionHelper.getByteObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_CHARACTER_ARRAY))
                    {
                        obj = TypeConversionHelper.getCharObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_DOUBLE_ARRAY))
                    {
                        obj = TypeConversionHelper.getDoubleObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_FLOAT_ARRAY))
                    {
                        obj = TypeConversionHelper.getFloatObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_INTEGER_ARRAY))
                    {
                        obj = TypeConversionHelper.getIntObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_LONG_ARRAY))
                    {
                        obj = TypeConversionHelper.getLongObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(ClassNameConstants.JAVA_LANG_SHORT_ARRAY))
                    {
                        obj = TypeConversionHelper.getShortObjectArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(BigDecimal[].class.getName()))
                    {
                        return TypeConversionHelper.getBigDecimalArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(BigInteger[].class.getName()))
                    {
                        return TypeConversionHelper.getBigIntegerArrayFromByteArray(bytes);
                    }
                    else if (getJavaTypeMapping().getType().equals(java.util.BitSet.class.getName()))
                    {
                        return TypeConversionHelper.getBitSetFromBooleanArray(TypeConversionHelper.getBooleanArrayFromByteArray(bytes));
                    }
                    else
                    {
                        obj = new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
                    }
                }
                catch (StreamCorruptedException e)
                {
                    String msg = "StreamCorruptedException: object is corrupted";
                    NucleusLogger.DATASTORE.error(msg);
                    throw new NucleusUserException(msg, e).setFatal();
                }
                catch (IOException e)
                {
                    String msg = "IOException: error when reading object";
                    NucleusLogger.DATASTORE.error(msg);
                    throw new NucleusUserException(msg, e).setFatal();
                }
                catch (ClassNotFoundException e)
                {
                    String msg = "ClassNotFoundException: error when creating object";
                    NucleusLogger.DATASTORE.error(msg);
                    throw new NucleusUserException(msg, e).setFatal();
                }
            }
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, sqle.getMessage()), sqle);
        }

        return obj;
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping#getString(ResultSet, int)
     */
    public String getString(ResultSet resultSet, int exprIndex)
    {
        return (String)getObject(resultSet, exprIndex);
    }

    public int getJDBCType()
    {
        return Types.BLOB;
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping#getUpdateInputParameter()
     */
    public String getUpdateInputParameter()
    {
        return "EMPTY_BLOB()";
    }

    /**
     * Whether to include this mapping in a fetch statement.
     * @return Whether to include it when fetching the object
     */
    public boolean includeInSQLFetchStatement()
    {
        return true;
    }

    /**
     * Convenience method to update the contents of a BLOB column.
     * Oracle requires that a BLOB is initialised with EMPTY_BLOB() and then you retrieve
     * the column and update its BLOB value. Performs a statement
     * <pre>
     * SELECT {blobColumn} FROM TABLE WHERE ID=? FOR UPDATE
     * </pre>
     * and then updates the Blob value returned.
     * @param sm ObjectProvider of the object
     * @param table Table storing the BLOB column
     * @param mapping Datastore mapping for the BLOB column
     * @param bytes The bytes to store in the BLOB
     * @throws NucleusObjectNotFoundException thrown if an object isnt found
     * @throws NucleusDataStoreException thrown if an error occurs in datastore communication
     */
    @SuppressWarnings("deprecation")
    public static void updateBlobColumn(ObjectProvider sm, Table table,
            DatastoreMapping mapping, byte[] bytes)
    {
        ExecutionContext ec = sm.getExecutionContext();
        RDBMSStoreManager storeMgr = table.getStoreManager();
        DatastoreClass classTable = (DatastoreClass)table; // Don't support join tables yet
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();

        // Generate "SELECT {blobColumn} FROM TABLE WHERE ID=? FOR UPDATE" statement
        SelectStatement sqlStmt = new SelectStatement(storeMgr, table, null, null);
        sqlStmt.setClassLoaderResolver(ec.getClassLoaderResolver());
        sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
        SQLTable blobSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), mapping.getJavaTypeMapping());
        sqlStmt.select(blobSqlTbl, mapping.getColumn(), null);
        StatementClassMapping mappingDefinition = new StatementClassMapping();
        AbstractClassMetaData cmd = sm.getClassMetaData();
        int inputParamNum = 1;
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Datastore identity value for input
            JavaTypeMapping datastoreIdMapping = classTable.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false);
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

        String textStmt = sqlStmt.getSQLText().toSQL();

        if (sm.isEmbedded())
        {
            // This mapping is embedded, so navigate back to the real owner since that is the "id" in the table
            ObjectProvider[] embeddedOwners = ec.getOwnersForEmbeddedObjectProvider(sm);
            if (embeddedOwners != null)
            {
                // Just use the first owner
                // TODO Should check if the owner is stored in this table
                sm = embeddedOwners[0];
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
                            classTable.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false).setObject(ec, ps,
                                datastoreIdx.getParameterPositionsForOccurrence(i), sm.getInternalObjectId());
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        sm.provideFields(cmd.getPKMemberPositions(),
                            storeMgr.getFieldManagerForStatementGeneration(sm, ps, mappingDefinition));
                    }

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, textStmt, ps);

                    try
                    {
                        if (!rs.next())
                        {
                            throw new NucleusObjectNotFoundException("No such database row", sm.getInternalObjectId());
                        }

                        DatastoreAdapter dba = storeMgr.getDatastoreAdapter();
                        int jdbcMajorVersion = dba.getDriverMajorVersion();
                        if (dba.getDatastoreDriverName().equalsIgnoreCase(OracleAdapter.OJDBC_DRIVER_NAME) && jdbcMajorVersion < 10)
                        {
                            // Oracle JDBC drivers version 9 and below use some sh*tty Oracle-specific BLOB type
                            // we have to cast to that, face west, pray whilst saying ommmmmmmmmmm
                            oracle.sql.BLOB blob = null;
                            if (jdbcMajorVersion <= 8)
                            {
                                OracleResultSet ors = (OracleResultSet)rs;
                                blob = ors.getBLOB(1);
                            }
                            else
                            {
                                blob = (oracle.sql.BLOB)rs.getBlob(1);
                            }

                            if (blob != null)
                            {
                                blob.putBytes(1, bytes); // Deprecated but what can you do
                            }
                        }
                        else
                        {
                            // Oracle JDBC drivers 10 and above supposedly use the JDBC standard class for Blobs
                            java.sql.Blob blob = rs.getBlob(1);
                            if (blob != null)
                            {
                                blob.setBytes(1, bytes);
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
            throw new NucleusDataStoreException("Update of BLOB value failed: " + textStmt, e);
        }
    }
}