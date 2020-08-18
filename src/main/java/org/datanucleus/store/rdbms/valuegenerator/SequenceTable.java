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
2004 Andy Jefferson - changed to pass Properties rather than MetaData
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.valuegenerator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.store.valuegenerator.ValueGenerationException;
import org.datanucleus.util.Localiser;

/**
 * Class defining a table for storing generated values for use with TableGenerator.
 * The table has 2 columns : a primary key String, and a value Long.
 */ 
public class SequenceTable extends TableImpl
{
    /** Mapping for the sequence name column. */
    private JavaTypeMapping sequenceNameMapping = null;

    /** Mapping for the next value column */
    private JavaTypeMapping nextValMapping = null;

    private String insertStmt = null;
    private String incrementByStmt = null;
    private String deleteStmt = null;
    private String deleteAllStmt = null;
    private String fetchAllStmt = null;
    private String fetchStmt = null;

    /** the sequence column name */
    private String sequenceNameColumnName;

    /** the next value column name */
    private String nextValColumnName;

    /**
     * Constructor
     * @param identifier Datastore identifier for this table
     * @param storeMgr The RDBMSManager for this datastore
     * @param seqNameColName Name for the "sequence name" column
     * @param nextValColName Name for the "next value" column
     **/
    public SequenceTable(DatastoreIdentifier identifier, RDBMSStoreManager storeMgr, String seqNameColName, String nextValColName)
    {
        super(identifier, storeMgr);

        this.sequenceNameColumnName = seqNameColName;
        this.nextValColumnName = nextValColName;
    }

    /**
     * Method to initialise the table.
     * @param clr The ClassLoaderResolver
     **/
    public void initialize(ClassLoaderResolver clr)
    {
        assertIsUninitialized();

        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();

        // "SEQUENCE_NAME" column
        sequenceNameMapping = storeMgr.getMappingManager().getMapping(String.class);
        Column colSequenceName = addColumn(String.class.getName(), idFactory.newColumnIdentifier(sequenceNameColumnName), sequenceNameMapping, null);
        colSequenceName.setPrimaryKey();
        colSequenceName.getColumnMetaData().setLength(Integer.valueOf("255"));
        colSequenceName.getColumnMetaData().setJdbcType(JdbcType.VARCHAR);
		getStoreManager().getMappingManager().createColumnMapping(sequenceNameMapping, colSequenceName, String.class.getName());

        // "NEXT_VAL" column
        nextValMapping = storeMgr.getMappingManager().getMapping(Long.class);
        Column colNextVal = addColumn(Long.class.getName(), idFactory.newColumnIdentifier(nextValColumnName), nextValMapping, null);
		getStoreManager().getMappingManager().createColumnMapping(nextValMapping, colNextVal, Long.class.getName());

        // Set up JDBC statements for supported operations
        insertStmt = "INSERT INTO " + identifier.getFullyQualifiedName(false) + " (" + colSequenceName.getIdentifier() + "," + colNextVal.getIdentifier() + ") VALUES (?,?)";
        incrementByStmt = "UPDATE " + identifier.getFullyQualifiedName(false) + " SET " + colNextVal.getIdentifier() + "=(" + 
            colNextVal.getIdentifier() + "+?) WHERE " + colSequenceName.getIdentifier() + "=?";
        deleteStmt = "DELETE FROM " + identifier.getFullyQualifiedName(false) + " WHERE " + colSequenceName.getIdentifier() + "=?";
        deleteAllStmt = "DELETE FROM " + identifier.getFullyQualifiedName(false);
        fetchStmt = "SELECT " + colNextVal.getIdentifier() + " FROM " + identifier.getFullyQualifiedName(false) + " WHERE " + colSequenceName.getIdentifier() + "=?";
        if (dba.supportsOption(DatastoreAdapter.LOCK_WITH_SELECT_FOR_UPDATE))
        {
            fetchStmt = dba.generateLockWithSelectForUpdate(fetchStmt);
        }
        if (dba.supportsOption(DatastoreAdapter.LOCK_WITH_SELECT_FOR_UPDATE))
        {
            fetchStmt += " FOR UPDATE";
        }
        if (dba.supportsOption(DatastoreAdapter.LOCK_WITH_SELECT_WITH_UPDLOCK))
        {
            String modifier = " with (updlock)";
            int wherePos = fetchStmt.toUpperCase().indexOf(" WHERE ");
            if (wherePos < 0) {
                fetchStmt = fetchStmt + modifier;
            }else{
                fetchStmt = fetchStmt.substring(0, wherePos) + modifier + fetchStmt.substring(wherePos, fetchStmt.length());
            }
        }
        fetchAllStmt = "SELECT " + colNextVal.getIdentifier() + "," + colSequenceName.getIdentifier() + 
            " FROM " + identifier.getFullyQualifiedName(false) + " ORDER BY " + colSequenceName.getIdentifier();

        storeMgr.registerTableInitialized(this);
        state = TABLE_STATE_INITIALIZED;
    }

    /**
     * Accessor for a mapping for the ID (persistable) for this table.
     * @return The (persistable) ID mapping.
     **/
    public JavaTypeMapping getIdMapping()
    {
        throw new NucleusException("Attempt to get ID mapping of Sequence table!").setFatal();
    }

    /**
     * Accessor for the sequences
     * @param conn Connection for this datastore.
     * @return The HashSet of Sequence names
     * @throws SQLException Thrown when an error occurs in the process.
     **/
    public HashSet getFetchAllSequences(ManagedConnection conn)
    throws SQLException
    {
        HashSet sequenceNames = new HashSet();

        PreparedStatement ps = null;
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ps = sqlControl.getStatementForQuery(conn, fetchAllStmt);

            ResultSet rs = sqlControl.executeStatementQuery(null, conn, fetchAllStmt, ps);
            try
            {
	            while (rs.next())
	            {
	                sequenceNames.add(rs.getString(2));
	            }
            }
            finally
            {
                rs.close();
            }
        }
        finally
        {
            if (ps != null)
            {
                sqlControl.closeStatement(conn, ps);
            }
        }

        return sequenceNames;
    }

    /**
     * Accessor for the nextval of a sequence
     * @param conn Connection for this datastore.
     * @param sequenceName The sequence name (the key)
     * @param incrementBy The amount to increment (from the current value)
     * @param tableIdentifier Identifier for the table being incremented (used when there is no current value)
     * @param columnName Name of the column being incremented (used when there is no current value)
     * @param initialValue Initial value (if not using tableIdentifier/columnName to find the initial value)
     * @return The next value that should be used
     * @throws SQLException Thrown when an error occurs in the process.
     **/
    public Long getNextVal(String sequenceName, ManagedConnection conn, int incrementBy, 
            DatastoreIdentifier tableIdentifier, String columnName, int initialValue)
    throws SQLException
    {
        PreparedStatement ps = null;
        Long nextVal = null;
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ps = sqlControl.getStatementForQuery(conn, fetchStmt);
            sequenceNameMapping.setString(null, ps, new int[]{1}, sequenceName);

            ResultSet rs = sqlControl.executeStatementQuery(null, conn, fetchStmt, ps);
            try
            {
                if (!rs.next())
                {
                    // No data in the SEQUENCE_TABLE for this sequence currently
                    boolean addedSequence = false;
                    if (initialValue >= 0)
                    {
                        // Just start at "initialValue" since value provided
                        addSequence(sequenceName, Long.valueOf(incrementBy + initialValue), conn);
                        nextVal = Long.valueOf(initialValue);
                    }
                    else
                    {
                        if (columnName != null && tableIdentifier != null)
                        {
                            // Table/Column specified so find the current max value for the field being generated 
                            PreparedStatement ps2 = null;
                            ResultSet rs2 = null;
                            try
                            {
                                String fetchInitStmt = "SELECT MAX(" + columnName + ") FROM " + tableIdentifier.getFullyQualifiedName(false);
                                ps2 = sqlControl.getStatementForQuery(conn, fetchInitStmt);
                                rs2 = sqlControl.executeStatementQuery(null, conn, fetchInitStmt, ps2);
                                if (rs2.next())
                                {
                                    long val = rs2.getLong(1);
                                    addSequence(sequenceName, Long.valueOf(incrementBy + 1 + val), conn);
                                    nextVal = Long.valueOf(1 + val);
                                    addedSequence = true;
                                }
                            }
                            catch (Exception e)
                            {
                                // Do nothing - since if the table is empty we get this
                            }
                            finally
                            {
                                if (rs2 != null)
                                {
                                    rs2.close();
                                }
                                if (ps2 != null)
                                {
                                    sqlControl.closeStatement(conn, ps2);
                                }
                            }
                        }
                        if (!addedSequence)
                        {
                            // Just start at "initialValue"
                            addSequence(sequenceName, Long.valueOf(incrementBy + 0), conn);
                            nextVal = Long.valueOf(initialValue);
                        }
                    }
                }
                else
                {
                    // Data already exists in sequence table for this key so increment it
                    nextVal = Long.valueOf(rs.getLong(1));
                    incrementSequence(sequenceName, incrementBy, conn);
                }
            }
            finally
            {
                rs.close();
            }
        }
        catch (SQLException e)
        {
           throw new ValueGenerationException(Localiser.msg("061001", e.getMessage()),e);
        }
        finally
        {
            if (ps != null)
            {
                sqlControl.closeStatement(conn, ps);
            }
        }

        return nextVal;
    }

    /**
     * Method to increment a sequence
     * @param conn Connection to the datastore
     * @throws SQLException Thrown when an error occurs incrementing the sequence. 
     **/
    private void incrementSequence(String sequenceName, long incrementBy, ManagedConnection conn)
    throws SQLException
    {
        PreparedStatement ps = null;
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ps = sqlControl.getStatementForUpdate(conn, incrementByStmt, false);
            nextValMapping.setLong(null, ps, new int[] {1}, incrementBy);

            sequenceNameMapping.setString(null, ps, new int[] {2}, sequenceName);

            sqlControl.executeStatementUpdate(null, conn, incrementByStmt, ps, true);

            // TODO : handle any warning messages
        }
        finally
        {
            if (ps != null)
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
    }

    /**
     * Method to insert a row in the SequenceTable
     * @param conn Connection to the datastore
     * @throws SQLException Thrown when an error occurs inserting the sequence. 
     **/
    private void addSequence(String sequenceName, Long nextVal, ManagedConnection conn)
    throws SQLException
    {
        PreparedStatement ps=null;
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ps = sqlControl.getStatementForUpdate(conn, insertStmt, false);
            sequenceNameMapping.setString(null, ps, new int[] {1}, sequenceName);
            nextValMapping.setLong(null, ps, new int[] {2}, nextVal.longValue());

            sqlControl.executeStatementUpdate(null, conn, insertStmt, ps, true);

            // TODO : handle any warning messages
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if (ps != null)
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
    }

    /**
     * Method to delete a sequence.
     * @param sequenceName Name of the sequence
     * @param conn Connection to the datastore
     * @throws SQLException Thrown when an error occurs deleting the schema. 
     **/
    public void deleteSequence(String sequenceName, ManagedConnection conn)
    throws SQLException
    {
        PreparedStatement ps=null;
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ps = sqlControl.getStatementForUpdate(conn, deleteStmt, false);
            ps.setString(1, sequenceName);

            sqlControl.executeStatementUpdate(null, conn, deleteStmt, ps, true);

            // TODO : handle any warning messages
        }
        finally
        {
            if (ps != null)
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
    }

    /**
     * Method to delete all sequences
     *
     * @param conn Connection to the datastore
     * @throws SQLException Thrown when an error occurs deleting. 
     **/
    public void deleteAllSequences(ManagedConnection conn)
    throws SQLException
    {
        PreparedStatement ps=null;
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ps = sqlControl.getStatementForUpdate(conn, deleteAllStmt, false);

            sqlControl.executeStatementUpdate(null, conn, deleteAllStmt, ps, true);

            // TODO : handle any warning messages
        }
        finally
        {
            if (ps != null)
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
    }

    /**
     * Accessor the for the mapping for a field store in this table
     * @param mmd MetaData for the field whose mapping we want
     * @return The mapping
     */
    public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd)
    {
        return null;
    }
}