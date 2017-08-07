/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.table.PersistableJoinTable;
import org.datanucleus.store.types.scostore.PersistableRelationStore;

/**
 * RDBMS implementation of a persistable relation backing store.
 * Represents an N-1 unidirectional join table relation, and manages the access to the join table.
 */
public class JoinPersistableRelationStore implements PersistableRelationStore
{
    /** Manager for the store. */
    protected RDBMSStoreManager storeMgr;

    /** Datastore adapter in use by this store. */
    protected DatastoreAdapter dba;

    /** Mapping to the owner of the relation (which holds the member). */
    protected JavaTypeMapping ownerMapping;

    /** MetaData for the member in the owner. */
    protected AbstractMemberMetaData ownerMemberMetaData;

    /** Table containing the link between owner and related object. */
    protected PersistableJoinTable joinTable;

    /** ClassLoader resolver. */
    protected ClassLoaderResolver clr;

    /** Statement for adding a relation to the join table. */
    protected String addStmt;

    /** Statement for updating a relation to the join table. */
    protected String updateStmt;

    /** Statement for removing a relation from the join table. */
    protected String removeStmt;

    /**
     * Constructor for a persistable relation join store for RDBMS.
     * @param mmd owner member metadata
     * @param joinTable The join table
     * @param clr The ClassLoaderResolver
     */
    public JoinPersistableRelationStore(AbstractMemberMetaData mmd, PersistableJoinTable joinTable, ClassLoaderResolver clr)
    {
        this.storeMgr = joinTable.getStoreManager();
        this.dba = this.storeMgr.getDatastoreAdapter();
        this.ownerMemberMetaData = mmd;
        this.joinTable = joinTable;
        this.clr = clr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.scostore.Store#getStoreManager()
     */
    public StoreManager getStoreManager()
    {
        return storeMgr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.scostore.Store#getOwnerMemberMetaData()
     */
    public AbstractMemberMetaData getOwnerMemberMetaData()
    {
        return ownerMemberMetaData;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.scostore.PersistableRelationStore#add(org.datanucleus.store.ObjectProvider, org.datanucleus.store.ObjectProvider)
     */
    public boolean add(ObjectProvider op1, ObjectProvider op2)
    {
        String addStmt = getAddStmt();
        ExecutionContext ec = op1.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, addStmt, false);
            try
            {
                // Insert the join table row
                int jdbcPosition = 1;
                jdbcPosition = populateOwnerInStatement(op1, ec, ps, jdbcPosition, joinTable);
                BackingStoreHelper.populateElementInStatement(ec, ps, op2.getObject(), jdbcPosition, joinTable.getRelatedMapping());

                // Execute the statement
                int[] nums = sqlControl.executeStatementUpdate(ec, mconn, addStmt, ps, true);
                return (nums != null && nums.length == 1 && nums[0] == 1);
            }
            finally
            {
               sqlControl.closeStatement(mconn, ps);
               mconn.release();
            }
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException("Exception thrown inserting row into persistable relation join table", sqle);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.scostore.PersistableRelationStore#remove(org.datanucleus.store.ObjectProvider)
     */
    public boolean remove(ObjectProvider op)
    {
        String removeStmt = getRemoveStmt();
        ExecutionContext ec = op.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, removeStmt, false);
            try
            {
                // Update the join table row
                int jdbcPosition = 1;
                populateOwnerInStatement(op, ec, ps, jdbcPosition, joinTable);

                // Execute the statement
                int[] nums = sqlControl.executeStatementUpdate(ec, mconn, removeStmt, ps, true);
                return (nums != null && nums.length == 1 && nums[0] == 1);
            }
            finally
            {
               sqlControl.closeStatement(mconn, ps);
               mconn.release();
            }
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException("Exception thrown deleting row from persistable relation join table", sqle);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.scostore.PersistableRelationStore#update(org.datanucleus.store.ObjectProvider, org.datanucleus.store.ObjectProvider)
     */
    public boolean update(ObjectProvider op1, ObjectProvider op2)
    {
        String updateStmt = getUpdateStmt();
        ExecutionContext ec = op1.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, updateStmt, false);
            try
            {
                // Update the join table row
                int jdbcPosition = 1;
                jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, op2.getObject(), jdbcPosition, joinTable.getRelatedMapping());
                populateOwnerInStatement(op1, ec, ps, jdbcPosition, joinTable);

                // Execute the statement
                int[] nums = sqlControl.executeStatementUpdate(ec, mconn, updateStmt, ps, true);
                return (nums != null && nums.length == 1 && nums[0] == 1);
            }
            finally
            {
               sqlControl.closeStatement(mconn, ps);
               mconn.release();
            }
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException("Exception thrown updating row into persistable relation join table", sqle);
        }
    }

    /**
     * Generates the statement for adding items.
     * <PRE>
     * INSERT INTO JOINTABLE (OWNER_COL, RELATED_COL) VALUES (?,?)
     * </PRE>
     * @return The Statement for adding an item
     */
    protected String getAddStmt()
    {
        if (addStmt == null)
        {
            JavaTypeMapping ownerMapping = joinTable.getOwnerMapping();
            JavaTypeMapping relatedMapping = joinTable.getRelatedMapping();

            StringBuilder stmt = new StringBuilder("INSERT INTO ");
            stmt.append(joinTable.toString());
            stmt.append(" (");
            for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
            {
                if (i > 0)
                {
                    stmt.append(",");
                }
                stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            }
            for (int i = 0; i < relatedMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(relatedMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            }

            stmt.append(") VALUES (");
            for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
            {
                if (i > 0)
                {
                    stmt.append(",");
                }
                stmt.append(ownerMapping.getDatastoreMapping(i).getInsertionInputParameter());
            }

            for (int i = 0; i < relatedMapping.getNumberOfDatastoreMappings(); i++)
            {
                stmt.append(",");
                stmt.append(relatedMapping.getDatastoreMapping(0).getInsertionInputParameter());
            }
            stmt.append(") ");

            addStmt = stmt.toString();
        }

        return addStmt;
    }

    /**
     * Generates the statement for updating items.
     * <PRE>
     * UPDATE JOINTABLE SET RELATED_COL = ? WHERE OWNER_COL = ?
     * </PRE>
     * @return The Statement for updating an item
     */
    protected String getUpdateStmt()
    {
        if (updateStmt == null)
        {
            JavaTypeMapping ownerMapping = joinTable.getOwnerMapping();
            JavaTypeMapping relatedMapping = joinTable.getRelatedMapping();

            StringBuilder stmt = new StringBuilder("UPDATE ");
            stmt.append(joinTable.toString());
            stmt.append(" SET ");
            for (int i = 0; i < relatedMapping.getNumberOfDatastoreMappings(); i++)
            {
                if (i > 0)
                {
                    stmt.append(",");
                }
                stmt.append(relatedMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                stmt.append("=");
                stmt.append(ownerMapping.getDatastoreMapping(i).getInsertionInputParameter());
            }
            stmt.append(" WHERE ");
            BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);

            updateStmt = stmt.toString();
        }

        return updateStmt;
    }

    /**
     * Generates the statement for removing items.
     * <PRE>
     * DELETE FROM JOINTABLE WHERE OWNER_COL = ?
     * </PRE>
     * @return The Statement for removing an item
     */
    protected String getRemoveStmt()
    {
        if (removeStmt == null)
        {
            JavaTypeMapping ownerMapping = joinTable.getOwnerMapping();

            StringBuilder stmt = new StringBuilder("DELETE FROM ");
            stmt.append(joinTable.toString());
            stmt.append(" WHERE ");
            BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);

            removeStmt = stmt.toString();
        }

        return removeStmt;
    }

    /**
     * Convenience method to populate the passed PreparedStatement with the value from the owner.
     * @param op ObjectProvider
     * @param ec execution context
     * @param ps The PreparedStatement
     * @param jdbcPosition Position in JDBC statement to populate
     * @param joinTable Join table
     * @return The next position in the JDBC statement
     */
    public static int populateOwnerInStatement(ObjectProvider op, ExecutionContext ec, PreparedStatement ps, int jdbcPosition, PersistableJoinTable joinTable)
    {
        if (!joinTable.getStoreManager().insertValuesOnInsert(joinTable.getOwnerMapping().getDatastoreMapping(0)))
        {
            // Don't try to insert any mappings with insert parameter that isnt ? (e.g Oracle)
            return jdbcPosition;
        }

        if (joinTable.getOwnerMemberMetaData() != null)
        {
            joinTable.getOwnerMapping().setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, joinTable.getOwnerMapping()),
                op.getObject(), op, joinTable.getOwnerMemberMetaData().getAbsoluteFieldNumber());
        }
        else
        {
            joinTable.getOwnerMapping().setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, joinTable.getOwnerMapping()), op.getObject());
        }
        return jdbcPosition + joinTable.getOwnerMapping().getNumberOfDatastoreMappings();
    }
}