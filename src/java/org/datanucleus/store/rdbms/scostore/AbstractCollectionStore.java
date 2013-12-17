/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.scostore.CollectionStore;

/**
 * Abstract representation of a store of a Collection.
 * Contains all common parts of storing Sets and Lists.
 */
public abstract class AbstractCollectionStore extends ElementContainerStore implements CollectionStore
{
    protected String containsStmt;

    /**
     * Constructor.
     * @param storeMgr Manager for the store
     * @param clr ClassLoader resolver
     */
    protected AbstractCollectionStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    /**
     * Method to update a field of an embedded element.
     * @param op ObjectProvider of the owner
     * @param element The element to update
     * @param fieldNumber The number of the field to update
     * @param value The value
     * @return true if the datastore was updated
     */
    public boolean updateEmbeddedElement(ObjectProvider op, Object element, int fieldNumber, Object value)
    {
        boolean modified = false;
        if (elementMapping != null && elementMapping instanceof EmbeddedElementPCMapping)
        {
            String fieldName = emd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getName();
            if (fieldName == null)
            {
                // We have no mapping for this field so presumably is the owner field or a PK field
                return false;
            }
            JavaTypeMapping fieldMapping = ((EmbeddedElementPCMapping)elementMapping).getJavaTypeMapping(fieldName);
            if (fieldMapping == null)
            {
                // We have no mapping for this field so presumably is the owner field or a PK field
                return false;
            }
            modified = updateEmbeddedElement(op, element, fieldNumber, value, fieldMapping);
        }

        return modified;
    }

    /**
     * Method to update the collection to be the supplied collection of elements.
     * @param op ObjectProvider of the object
     * @param coll The collection to use
     */
    public void update(ObjectProvider op, Collection coll)
    {
        // Crude update - remove existing and add new!
        clear(op);
        addAll(op, coll, 0);
    }

    /**
     * Method to verify if the association owner vs elements contains
     * a specific element in the association 
     * @param op ObjectProvider
     * @param element The element
     * @return Whether it contains the element 
     */
    public boolean contains(ObjectProvider op, Object element)
    {
        if (!validateElementForReading(op, element))
        {
            return false;
        }
        return containsInternal(op, element);
    }

    /**
     * Generate statement for update the field of an embedded element.
     * <PRE>
     * UPDATE SETTABLE
     * SET EMBEDDEDFIELD1 = ?
     * WHERE OWNERCOL=?
     * AND ELEMENTCOL = ?
     * </PRE>
     *
     * @param fieldMapping The mapping for the field within the embedded object to be updated
     * @return Statement for updating an embedded element in the Set
     */
    protected String getUpdateEmbeddedElementStmt(JavaTypeMapping fieldMapping)
    {
        JavaTypeMapping ownerMapping = getOwnerMapping();
        Table containerTable = getContainerTable();
        JavaTypeMapping elementMapping = getElementMapping();

        StringBuilder stmt = new StringBuilder("UPDATE ");
        stmt.append(containerTable.toString());
        stmt.append(" SET ");
        for (int i = 0; i < fieldMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(fieldMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            stmt.append(" = ");
            stmt.append(((AbstractDatastoreMapping) fieldMapping.getDatastoreMapping(i)).getUpdateInputParameter());
        }

        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);

        EmbeddedElementPCMapping embeddedMapping = (EmbeddedElementPCMapping) elementMapping;
        for (int i = 0; i < embeddedMapping.getNumberOfJavaTypeMappings(); i++)
        {
            JavaTypeMapping m = embeddedMapping.getJavaTypeMapping(i);
            if (m != null)
            {
                for (int j = 0; j < m.getNumberOfDatastoreMappings(); j++)
                {
                    stmt.append(" AND ");
                    stmt.append(m.getDatastoreMapping(j).getColumn().getIdentifier().toString());
                    stmt.append(" = ");
                    stmt.append(((AbstractDatastoreMapping) m.getDatastoreMapping(j)).getUpdateInputParameter());
                }
            }
        }
        return stmt.toString();
    }

    public boolean updateEmbeddedElement(ObjectProvider op, Object element, int fieldNumber, Object value, 
            JavaTypeMapping fieldMapping)
    {
        boolean modified = false;
        String stmt = getUpdateEmbeddedElementStmt(fieldMapping);
        try
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();

            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, stmt, false);
                try
                {
                    int jdbcPosition = 1;
                    fieldMapping.setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, fieldMapping), value);
                    jdbcPosition += fieldMapping.getNumberOfDatastoreMappings();
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    jdbcPosition = BackingStoreHelper.populateEmbeddedElementFieldsInStatement(op, element, 
                        ps, jdbcPosition, ((JoinTable) getContainerTable()).getOwnerMemberMetaData(),
                        getElementMapping(), getEmd(), this);

                    sqlControl.executeStatementUpdate(ec, mconn, stmt, ps, true);
                    modified = true;
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
            e.printStackTrace();
            // TODO Update this localised message to reflect that it is the update of an embedded element
            throw new NucleusDataStoreException(LOCALISER.msg("056009", stmt), e);
        }
        return modified;
    }

    /**
     * Generate statement for retrieving the contents of the Collection.
     * The discriminator part is only present when the element type has
     * inheritance strategy of "superclass-table" and is Inverse.
     * <PRE>
     * SELECT OWNERCOL FROM COLLECTIONTABLE
     * WHERE OWNERCOL=?
     * AND ELEMENTCOL=?
     * [AND DISCRIMINATOR=?]
     * [AND RELATION_DISCRIM=?]
     * </PRE>
     *
     * @return Statement for retrieving the contents of the Collection.
     */
    private String getContainsStmt(Object element)
    {
        if (elementMapping instanceof ReferenceMapping && elementMapping.getNumberOfDatastoreMappings() > 1)
        {
            // The statement is based on the element passed in so don't cache
            return getContainsStatementString(element);
        }

        if (containsStmt == null)
        {
            synchronized (this)
            {
                containsStmt = getContainsStatementString(element);
            }
        }
        return containsStmt;
    }

    private String getContainsStatementString(Object element)
    {
        JavaTypeMapping ownerMapping = getOwnerMapping();
        Table containerTable = getContainerTable();
        boolean elementsAreSerialised = isElementsAreSerialised();
        JavaTypeMapping elementMapping = getElementMapping();
        ElementContainerStore.ElementInfo[] elementInfo = getElementInfo();

        StringBuilder stmt = new StringBuilder("SELECT ");
        String containerAlias = "THIS";
        String joinedElementAlias = "ELEM";
        for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
        }
        stmt.append(" FROM ");
        stmt.append(getContainerTable().toString()).append(" ").append(containerAlias);

        // Add join to element table if required (only allows for 1 element table currently)
        boolean joinedDiscrim = false;
        // TODO Enable this code applying the discrim restriction to JoinTable cases
        /*if (elementInfo != null && elementInfo[0].getTable() != containerTable && elementInfo[0].getDiscriminatorMapping() != null)
        {
            // Need join to the element table to restrict the discriminator
            joinedDiscrim = true;
            JavaTypeMapping elemIdMapping = elementInfo[0].getTable().getIdMapping();
            stmt.append(" INNER JOIN ");
            stmt.append(elementInfo[0].getTable().toString()).append(" ").append(joinedElementAlias).append(" ON ");
            for (int i=0;i<elementMapping.getNumberOfDatastoreFields();i++)
            {
                if (i > 0)
                {
                    stmt.append(" AND ");
                }
                stmt.append(containerAlias).append(".").append(elementMapping.getDataStoreMapping(i).getDatastoreField().getIdentifier());
                stmt.append("=");
                stmt.append(joinedElementAlias).append(".").append(elemIdMapping.getDataStoreMapping(0).getDatastoreField().getIdentifier());
            }
        }*/

        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, containerAlias, true);
        BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, elementsAreSerialised,
            containerAlias, false);

        // TODO Remove the "containerTable == " clause and make discriminator restriction part of the JoinTable statement too
        // Needs to pass TCK M-M relationship test. see contains(ObjectProvider, Object) method also
        if (elementInfo != null && containerTable == elementInfo[0].getDatastoreClass() &&
                elementInfo[0].getDiscriminatorMapping() != null)
        {
            // Element table has discriminator so restrict to the element-type and subclasses
            stmt.append(" AND (");

            // Add WHERE for the element and each subclass type so we restrict to valid element types
            Collection<String> subclasses = storeMgr.getSubClassesForClass(elementInfo[0].getClassName(), true, clr);
            for (int i = 0; i < subclasses.size() + 1; i++)
            {
                JavaTypeMapping discrimMapping = elementInfo[0].getDiscriminatorMapping();
                for (int j = 0; j < discrimMapping.getNumberOfDatastoreMappings(); j++)
                {
                    if (joinedDiscrim)
                    {
                        stmt.append(joinedElementAlias);
                    }
                    else
                    {
                        stmt.append(containerAlias);
                    }
                    stmt.append(".").append(discrimMapping.getDatastoreMapping(j).getColumn().getIdentifier().toString());
                    stmt.append(" = ");
                    stmt.append(((AbstractDatastoreMapping) discrimMapping.getDatastoreMapping(j)).getUpdateInputParameter());

                    if (j != discrimMapping.getNumberOfDatastoreMappings() - 1 || i != subclasses.size())
                    {
                        stmt.append(" OR ");
                    }
                }
            }
            stmt.append(")");
        }

        if (relationDiscriminatorMapping != null)
        {
            // Relation uses shared resource (FK, JoinTable) so restrict to this particular relation
            BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping,
                containerAlias, false);
        }

        return stmt.toString();
    }
    protected boolean containsInternal(ObjectProvider op, Object element)
    {
        boolean retval;

        Table containerTable = getContainerTable();
        JavaTypeMapping elementMapping = getElementMapping();
        ElementContainerStore.ElementInfo[] elementInfo = getElementInfo();
        String stmt = getContainsStmt(element);
        try
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                try
                {
                    int jdbcPosition = 1;
                    // Populate the owner
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);

                    // Populate element
                    jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element,
                        jdbcPosition, elementMapping);

                    // TODO Remove the containerTable == part of this so that the discrim restriction applies to JoinTable case too
                    // Needs to pass TCK M-M relation test
                    if (elementInfo != null && elementInfo[0].getDiscriminatorMapping() != null && 
                        elementInfo[0].getDatastoreClass() == containerTable)
                    {
                        jdbcPosition = BackingStoreHelper.populateElementDiscriminatorInStatement(ec, ps, jdbcPosition,
                                true, elementInfo[0], clr);
                    }
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        retval = rs.next();
                        JDBCUtils.logWarnings(rs);
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
            throw new NucleusDataStoreException(LOCALISER.msg("056008", stmt), e);
        }
        return retval;
    }

    public int[] internalRemove(ObjectProvider op, ManagedConnection conn, boolean batched, Object element, 
            boolean executeNow) throws MappedDatastoreException
    {
        ExecutionContext ec = op.getExecutionContext();
        SQLController sqlControl = storeMgr.getSQLController();
        String removeStmt = getRemoveStmt(element);
        try
        {
            PreparedStatement ps = sqlControl.getStatementForUpdate(conn, removeStmt, batched);
            try
            {
                int jdbcPosition = 1;

                jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, element, 
                    jdbcPosition, elementMapping);
                if (relationDiscriminatorMapping != null)
                {
                    jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                }

                // Execute the statement
                return sqlControl.executeStatementUpdate(ec, conn, removeStmt, ps, executeNow);
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }
        }
        catch (SQLException sqle)
        {
            throw new MappedDatastoreException("SQLException", sqle);
        }
    }

    /**
     * Generate statement for removing an element from the Collection.
     * <PRE>
     * DELETE FROM COLLTABLE WHERE OWNERCOL=? AND ELEMENTCOL = ?
     * </PRE>
     * @return Statement for deleting an item from the Collection.
     */
    protected String getRemoveStmt(Object element)
    {
        if (elementMapping instanceof ReferenceMapping && elementMapping.getNumberOfDatastoreMappings() > 1)
        {
            // The statement is based on the element passed in so don't cache
            return getRemoveStatementString(element);
        }

        if (removeStmt == null)
        {
            synchronized (this)
            {
                removeStmt = getRemoveStatementString(element);
            }
        }
        return removeStmt;
    }

    private String getRemoveStatementString(Object element)
    {
        StringBuilder stmt = new StringBuilder();
        stmt.append("DELETE FROM ");
        stmt.append(containerTable.toString());

        // Add join to element table if required (only allows for 1 element table currently)
        /*            ElementContainerStore.ElementInfo[] elementInfo = ecs.getElementInfo();
        boolean joinedDiscrim = false;
        if (elementInfo != null && elementInfo[0].getDatastoreClass() != containerTable &&
            elementInfo[0].getDiscriminatorMapping() != null)
        {
            joinedDiscrim = true;
            stmt.append(" USING ");
            stmt.append(elementInfo[0].getDatastoreClass().toString());
        }*/

        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, containerTable.toString(), true);
        BackingStoreHelper.appendWhereClauseForElement(stmt, elementMapping, element, elementsAreSerialised, 
            containerTable.toString(), false);
        if (relationDiscriminatorMapping != null)
        {
            // Relation uses shared resource (FK, JoinTable) so restrict to this particular relation
            BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, 
                containerTable.toString(), false);
        }

        return stmt.toString();
    }
}