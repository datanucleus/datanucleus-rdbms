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
package org.datanucleus.store.rdbms.scostore;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.query.StatementParameterMapping;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SelectStatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * RDBMS-specific implementation of an FK ArrayStore.
 */
public class FKArrayStore<E> extends AbstractArrayStore<E>
{
    /** Statement for nullifying a FK in the element. */
    private String clearNullifyStmt;

    /** Statement for updating a foreign key in a 1-N unidirectional */
    private String updateFkStmt;

    /**
     * @param mmd Metadata for the owning field/property
     * @param storeMgr Manager for the datastore
     * @param clr ClassLoader resolver
     */
    public FKArrayStore(AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);

        setOwner(mmd);
        ArrayMetaData arrmd = mmd.getArray();
        if (arrmd == null)
        {
            throw new NucleusUserException(Localiser.msg("056000", mmd.getFullFieldName()));
        }

        // Load the element class
        elementType = mmd.getType().getComponentType().getName();
        Class element_class = clr.classForName(elementType);

        if (ClassUtils.isReferenceType(element_class))
        {
            elementIsPersistentInterface = storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterface(element_class.getName());
            if (elementIsPersistentInterface)
            {
                elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForInterface(element_class,clr);
            }
            else
            {
                // Take the metadata for the first implementation of the reference type
                elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForImplementationOfReference(element_class,null,clr);
            }
        }
        else
        {
            // Check that the element class has MetaData
            elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(element_class, clr);
        }
        if (elementCmd == null)
        {
            throw new NucleusUserException(Localiser.msg("056003", element_class.getName(), mmd.getFullFieldName()));
        }

        elementInfo = getComponentInformationForClass(elementType, elementCmd);
        if (elementInfo == null || elementInfo.length == 0)
        {
            throw new NucleusUserException(Localiser.msg("056075", ownerMemberMetaData.getFullFieldName(), elementType));
        }
        if (elementInfo != null && elementInfo.length > 1)
        {
            throw new NucleusUserException(Localiser.msg("056045", ownerMemberMetaData.getFullFieldName()));
        }

        elementMapping = elementInfo[0].getDatastoreClass().getIdMapping(); // Just use the first element type as the guide for the element mapping
        elementsAreEmbedded = false; // Can't embed element when using FK relation
        elementsAreSerialised = false; // Can't serialise element when using FK relation

        // Find the mapping back to the owning object
        for (int i=0;i<elementInfo.length;i++)
        {
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                // 1-N FK bidirectional : The element class has a field for the owner
                if (mmd.getMappedBy().indexOf('.') < 0)
                {
                    AbstractClassMetaData eoCmd = storeMgr.getMetaDataManager().getMetaDataForClass(element_class, clr);
                    AbstractMemberMetaData eofmd = (eoCmd != null ? eoCmd.getMetaDataForMember(mmd.getMappedBy()) : null);
                    if (eofmd == null)
                    {
                        throw new NucleusUserException(Localiser.msg("056024", mmd.getFullFieldName(), mmd.getMappedBy(), element_class.getName()));
                    }

                    // Check that the type of the element "mapped-by" field is consistent with the owner type
                    if (!clr.isAssignableFrom(eofmd.getType(), mmd.getAbstractClassMetaData().getFullClassName()))
                    {
                        throw new NucleusUserException(Localiser.msg("056025", mmd.getFullFieldName(), 
                            eofmd.getFullFieldName(), eofmd.getTypeName(), mmd.getAbstractClassMetaData().getFullClassName()));
                    }

                    String ownerFieldName = eofmd.getName();
                    ownerMapping = elementInfo[i].getDatastoreClass().getMemberMapping(eofmd);
                    if (ownerMapping == null)
                    {
                        throw new NucleusUserException(Localiser.msg("056046", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), elementType, ownerFieldName));
                    }
                    if (isEmbeddedMapping(ownerMapping))
                    {
                        throw new NucleusUserException(Localiser.msg("056026", ownerFieldName, elementType, eofmd.getTypeName(), mmd.getClassName()));
                    }
                }
                else
                {
                    // mappedBy uses DOT notation, so refers to a field in an embedded field of the element
                    AbstractMemberMetaData otherMmd = null;
                    AbstractClassMetaData otherCmd = elementCmd;
                    String remainingMappedBy = ownerMemberMetaData.getMappedBy();
                    JavaTypeMapping otherMapping = null;
                    while (remainingMappedBy.indexOf('.') > 0)
                    {
                        int dotPosition = remainingMappedBy.indexOf('.');
                        String thisMappedBy = remainingMappedBy.substring(0, dotPosition);
                        otherMmd = otherCmd.getMetaDataForMember(thisMappedBy);
                        if (otherMapping == null)
                        {
                            otherMapping = elementInfo[i].getDatastoreClass().getMemberMapping(thisMappedBy);
                        }
                        else
                        {
                            if (!(otherMapping instanceof EmbeddedPCMapping))
                            {
                                throw new NucleusUserException("Processing of mappedBy DOT notation for " + ownerMemberMetaData.getFullFieldName() + " found mapping=" + otherMapping + 
                                        " but expected to be embedded");
                            }
                            otherMapping = ((EmbeddedPCMapping)otherMapping).getJavaTypeMapping(thisMappedBy);
                        }

                        remainingMappedBy = remainingMappedBy.substring(dotPosition+1);
                        otherCmd = storeMgr.getMetaDataManager().getMetaDataForClass(otherMmd.getTypeName(), clr); // TODO Cater for N-1
                        if (remainingMappedBy.indexOf('.') < 0)
                        {
                            if (!(otherMapping instanceof EmbeddedPCMapping))
                            {
                                throw new NucleusUserException("Processing of mappedBy DOT notation for " + ownerMemberMetaData.getFullFieldName() + " found mapping=" + otherMapping + 
                                        " but expected to be embedded");
                            }
                            otherMapping = ((EmbeddedPCMapping)otherMapping).getJavaTypeMapping(remainingMappedBy);
                        }
                    }
                    ownerMapping = otherMapping;
                }
            }
            else
            {
                // 1-N FK unidirectional : the element class knows nothing about the owner (but the table has external mappings)
                ownerMapping = elementInfo[0].getDatastoreClass().getExternalMapping(mmd, MappingType.EXTERNAL_FK);
                if (ownerMapping == null)
                {
                    throw new NucleusUserException(Localiser.msg("056047", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), elementType));
                }
            }
            elementInfo[i].setOwnerMapping(ownerMapping);
        }
        this.ownerMapping = elementInfo[0].getOwnerMapping(); // TODO Get rid of ownerMapping and refer to elementInfo[i].getOwnerMapping

        orderMapping = elementInfo[0].getDatastoreClass().getExternalMapping(mmd, MappingType.EXTERNAL_INDEX);
        if (orderMapping == null)
        {
            throw new NucleusUserException(Localiser.msg("056048", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), elementType));
        }

        relationDiscriminatorMapping = elementInfo[0].getDatastoreClass().getExternalMapping(mmd, MappingType.EXTERNAL_FK_DISCRIMINATOR);
        if (relationDiscriminatorMapping != null)
        {
            relationDiscriminatorValue = mmd.getValueForExtension("relation-discriminator-value");
            if (relationDiscriminatorValue == null)
            {
                // No value defined so just use the field name
                relationDiscriminatorValue = mmd.getFullFieldName();
            }
        }

        // TODO Cater for multiple element tables
        containerTable = elementInfo[0].getDatastoreClass();
        if (mmd.getMappedBy() != null && ownerMapping.getTable() != containerTable)
        {
            // Element and owner don't have consistent tables so use the one with the mapping
            // e.g collection is of subclass, yet superclass has the link back to the owner
            containerTable = ownerMapping.getTable();
        }
    }

    /**
     * Update a FK and element position in the element.
     * @param ownerOP ObjectProvider for the owner
     * @param element The element to update
     * @param owner The owner object to set in the FK
     * @param index The index position (or -1 if not known)
     * @return Whether it was performed successfully
     */
    private boolean updateElementFk(ObjectProvider ownerOP, E element, Object owner, int index)
    {
        if (element == null)
        {
            return false;
        }

        boolean retval;
        String updateFkStmt = getUpdateFkStmt();
        ExecutionContext ec = ownerOP.getExecutionContext();
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, updateFkStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    if (elementInfo.length > 1)
                    {
                        DatastoreClass table = storeMgr.getDatastoreClass(element.getClass().getName(), clr);
                        if (table != null)
                        {
                            ps.setString(jdbcPosition++, table.toString());
                        }
                        else
                        {
                            NucleusLogger.PERSISTENCE.info(">> FKArrayStore.updateElementFK : need to set table in statement but dont know table where to store " + element);
                        }
                    }
                    if (owner == null)
                    {
                        ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(jdbcPosition, ownerMapping), null);
                        jdbcPosition += ownerMapping.getNumberOfDatastoreMappings();
                    }
                    else
                    {
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                    }
                    jdbcPosition = BackingStoreHelper.populateOrderInStatement(ec, ps, index, jdbcPosition, orderMapping);
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }
                    jdbcPosition = BackingStoreHelper.populateElementInStatement(ec, ps, element, jdbcPosition, elementMapping);

                    sqlControl.executeStatementUpdate(ec, mconn, updateFkStmt, ps, true);
                    retval = true;
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
            throw new NucleusDataStoreException(Localiser.msg("056027", updateFkStmt), e);
        }

        return retval;
    }

    /**
     * Generate statement for updating the owner, index columns in an inverse 1-N. 
     * Will result in the statement
     * <PRE>
     * UPDATE ELEMENTTABLE SET FK_COL_1=?, FK_COL_2=?, FK_IDX=? [,DISTINGUISHER=?]
     * WHERE ELEMENT_ID=?
     * </PRE>
     * when we have a single element table, and
     * <PRE>
     * UPDATE ? SET FK_COL_1=?, FK_COL_2=?, FK_IDX=? [,DISTINGUISHER=?]
     * WHERE ELEMENT_ID=?
     * </PRE>
     * when we have multiple element tables possible.
     * @return Statement for updating the owner/index of an element in an inverse 1-N
     */
    private String getUpdateFkStmt()
    {
        if (updateFkStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("UPDATE ");
                if (elementInfo.length > 1)
                {
                    stmt.append("?");
                }
                else
                {
                    stmt.append(elementInfo[0].getDatastoreClass().toString());
                }
                stmt.append(" SET ");
                for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(",");
                    }
                    stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    stmt.append(" = ");
                    stmt.append(((AbstractDatastoreMapping) ownerMapping.getDatastoreMapping(i)).getUpdateInputParameter());
                }
                for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
                {
                    stmt.append(",");
                    stmt.append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    stmt.append(" = ");
                    stmt.append(((AbstractDatastoreMapping) orderMapping.getDatastoreMapping(i)).getUpdateInputParameter());
                }
                if (relationDiscriminatorMapping != null)
                {
                    for (int i = 0; i < relationDiscriminatorMapping.getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(",");
                        stmt.append(
                            relationDiscriminatorMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                        stmt.append(" = ");
                        stmt.append(((AbstractDatastoreMapping) relationDiscriminatorMapping.getDatastoreMapping(i)).getUpdateInputParameter());
                    }
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, elementMapping, null, true);

                updateFkStmt = stmt.toString();
            }
        }

        return updateFkStmt;
    }

    /**
     * Method to clear the Array.
     * This is called when the container object is being deleted and the elements are to be removed (maybe for dependent field).
     * @param ownerOP The ObjectProvider
     */
    public void clear(ObjectProvider ownerOP)
    {
        boolean deleteElements = false;
        if (ownerMemberMetaData.getArray().isDependentElement())
        {
            // Elements are dependent and can't exist on their own, so delete them all
            NucleusLogger.DATASTORE.debug(Localiser.msg("056034"));
            deleteElements = true;
        }
        else
        {
            if (ownerMapping.isNullable() && orderMapping.isNullable())
            {
                // Field is not dependent, and nullable so we null the FK
                NucleusLogger.DATASTORE.debug(Localiser.msg("056036"));
                deleteElements = false;
            }
            else
            {
                // Field is not dependent, and not nullable so we just delete the elements
                NucleusLogger.DATASTORE.debug(Localiser.msg("056035"));
                deleteElements = true;
            }
        }

        if (deleteElements)
        {
            ownerOP.isLoaded(ownerMemberMetaData.getAbsoluteFieldNumber()); // Make sure the field is loaded
            Object[] value = (Object[]) ownerOP.provideField(ownerMemberMetaData.getAbsoluteFieldNumber());
            if (value != null && value.length > 0)
            {
                ownerOP.getExecutionContext().deleteObjects(value);
            }
        }
        else
        {
            boolean ownerSoftDelete = ownerOP.getClassMetaData().hasExtension(MetaData.EXTENSION_CLASS_SOFTDELETE);
            if (!ownerSoftDelete)
            {
                // TODO Cater for multiple element roots
                // TODO If the relation is bidirectional we need to clear the owner in the element
                String clearNullifyStmt = getClearNullifyStmt();
                try
                {
                    ExecutionContext ec = ownerOP.getExecutionContext();
                    ManagedConnection mconn = storeMgr.getConnection(ec);
                    SQLController sqlControl = storeMgr.getSQLController();
                    try
                    {
                        PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, clearNullifyStmt, false);
                        try
                        {
                            int jdbcPosition = 1;
                            jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                            if (relationDiscriminatorMapping != null)
                            {
                                BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                            }
                            sqlControl.executeStatementUpdate(ec, mconn, clearNullifyStmt, ps, true);
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
                    throw new NucleusDataStoreException(Localiser.msg("056013", clearNullifyStmt), e);
                }
            }
        }
    }

    /**
     * Generates the statement for clearing items by nulling the owner link out. The statement will be
     * <PRE>
     * UPDATE ARRAYTABLE SET OWNERCOL=NULL, INDEXCOL=NULL [,DISTINGUISHER=NULL]
     * WHERE OWNERCOL=? [AND DISTINGUISHER=?]
     * </PRE>
     * when there is only one element table, and will be
     * <PRE>
     * UPDATE ? SET OWNERCOL=NULL, INDEXCOL=NULL [,DISTINGUISHER=NULL]
     * WHERE OWNERCOL=? [AND DISTINGUISHER=?]
     * </PRE>
     * when there is more than 1 element table.
     * @return The Statement for clearing items for the owner.
     */
    protected String getClearNullifyStmt()
    {
        if (clearNullifyStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("UPDATE ");
                if (elementInfo.length > 1)
                {
                    stmt.append("?");
                }
                else
                {
                    stmt.append(elementInfo[0].getDatastoreClass().toString());
                }
                stmt.append(" SET ");
                for (int i = 0; i < ownerMapping.getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(", ");
                    }
                    stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString() + " = NULL");
                }
                for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
                {
                    stmt.append(", ");
                    stmt.append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString() + " = NULL");
                }
                if (relationDiscriminatorMapping != null)
                {
                    for (int i = 0; i < relationDiscriminatorMapping.getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(", ");
                        stmt.append(relationDiscriminatorMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString() + " = NULL");
                    }
                }

                stmt.append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                if (relationDiscriminatorMapping != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }

                clearNullifyStmt = stmt.toString();
            }
        }
        return clearNullifyStmt;
    }

    /**
     * Method to set the array for the specified owner to the passed value.
     * @param ownerOP ObjectProvider for the owner
     * @param array the array
     * @return Whether the array was updated successfully
     */
    public boolean set(ObjectProvider ownerOP, Object array)
    {
        if (array == null)
        {
            return true;
        }

        // Check that all elements are inserted
        for (int i=0;i<Array.getLength(array);i++)
        {
            validateElementForWriting(ownerOP.getExecutionContext(), Array.get(array, i), null);
        }

        // Update the FK and position of all elements
        int length = Array.getLength(array);
        for (int i=0;i<length;i++)
        {
            E obj = (E)Array.get(array, i);
            updateElementFk(ownerOP, obj, ownerOP.getObject(), i);
        }

        return true;
    }

    /**
     * Accessor for an iterator for the set.
     * @param ownerOP ObjectProvider for the set.
     * @return Iterator for the set.
     */
    public Iterator<E> iterator(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();

        if (elementInfo == null || elementInfo.length == 0)
        {
            return null;
        }

        // Generate the statement, and statement mapping/parameter information
        IteratorStatement iterStmt = getIteratorStatement(ownerOP.getExecutionContext(), ownerOP.getExecutionContext().getFetchPlan(), true);
        SelectStatement sqlStmt = iterStmt.getSelectStatement();
        StatementClassMapping iteratorMappingDef = iterStmt.getStatementClassMapping();

        // Input parameter(s) - the owner
        int inputParamNum = 1;
        StatementMappingIndex ownerIdx = new StatementMappingIndex(ownerMapping);
        if (sqlStmt.getNumberOfUnions() > 0)
        {
            // Add parameter occurrence for each union of statement
            for (int j=0;j<sqlStmt.getNumberOfUnions()+1;j++)
            {
                int[] paramPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
                for (int k=0;k<ownerMapping.getNumberOfDatastoreMappings();k++)
                {
                    paramPositions[k] = inputParamNum++;
                }
                ownerIdx.addParameterOccurrence(paramPositions);
            }
        }
        else
        {
            int[] paramPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<ownerMapping.getNumberOfDatastoreMappings();k++)
            {
                paramPositions[k] = inputParamNum++;
            }
            ownerIdx.addParameterOccurrence(paramPositions);
        }

        StatementParameterMapping iteratorMappingParams = new StatementParameterMapping();
        iteratorMappingParams.addMappingForParameter("owner", ownerIdx);

        if (ec.getTransaction().getSerializeRead() != null && ec.getTransaction().getSerializeRead())
        {
            sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
        }
        String stmt = sqlStmt.getSQLText().toSQL();

        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Create the statement
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);

                // Set the owner
                ObjectProvider stmtOwnerOP = BackingStoreHelper.getOwnerObjectProviderForBackingStore(ownerOP);
                int numParams = ownerIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerIdx.getMapping().setObject(ec, ps, ownerIdx.getParameterPositionsForOccurrence(paramInstance), stmtOwnerOP.getObject());
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        ResultObjectFactory rof = null;
                        if (elementsAreEmbedded || elementsAreSerialised)
                        {
                            throw new NucleusException("Cannot have FK array with non-persistent objects");
                        }

                        rof = new PersistentClassROF(storeMgr, elementCmd, iteratorMappingDef, false, null, clr.classForName(elementType));
                        return new ArrayStoreIterator(ownerOP, rs, rof, this);
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
        catch (SQLException | MappedDatastoreException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056006", stmt),e);
        }
    }

    /**
     * Method to return the SQLStatement and mapping for an iterator for this backing store.
     * Create a statement of the form
     * <pre>
     * SELECT ELEM_COLS
     * FROM ELEM_TBL
     * [WHERE]
     *   [ELEM_TBL.OWNER_ID = {value}] [AND]
     *   [ELEM_TBL.DISCRIM = {discrimValue}]
     * [ORDER BY {orderClause}]
     * </pre>
     * @param ec ExecutionContext
     * @param fp FetchPlan to use in determing which fields of element to select
     * @param addRestrictionOnOwner Whether to restrict to a particular owner (otherwise functions as bulk fetch for many owners).
     * @return The SQLStatement and its associated StatementClassMapping
     */
    public IteratorStatement getIteratorStatement(ExecutionContext ec, FetchPlan fp, boolean addRestrictionOnOwner)
    {
        SelectStatement sqlStmt = null;
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        StatementClassMapping iteratorMappingClass = new StatementClassMapping();

        if (elementInfo[0].getDatastoreClass().getDiscriminatorMetaData() != null &&
            elementInfo[0].getDatastoreClass().getDiscriminatorMetaData().getStrategy() != DiscriminatorStrategy.NONE)
        {
            String elementType = ownerMemberMetaData.getArray().getElementType();
            if (ClassUtils.isReferenceType(clr.classForName(elementType)))
            {
                String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(elementType, clr);
                Class[] cls = new Class[clsNames.length];
                for (int i=0; i<clsNames.length; i++)
                {
                    cls[i] = clr.classForName(clsNames[i]);
                }
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null).getStatement(ec);
            }
            else
            {
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, clr.classForName(elementInfo[0].getClassName()), true, null, null).getStatement(ec);
            }
            iterateUsingDiscriminator = true;

            // Select the required fields
            SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingClass, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
        }
        else
        {
            for (int i=0;i<elementInfo.length;i++)
            {
                final Class elementCls = clr.classForName(this.elementInfo[i].getClassName());
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, elementCls, true, null, null);
                stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                iteratorMappingClass.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                SelectStatement subStmt = stmtGen.getStatement(ec);

                // Select the required fields (of the element class)
                if (sqlStmt == null)
                {
                    if (elementInfo.length > 1)
                    {
                        SQLStatementHelper.selectIdentityOfCandidateInStatement(subStmt, iteratorMappingClass, elementInfo[i].getAbstractClassMetaData());
                    }
                    else
                    {
                        SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(subStmt, iteratorMappingClass, fp, subStmt.getPrimaryTable(), elementInfo[i].getAbstractClassMetaData(), 
                            fp.getMaxFetchDepth());
                    }
                }
                else
                {
                    if (elementInfo.length > 1)
                    {
                        SQLStatementHelper.selectIdentityOfCandidateInStatement(subStmt, null, elementInfo[i].getAbstractClassMetaData());
                    }
                    else
                    {
                        SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(subStmt, null, fp, subStmt.getPrimaryTable(), elementInfo[i].getAbstractClassMetaData(), fp.getMaxFetchDepth());
                    }
                }

                if (sqlStmt == null)
                {
                    sqlStmt = subStmt;
                }
                else
                {
                    sqlStmt.union(subStmt);
                }
            }
        }

        if (addRestrictionOnOwner)
        {
            // Apply condition to filter by owner
            SQLTable ownerSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerExpr = exprFactory.newExpression(sqlStmt, ownerSqlTbl, ownerMapping);
            SQLExpression ownerVal = exprFactory.newLiteralParameter(sqlStmt, ownerMapping, null, "OWNER");
            sqlStmt.whereAnd(ownerExpr.eq(ownerVal), true);
        }

        if (relationDiscriminatorMapping != null)
        {
            // Apply condition on distinguisher field to filter by distinguisher (when present)
            SQLTable distSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), relationDiscriminatorMapping);
            SQLExpression distExpr = exprFactory.newExpression(sqlStmt, distSqlTbl, relationDiscriminatorMapping);
            SQLExpression distVal = exprFactory.newLiteral(sqlStmt, relationDiscriminatorMapping, relationDiscriminatorValue);
            sqlStmt.whereAnd(distExpr.eq(distVal), true);
        }

        if (orderMapping != null)
        {
            // Order by the ordering column, when present
            SQLTable orderSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), orderMapping);
            SQLExpression[] orderExprs = new SQLExpression[orderMapping.getNumberOfDatastoreMappings()];
            boolean descendingOrder[] = new boolean[orderMapping.getNumberOfDatastoreMappings()];
            orderExprs[0] = exprFactory.newExpression(sqlStmt, orderSqlTbl, orderMapping);
            sqlStmt.setOrdering(orderExprs, descendingOrder);
        }

        return new IteratorStatement(this, sqlStmt, iteratorMappingClass);
    }
}