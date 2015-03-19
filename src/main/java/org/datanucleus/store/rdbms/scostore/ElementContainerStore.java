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

import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;

/**
 * Representation of the store of an element-based container.
 * This is used to represent either a collection or an array.
 * There are the following types of situation that we try to cater for with respect to elements.
 * <UL>
 * <LI><B>element-type</B> is PC with "new-table" or "superclass-table" inheritance. 
 * In this case we will have
 * <I>elementInfo</I> with 1 entry.</LI>
 * <LI><B>element-type</B> is PC with "subclass-table" inheritance. In this case we will have <I>elementInfo</I>
 * with "n" entries (1 for each subclass type with its own table). We also have <I>emd</I> being the MetaData
 * for the element-type.</LI>
 * <LI><B>element-type</B> is Reference type. In this case we will have <I>elementInfo</I> with "n" entries
 * (1 for each implementation type).</LI>
 * <LI><B>element-type</B> is non-PC. In this case we have no <I>elementInfo</I> and no <I>emd</I></LI>
 * </UL>
 */
public abstract class ElementContainerStore extends BaseContainerStore
{
    /** Flag to set whether the iterator statement will use a discriminator or not. */
    protected boolean iterateUsingDiscriminator = false;

    /** Statement for getting the size of the container. */
    protected String sizeStmt;

    /** Statement for clearing the container. */
    protected String clearStmt;

    /** Statement for adding an element to the container. */
    protected String addStmt;

    /** Statement for removing an element from the container. */
    protected String removeStmt;

    /** Whether we are using a discriminator in the "size" statement. */
    protected boolean usingDiscriminatorInSizeStmt = false;

    /**
     * Information for the elements of this container.
     * When the "element-type" table is new-table, or superclass-table then there is 1 value here.
     * When the "element-type" table uses subclass-table, or when it is a reference type then there can be multiple.
     */
    protected ElementInfo[] elementInfo;

    /** Inner class wrapping the information required for a valid element type. */
    public static class ElementInfo
    {
        AbstractClassMetaData cmd; // MetaData for the element class
        DatastoreClass table; // Table storing the element

        public ElementInfo(AbstractClassMetaData cmd, DatastoreClass table)
        {
            this.cmd = cmd;
            this.table = table;
        }

        public String getClassName()
        {
            return cmd.getFullClassName();
        }

        public AbstractClassMetaData getAbstractClassMetaData()
        {
            return cmd;
        }

        public DatastoreClass getDatastoreClass()
        {
            return table;
        }

        public DiscriminatorStrategy getDiscriminatorStrategy()
        {
            return cmd.getDiscriminatorStrategyForTable();
        }

        public JavaTypeMapping getDiscriminatorMapping()
        {
            return table.getDiscriminatorMapping(false);
        }

        public String toString()
        {
            return "ElementInfo : [class=" + cmd.getFullClassName() + " table=" + table + "]";
        }
    }

    /** MetaData for the "element-type" class. Not used for reference types since no metadata is present for the declared type. */
    protected AbstractClassMetaData emd;

    /** Table containing the link between owner and element. Not set when using FK relations. */
    protected Table containerTable;

    /** Mapping for the element. */
    protected JavaTypeMapping elementMapping;

    /** Type of the element. */
    protected String elementType;

    /** Whether the elements are embedded. */
    protected boolean elementsAreEmbedded;

    /** Whether the elements are serialised. */
    protected boolean elementsAreSerialised;

    /** Whether the element is of a persistent-interface (defined using "{interface}") type. */
    protected boolean elementIsPersistentInterface = false;

    /**
     * Mapping for an ordering column to allow for duplicates in the container.
     * Can also be used for ordering elements in a List/array.
     * Can also be used where we have an embedded object and so need to form the PK with something.
     */
    protected JavaTypeMapping orderMapping;

    /** Optional mapping to distinguish elements of one collection from another when sharing the join table. */
    protected JavaTypeMapping relationDiscriminatorMapping;

    /** Value to use to discriminate between elements of this collection from others using the same join table. */
    protected String relationDiscriminatorValue;

    /**
     * Constructor.
     * @param storeMgr Manager for the store
     * @param clr ClassLoader resolver
     */
    protected ElementContainerStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    public ElementInfo[] getElementInfo()
    {
        return elementInfo;
    }

    public JavaTypeMapping getElementMapping()
    {
        return elementMapping;
    }

    public JavaTypeMapping getOrderMapping()
    {
        return orderMapping;
    }

    public JavaTypeMapping getRelationDiscriminatorMapping()
    {
        return relationDiscriminatorMapping;
    }

    public String getRelationDiscriminatorValue()
    {
        return relationDiscriminatorValue;
    }

    public Table getContainerTable()
    {
        return containerTable;
    }

    public AbstractClassMetaData getEmd()
    {
        return emd;
    }

    public boolean isElementsAreSerialised()
    {
        return elementsAreSerialised;
    }

    public boolean isElementsAreEmbedded()
    {
        return elementsAreEmbedded;
    }

    /**
     * Convenience method to find the element information relating to the element type.
     * Used specifically for the "element-type" of a collection/array to find the elements
     * which have table information. Not used for reference types.
     * @return Element information relating to the element type
     */
    protected ElementInfo[] getElementInformationForClass()
    {
        ElementInfo[] info = null;

        DatastoreClass rootTbl;
        String[] clsNames;
        if (clr.classForName(elementType).isInterface())
        {
            // Collection<interface>, so find implementations of the interface and choose the first
            clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(elementType, clr);
            rootTbl = storeMgr.getDatastoreClass(clsNames[0], clr);
        }
        else
        {
            clsNames = new String[] {elementType};
            rootTbl = storeMgr.getDatastoreClass(elementType, clr);
        }

        if (rootTbl == null)
        {
            AbstractClassMetaData[] subclassCmds = storeMgr.getClassesManagingTableForClass(emd, clr);
            info = new ElementInfo[subclassCmds.length];
            for (int i=0;i<subclassCmds.length;i++)
            {
                DatastoreClass table = storeMgr.getDatastoreClass(subclassCmds[i].getFullClassName(), clr);
                info[i] = new ElementInfo(subclassCmds[i], table);
            }
        }
        else
        {
            info = new ElementInfo[clsNames.length];
            for (int i=0; i<clsNames.length; i++)
            {
                AbstractClassMetaData cmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(clsNames[i], clr);
                DatastoreClass table = storeMgr.getDatastoreClass(cmd.getFullClassName(), clr);
                info[i] = new ElementInfo(cmd, table);
            }
        }

        return info;
    }

    /**
     * Accessor for whether the store has an order mapping, to allow for duplicates or ordering.
     * @return Whether an order mapping is present.
     */
    public boolean hasOrderMapping()
    {
        return (orderMapping != null);
    }

    /**
     * Method to validate an element against the accepted type.
     * @param clr The ClassLoaderResolver
     * @param element The element to validate
     * @return Whether it is valid.
     **/ 
    protected boolean validateElementType(ClassLoaderResolver clr, Object element)
    {
        if (element == null)
        {
            return true;
        }

        Class primitiveElementClass = ClassUtils.getPrimitiveTypeForType(element.getClass());
        if (primitiveElementClass != null)
        {
            
            // Allow for the element type being primitive, and the user wanting to store its wrapper
            String elementTypeWrapper = elementType;
            Class elementTypeClass = clr.classForName(elementType);
            if (elementTypeClass.isPrimitive())
            {
                elementTypeWrapper = ClassUtils.getWrapperTypeForPrimitiveType(elementTypeClass).getName();
            }
            return clr.isAssignableFrom(elementTypeWrapper, element.getClass());
        }
        return clr.isAssignableFrom(elementType, element.getClass());
    }

    /**
     * Method to check if an element is already persistent or is persistent but managed by 
     * a different persistence manager.
     * @param op The ObjectProvider of this owner
     * @param element The element
     * @return Whether it is valid for reading.
     */
    protected boolean validateElementForReading(ObjectProvider op, Object element)
    {
        if (!validateElementType(op.getExecutionContext().getClassLoaderResolver(), element))
        {
            return false;
        }

        if (element != null && !elementsAreEmbedded && !elementsAreSerialised)
        {
            ExecutionContext ec = op.getExecutionContext();
            if ((!ec.getApiAdapter().isPersistent(element) ||
                 ec != ec.getApiAdapter().getExecutionContext(element)) && !ec.getApiAdapter().isDetached(element))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Method to check if an element is already persistent, or is managed by a different
     * Persistencemanager. If not persistent, this will persist it.
     * @param ec execution context
     * @param element The element
     * @param fieldValues any initial field values to use if persisting the element
     * @return Whether the element was persisted during this call
     */
    protected boolean validateElementForWriting(ExecutionContext ec, Object element, FieldValues fieldValues)
    {
        // TODO Pass in cascade flag and if element not present then throw exception
        // Check the element type for this collection
        if (!elementIsPersistentInterface && !validateElementType(ec.getClassLoaderResolver(), element))
        {
            throw new ClassCastException(Localiser.msg("056033", element.getClass().getName(), ownerMemberMetaData.getFullFieldName(), elementType));
        }

        boolean persisted = false;
        if (elementsAreEmbedded || elementsAreSerialised)
        {
            // Element is embedded/serialised so has no id
        }
        else
        {
            ObjectProvider elementSM = ec.findObjectProvider(element);
            if (elementSM != null && elementSM.isEmbedded())
            {
                // Element is already with ObjectProvider and is embedded in another field!
                throw new NucleusUserException(Localiser.msg("056028", ownerMemberMetaData.getFullFieldName(), element));
            }

            persisted = SCOUtils.validateObjectForWriting(ec, element, fieldValues);
        }
        return persisted;
    }

    /**
     * Accessor for an iterator through the container elements.
     * @param ownerOP ObjectProvider for the container.
     * @return The Iterator
     */
    public abstract Iterator iterator(ObjectProvider ownerOP);

    /**
     * Clear the association from owner to all elements.
     * Provides cascade-delete when the elements being deleted are PC types.
     * @param ownerOP ObjectProvider for the container. 
     */
    public void clear(ObjectProvider ownerOP)
    {
        Collection dependentElements = null;
        CollectionMetaData collmd = ownerMemberMetaData.getCollection();
        boolean dependent = collmd.isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (dependent && !collmd.isEmbeddedElement() && !collmd.isSerializedElement())
        {
            // Retain the dependent elements that need deleting after clearing
            dependentElements = new HashSet();
            Iterator iter = iterator(ownerOP);
            while (iter.hasNext())
            {
                dependentElements.add(iter.next());
            }
        }
        executeClear(ownerOP);

        // Cascade-delete
        if (dependentElements != null && dependentElements.size() > 0)
        {
            Iterator iter = dependentElements.iterator();
            while (iter.hasNext())
            {
                Object obj = iter.next();
                if (ownerOP.getExecutionContext().getApiAdapter().isDeleted(obj))
                {
                    // Element is tagged for deletion so will be deleted at flush(), and we dont need it immediately
                }
                else
                {
                    ownerOP.getExecutionContext().deleteObjectInternal(obj);
                }
            }
        }
    }

    /**
     * Method to return the size of the container.
     * @param op The ObjectProvider.
     * @return The size.
     */
    public int size(ObjectProvider op)
    {
        return getSize(op);
    }

    /**
     * Method to remove any stored statement for addition of an element.
     */
    protected void invalidateAddStmt()
    {
        addStmt = null;
    }

    /**
     * Generates the statement for adding items.
     * The EMBEDDEDFIELDX columns are only added for embedded PC elements.
     * <PRE>
     * INSERT INTO COLLTABLE (OWNERCOL,[ELEMENTCOL],[EMBEDDEDFIELD1, EMBEDDEDFIELD2,...],[ORDERCOL]) VALUES (?,?,?)
     * </PRE>
     *
     * @return The Statement for adding an item
     */
    protected String getAddStmtForJoinTable()
    {
        if (addStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("INSERT INTO ");
                stmt.append(getContainerTable().toString());
                stmt.append(" (");
                for (int i = 0; i < getOwnerMapping().getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(",");
                    }
                    stmt.append(getOwnerMapping().getDatastoreMapping(i).getColumn().getIdentifier().toString());
                }

                for (int i = 0; i < getElementMapping().getNumberOfDatastoreMappings(); i++)
                {
                    stmt.append(",").append(getElementMapping().getDatastoreMapping(i).getColumn().getIdentifier().toString());
                }

                if (getOrderMapping() != null)
                {
                    for (int i = 0; i < getOrderMapping().getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(",").append(getOrderMapping().getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    }
                }
                if (getRelationDiscriminatorMapping() != null)
                {
                    for (int i = 0; i < getRelationDiscriminatorMapping().getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(",").append(getRelationDiscriminatorMapping().getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    }
                }

                stmt.append(") VALUES (");
                for (int i = 0; i < getOwnerMapping().getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(",");
                    }
                    stmt.append(((AbstractDatastoreMapping) getOwnerMapping().getDatastoreMapping(i)).getInsertionInputParameter());
                }

                for (int i = 0; i < getElementMapping().getNumberOfDatastoreMappings(); i++)
                {
                    stmt.append(",").append(((AbstractDatastoreMapping) getElementMapping().getDatastoreMapping(0)).getInsertionInputParameter());
                }

                if (getOrderMapping() != null)
                {
                    for (int i = 0; i < getOrderMapping().getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(",").append(((AbstractDatastoreMapping) getOrderMapping().getDatastoreMapping(0)).getInsertionInputParameter());
                    }
                }
                if (getRelationDiscriminatorMapping() != null)
                {
                    for (int i = 0; i < getRelationDiscriminatorMapping().getNumberOfDatastoreMappings(); i++)
                    {
                        stmt.append(",").append(((AbstractDatastoreMapping) getRelationDiscriminatorMapping().getDatastoreMapping(0)).getInsertionInputParameter());
                    }
                }

                stmt.append(") ");

                addStmt = stmt.toString();
            }
        }

        return addStmt;
    }

    public void executeClear(ObjectProvider ownerOP)
    {
        String clearStmt = getClearStmt();
        try
        {
            ExecutionContext ec = ownerOP.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, clearStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                    if (getRelationDiscriminatorMapping() != null)
                    {
                        BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    sqlControl.executeStatementUpdate(ec, mconn, clearStmt, ps, true);
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
            throw new NucleusDataStoreException(Localiser.msg("056013", clearStmt), e);
        }
    }

    /**
     * Generate statement for clearing the container.
     * <PRE>
     * DELETE FROM CONTAINERTABLE WHERE OWNERCOL = ? [AND RELATION_DISCRIM=?]
     * </PRE>
     * TODO Add a discriminator restriction on this statement so we only clear ones with a
     * valid discriminator value
     *
     * @return Statement for clearing the container.
     */
    protected String getClearStmt()
    {
        if (clearStmt == null)
        {
            synchronized (this)
            {
                StringBuilder stmt = new StringBuilder("DELETE FROM ").append(getContainerTable().toString()).append(" WHERE ");
                BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
                if (getRelationDiscriminatorMapping() != null)
                {
                    BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, null, false);
                }

                clearStmt = stmt.toString();
            }
        }

        return clearStmt;
    }

    public int getSize(ObjectProvider ownerOP)
    {
        int numRows;

        String sizeStmt = getSizeStmt();
        try
        {
            ExecutionContext ec = ownerOP.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, sizeStmt);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(ownerOP, ec, ps, jdbcPosition, this);
                    if (getElementInfo() != null && getElementInfo().length == 1)
                    {
                        // TODO Allow for multiple element types (e.g interface implementations)
                        for (int i = 0; i < getElementInfo().length; i++)
                        {
                            if (getElementInfo()[i].getDiscriminatorMapping() != null)
                            {
                                jdbcPosition = BackingStoreHelper.populateElementDiscriminatorInStatement(ec, ps, jdbcPosition, true, getElementInfo()[i], clr);
                            }
                        }
                    }
                    if (getRelationDiscriminatorMapping() != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, sizeStmt, ps);
                    try
                    {
                        if (!rs.next())
                        {
                            throw new NucleusDataStoreException(Localiser.msg("056007", sizeStmt));
                        }

                        numRows = rs.getInt(1);
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
            throw new NucleusDataStoreException(Localiser.msg("056007", sizeStmt), e);
        }

        return numRows;
    }

    /**
     * Generate statement for getting the size of thecontainer.
     * The order part is only present when an order mapping is used.
     * The discriminator part is only present when the element has a discriminator.
     * <PRE>
     * SELECT COUNT(*) FROM TBL THIS
     * [INNER JOIN ELEM_TBL ELEM ON TBL.COL = ELEM.ID] - when no null
     * [LEFT OUTER JOIN ELEM_TBL ELEM ON TBL.COL = ELEM.ID] - when allows null
     * WHERE THIS.OWNERCOL=?
     * [AND THIS.ORDERCOL IS NOT NULL]
     * [AND (DISCRIMINATOR=? OR DISCRMINATOR=? OR DISCRIMINATOR=? [OR DISCRIMINATOR IS NULL])]
     * [AND RELATION_DISCRIM=?]
     * </PRE>
     * The discriminator part includes all subclasses of the element type.
     * If the element is in a different table to the container then an INNER JOIN will be present to
     * link the two tables, and table aliases will be present also.
     * TODO Update this to allow for getting the size when more than 1 element table.
     *
     * @return The Statement returning the size of the container.
     */
    protected String getSizeStmt()
    {
        if (sizeStmt != null && !usingDiscriminatorInSizeStmt)
        {
            // Statement exists and didnt need any discriminator when setting up the statement so just reuse it
            return sizeStmt;
        }

        synchronized (this)
        {
            String containerAlias = "THIS";
            String joinedElementAlias = "ELEM";
            StringBuilder stmt = new StringBuilder("SELECT COUNT(*) FROM ").append(getContainerTable().toString()).append(" ").append(containerAlias);

            // Add join to element table if required (only allows for 1 element table currently)
            boolean joinedDiscrim = false;
            if (getElementInfo() != null && getElementInfo().length == 1 &&
                getElementInfo()[0].getDatastoreClass() != getContainerTable() &&
                getElementInfo()[0].getDiscriminatorMapping() != null)
            {
                // TODO Allow for more than 1 possible element table
                // Need join to the element table to restrict the discriminator
                joinedDiscrim = true;
                JavaTypeMapping elemIdMapping = getElementInfo()[0].getDatastoreClass().getIdMapping();
                stmt.append(allowNulls ? " LEFT OUTER JOIN " : " INNER JOIN ");
                stmt.append(getElementInfo()[0].getDatastoreClass().toString()).append(" ").append(joinedElementAlias).append(" ON ");
                for (int i = 0; i < getElementMapping().getNumberOfDatastoreMappings(); i++)
                {
                    if (i > 0)
                    {
                        stmt.append(" AND ");
                    }
                    stmt.append(containerAlias).append(".").append(getElementMapping().getDatastoreMapping(i).getColumn().getIdentifier());
                    stmt.append("=");
                    stmt.append(joinedElementAlias).append(".").append(elemIdMapping.getDatastoreMapping(i).getColumn().getIdentifier());
                }
            }
            // TODO Add join to owner if ownerMapping is for supertable

            stmt.append(" WHERE ");
            BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, containerAlias, true);
            if (orderMapping != null)
            {
                // If an ordering is present, restrict to items where the index is not null to
                // eliminate records that are added but may not be positioned yet.
                for (int i = 0; i < orderMapping.getNumberOfDatastoreMappings(); i++)
                {
                    stmt.append(" AND ");
                    stmt.append(containerAlias).append(".").append(orderMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
                    stmt.append(">=0");
                }
            }

            if (getElementInfo() != null && getElementInfo().length == 1)
            {
                // TODO Support more than one element table
                // Add a discriminator filter for collections with an element discriminator
                StringBuilder discrStmt = new StringBuilder();
                for (int i = 0; i < getElementInfo().length; i++)
                {
                    ElementInfo elemInfo = getElementInfo()[i];

                    if (elemInfo.getDiscriminatorMapping() != null)
                    {
                        usingDiscriminatorInSizeStmt = true;

                        JavaTypeMapping discrimMapping = elemInfo.getDiscriminatorMapping();
                        // TODO What if we have the discriminator in a supertable? the mapping will be null so we don't get this clause added!
                        Class cls = clr.classForName(elemInfo.getClassName());
                        if (!Modifier.isAbstract(cls.getModifiers()))
                        {
                            for (int j = 0; j < discrimMapping.getNumberOfDatastoreMappings(); j++)
                            {
                                if (discrStmt.length() > 0)
                                {
                                    discrStmt.append(" OR ");
                                }

                                discrStmt.append(joinedDiscrim ? joinedElementAlias : containerAlias);
                                discrStmt.append(".");
                                discrStmt.append(discrimMapping.getDatastoreMapping(j).getColumn().getIdentifier().toString());
                                discrStmt.append("=");
                                discrStmt.append(((AbstractDatastoreMapping) discrimMapping.getDatastoreMapping(j)).getUpdateInputParameter());
                            }
                        }

                        Collection<String> subclasses = storeMgr.getSubClassesForClass(elemInfo.getClassName(), true, clr);
                        if (subclasses != null && subclasses.size() > 0)
                        {
                            for (String subclass : subclasses)
                            {
                                cls = clr.classForName(subclass);
                                if (!Modifier.isAbstract(cls.getModifiers()))
                                {
                                    for (int k = 0; k < discrimMapping.getNumberOfDatastoreMappings(); k++)
                                    {
                                        if (discrStmt.length() > 0)
                                        {
                                            discrStmt.append(" OR ");
                                        }

                                        discrStmt.append(joinedDiscrim ? joinedElementAlias : containerAlias);
                                        discrStmt.append(".");
                                        discrStmt.append(discrimMapping.getDatastoreMapping(k).getColumn().getIdentifier().toString());
                                        discrStmt.append("=");
                                        discrStmt.append(((AbstractDatastoreMapping) discrimMapping.getDatastoreMapping(k)).getUpdateInputParameter());
                                    }
                                }
                            }
                        }
                    }
                }

                if (discrStmt.length() > 0)
                {
                    stmt.append(" AND (");
                    stmt.append(discrStmt);
                    if (allowNulls)
                    {
                        stmt.append(" OR ");
                        stmt.append(getElementInfo()[0].getDiscriminatorMapping().getDatastoreMapping(0).getColumn().getIdentifier().toString());
                        stmt.append(" IS NULL");
                    }
                    stmt.append(")");
                }
            }
            if (relationDiscriminatorMapping != null)
            {
                BackingStoreHelper.appendWhereClauseForMapping(stmt, relationDiscriminatorMapping, containerAlias, false);
            }

            sizeStmt = stmt.toString();
            return sizeStmt;
        }
    }
}