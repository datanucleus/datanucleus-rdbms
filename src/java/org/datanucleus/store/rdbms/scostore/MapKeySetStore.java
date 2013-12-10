/**********************************************************************
Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
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
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.Transaction;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.query.StatementParameterMapping;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.StatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.scostore.MapStore;
import org.datanucleus.util.ClassUtils;

/**
 * RDBMS-specific implementation of a SetStore for map keys.
 */
class MapKeySetStore extends AbstractSetStore
{
    protected final MapStore mapStore;

    /** JDBC statement to use for retrieving keys of the map (locking). */
    private String iteratorStmtLocked = null;

    /** JDBC statement to use for retrieving keys of the map (not locking). */
    private String iteratorStmtUnlocked = null;

    private StatementClassMapping iteratorMappingDef = null;
    private StatementParameterMapping iteratorMappingParams = null;

    /**
     * Constructor where a join table is used to store the map relation.
     * @param mapTable The table for the map (join table)
     * @param mapStore Backing store for the map
     * @param clr The ClassLoaderResolver
     */
    MapKeySetStore(MapTable mapTable, MapStore mapStore, ClassLoaderResolver clr)
    {
        super(mapTable.getStoreManager(), clr);

        this.mapStore = mapStore;
        this.containerTable = mapTable;
        this.ownerMemberMetaData = mapTable.getOwnerMemberMetaData();
        this.ownerMapping = mapTable.getOwnerMapping();
        this.elementMapping = mapTable.getKeyMapping();

        initialize(clr);
    }

    /**
     * Constructor where a foreign key is used to store the map relation.
     * @param mapTable Table holding the map relation (key or value)
     * @param mapStore Backing store for the map
     * @param clr The ClassLoaderResolver
     * @param ownerMapping mapping in the map table back to the owner
     * @param keyMapping mapping in the map table to the key
     * @param ownerMmd metadata for the owning field/property
     */
    MapKeySetStore(Table mapTable, MapStore mapStore, ClassLoaderResolver clr, 
        JavaTypeMapping ownerMapping, JavaTypeMapping keyMapping, AbstractMemberMetaData ownerMmd)
    {
        super(mapTable.getStoreManager(), clr);

        this.mapStore = mapStore;
        this.containerTable = mapTable;
        this.ownerMemberMetaData = ownerMmd;
        this.ownerMapping = ownerMapping;
        this.elementMapping = keyMapping;

        initialize(clr);
    }

    /**
     * Initialisation method.
     */
    private void initialize(ClassLoaderResolver clr)
    {
        elementType = elementMapping.getType();
        elementsAreEmbedded = isEmbeddedMapping(elementMapping);
        elementsAreSerialised = isEmbeddedMapping(elementMapping);

        // Load the element class
        Class element_class = clr.classForName(elementType);

        if (ClassUtils.isReferenceType(element_class))
        {
            emd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForImplementationOfReference(element_class, null, clr);
        }
        else
        {
            emd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(element_class, clr);
        }
        if (emd != null)
        {
            elementType = emd.getFullClassName();
            elementInfo = getElementInformationForClass();
        } 
    }

    /**
     * Method to add an element. Overridden because we want to prevent it.
     * @param op ObjectProvider of collection.
     * @param element Element to add.
     * @return Whether it was successful
     **/
    public boolean add(ObjectProvider op, Object element, int size)
    {
        throw new UnsupportedOperationException("Cannot add to a map through its key set");
    }

    /**
     * Method to add a collection of elements. Overridden because we want to prevent it.
     * @param op ObjectProvider of collection.
     * @param elements Elements to add.
     * @return Whether it was successful
     **/
    public boolean addAll(ObjectProvider op, Collection elements, int size)
    {
        throw new UnsupportedOperationException("Cannot add to a map through its key set");
    }

    /**
     * Method to remove an element. Overridden because we want to prevent it.
     * @param op ObjectProvider of collection.
     * @param element Element to remove.
     * @return Whether it was successful
     **/
    public boolean remove(ObjectProvider op, Object element, int size, boolean allowDependentField)
    {
        if (!canRemove())
        {
            throw new UnsupportedOperationException("Cannot remove from an inverse map through its key set");
        }

        return super.remove(op, element, size, allowDependentField);
    }

    /**
     * Method to remove a collection of elements. Overridden because we want to prevent it.
     * @param op ObjectProvider of collection.
     * @param elements Elements to remove.
     * @return Whether it was successful
     **/
    public boolean removeAll(ObjectProvider op, Collection elements, int size)
    {
        if (!canRemove())
        {
            throw new UnsupportedOperationException("Cannot remove from an inverse map through its key set");
        }

        return super.removeAll(op, elements, size);
    }

    /**
     * Method to clear the collection. Overridden because we want to prevent it.
     * @param op ObjectProvider of collection.
     **/
    public void clear(ObjectProvider op)
    {
        if (!canClear())
        {
            throw new UnsupportedOperationException("Cannot clear an inverse map through its key set");
        }

        super.clear(op);
    }

    protected boolean canRemove()
    {
        return false;
    }

    protected boolean canClear()
    {
        return false;
    }

    /**
     * Accessor for an iterator for the set.
     * @param ownerOP ObjectProvider for the set. 
     * @return Iterator for the set.
     */
    public Iterator iterator(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        if (iteratorStmtLocked == null)
        {
            synchronized (this) // Make sure this completes in case another thread needs the same info
            {
                // Generate the statement, and statement mapping/parameter information
                SQLStatement sqlStmt = getSQLStatementForIterator(ownerOP);
                iteratorStmtUnlocked = sqlStmt.getSelectStatement().toSQL();
                sqlStmt.addExtension("lock-for-update", true);
                iteratorStmtLocked = sqlStmt.getSelectStatement().toSQL();
            }
        }

        Transaction tx = ec.getTransaction();
        String stmt = (tx.getSerializeRead() != null && tx.getSerializeRead() ? iteratorStmtLocked : iteratorStmtUnlocked);
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Create the statement and set the owner
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                StatementMappingIndex ownerIdx = iteratorMappingParams.getMappingForParameter("owner");
                int numParams = ownerIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerIdx.getMapping().setObject(ec, ps,
                        ownerIdx.getParameterPositionsForOccurrence(paramInstance), ownerOP.getObject());
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        ResultObjectFactory rof = null;
                        if (elementsAreEmbedded || elementsAreSerialised)
                        {
                            // No ResultObjectFactory needed - handled by SetStoreIterator
                            return new SetStoreIterator(ownerOP, rs, null, this);
                        }
                        else
                        {
                            rof = storeMgr.newResultObjectFactory(emd, iteratorMappingDef, false, null,
                                        clr.classForName(elementType));
                            return new SetStoreIterator(ownerOP, rs, rof, this);
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
            throw new NucleusDataStoreException(LOCALISER.msg("056006", stmt),e);
        }
        catch (MappedDatastoreException e)
        {
            throw new NucleusDataStoreException(LOCALISER.msg("056006", stmt),e);
        }
    }

    /**
     * Method to generate an SQLStatement for iterating through keys of the map.
     * Populates the iteratorMappingDef and iteratorMappingParams.
     * Creates a statement that selects the key table(s), and adds any necessary join to the containerTable
     * if that is not the key table. If the key is embedded then selects the table it is embedded in.
     * Adds a restriction on the ownerMapping of the containerTable so we can restrict to the owner object.
     * @param ownerOP ObjectProvider for the owner object
     * @return The SQLStatement
     */
    protected SQLStatement getSQLStatementForIterator(ObjectProvider ownerOP)
    {
        SQLStatement sqlStmt = null;

        final ClassLoaderResolver clr = ownerOP.getExecutionContext().getClassLoaderResolver();
        final Class keyCls = clr.classForName(elementType);
        SQLTable containerSqlTbl = null;
        MapType mapType = getOwnerMemberMetaData().getMap().getMapType();
        if (emd != null && emd.getDiscriminatorStrategyForTable() != null &&
            emd.getDiscriminatorStrategyForTable() != DiscriminatorStrategy.NONE)
        {
            // Map<PC, ?> where key has discriminator
            if (ClassUtils.isReferenceType(keyCls))
            {
                // Take the metadata for the first implementation of the reference type
                String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(elementType, clr);
                Class[] cls = new Class[clsNames.length];
                for (int j=0; j<clsNames.length; j++)
                {
                    cls[j] = clr.classForName(clsNames[j]);
                }
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null).getStatement();
            }
            else
            {
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr,
                    clr.classForName(elementInfo[0].getClassName()), true, null, null).getStatement();
            }
            containerSqlTbl = sqlStmt.getPrimaryTable();
            iterateUsingDiscriminator = true;

            if (mapType == MapType.MAP_TYPE_VALUE_IN_KEY)
            {
                // Select key fields
                containerSqlTbl = sqlStmt.getPrimaryTable();

                iteratorMappingDef = new StatementClassMapping();
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef,
                    ownerOP.getExecutionContext().getFetchPlan(), sqlStmt.getPrimaryTable(), emd, 0);
            }
            else
            {
                // MAP_TYPE_KEY_IN_VALUE, MAP_TYPE_JOIN
                // Join to join table and select key fields
                JavaTypeMapping keyIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                containerSqlTbl = sqlStmt.innerJoin(sqlStmt.getPrimaryTable(), keyIdMapping,
                    containerTable, null, elementMapping, null, null);

                iteratorMappingDef = new StatementClassMapping();
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef,
                    ownerOP.getExecutionContext().getFetchPlan(), sqlStmt.getPrimaryTable(), emd, 0);
            }
        }
        else
        {
            if (mapType == MapType.MAP_TYPE_VALUE_IN_KEY)
            {
                // Select of key in key table (allow union of possible key types)
                iteratorMappingDef = new StatementClassMapping();
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, keyCls, true, null, null);
                stmtGen.setOption(StatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                iteratorMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.NUC_TYPE_COLUMN);
                sqlStmt = stmtGen.getStatement();
                containerSqlTbl = sqlStmt.getPrimaryTable();

                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef,
                    ownerOP.getExecutionContext().getFetchPlan(), sqlStmt.getPrimaryTable(), emd, 0);
            }
            else
            {
                // MAP_TYPE_KEY_IN_VALUE, MAP_TYPE_JOIN
                if (emd != null)
                {
                    // Select of key table, joining to join table
                    iteratorMappingDef = new StatementClassMapping();
                    UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, keyCls, true, null, null);
                    stmtGen.setOption(StatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                    iteratorMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.NUC_TYPE_COLUMN);
                    sqlStmt = stmtGen.getStatement();

                    JavaTypeMapping keyIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                    containerSqlTbl = sqlStmt.innerJoin(sqlStmt.getPrimaryTable(), keyIdMapping,
                        containerTable, null, elementMapping, null, null);

                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef,
                        ownerOP.getExecutionContext().getFetchPlan(), sqlStmt.getPrimaryTable(), emd, 0);
                }
                else
                {
                    // Select of key in join table
                    sqlStmt = new SQLStatement(storeMgr, containerTable, null, null);
                    sqlStmt.setClassLoaderResolver(clr);
                    containerSqlTbl = sqlStmt.getPrimaryTable();
                    SQLTable elemSqlTblForKey = containerSqlTbl;
                    if (elementMapping.getTable() != containerSqlTbl.getTable())
                    {
                        elemSqlTblForKey = sqlStmt.getTableForDatastoreContainer(elementMapping.getTable());
                        if (elemSqlTblForKey == null)
                        {
                            // Add join to element table
                            elemSqlTblForKey = sqlStmt.innerJoin(sqlStmt.getPrimaryTable(), sqlStmt.getPrimaryTable().getTable().getIdMapping(), 
                                elementMapping.getTable(), null, elementMapping.getTable().getIdMapping(), null, null);
                        }
                    }
                    sqlStmt.select(elemSqlTblForKey, elementMapping, null);
                }
            }
        }

        // Apply condition on owner field to filter by owner
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        SQLTable ownerSqlTbl =
            SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, containerSqlTbl, ownerMapping);
        SQLExpression ownerExpr = exprFactory.newExpression(sqlStmt, ownerSqlTbl, ownerMapping);
        SQLExpression ownerVal = exprFactory.newLiteralParameter(sqlStmt, ownerMapping, null, "OWNER");
        sqlStmt.whereAnd(ownerExpr.eq(ownerVal), true);

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
        iteratorMappingParams = new StatementParameterMapping();
        iteratorMappingParams.addMappingForParameter("owner", ownerIdx);

        return sqlStmt;
    }
}