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
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;
import org.datanucleus.store.rdbms.query.StatementParameterMapping;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SelectStatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.types.scostore.MapStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;

/**
 * Implementation of a backing SetStore for map keys.
 */
class MapKeySetStore<K> extends AbstractSetStore<K>
{
    /** Backing store for the map. */
    protected final MapStore<K, ?> mapStore;

    /** JDBC statement to use for retrieving keys of the map (locking). */
    private String iteratorStmtLocked = null;

    /** JDBC statement to use for retrieving keys of the map (not locking). */
    private String iteratorStmtUnlocked = null;

    private StatementClassMapping iteratorMappingDef = null;
    private StatementParameterMapping iteratorMappingParams = null;

    /**
     * Constructor where a join table is used to store the map relation.
     * @param mapTable Join table used by the map (join table)
     * @param mapStore Backing store for the map
     * @param clr The ClassLoaderResolver
     */
    MapKeySetStore(MapTable mapTable, JoinMapStore<K, ?> mapStore, ClassLoaderResolver clr)
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
     */
    MapKeySetStore(DatastoreClass mapTable, FKMapStore<K, ?> mapStore, ClassLoaderResolver clr)
    {
        super(mapTable.getStoreManager(), clr);

        this.mapStore = mapStore;
        this.containerTable = mapTable;
        this.ownerMemberMetaData = mapStore.getOwnerMemberMetaData();
        this.ownerMapping = mapStore.getOwnerMapping();
        this.elementMapping = mapStore.getKeyMapping();

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
        Class elementCls = clr.classForName(elementType);

        if (ClassUtils.isReferenceType(elementCls))
        {
            elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForImplementationOfReference(elementCls, null, clr);
        }
        else
        {
            elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(elementCls, clr);
        }
        if (elementCmd != null)
        {
            elementType = elementCmd.getFullClassName();
            elementInfo = getComponentInformationForClass(elementType, elementCmd);
        } 
    }

    public boolean add(DNStateManager sm, K key, int size)
    {
        throw new UnsupportedOperationException("Cannot add to a map through its key set");
    }

    public boolean addAll(DNStateManager sm, Collection keys, int size)
    {
        throw new UnsupportedOperationException("Cannot add to a map through its key set");
    }

    public boolean remove(DNStateManager sm, Object key, int size, boolean allowDependentField)
    {
        throw new UnsupportedOperationException("Cannot remove from a map through its key set");
    }

    public boolean removeAll(DNStateManager sm, Collection keys, int size)
    {
        throw new UnsupportedOperationException("Cannot remove from a map through its key set");
    }

    public void clear(DNStateManager sm)
    {
        throw new UnsupportedOperationException("Cannot clear a map through its key set");
    }

    /**
     * Accessor for an iterator for the set.
     * @param ownerSM StateManager for the set. 
     * @return Iterator for the set.
     */
    public Iterator<K> iterator(DNStateManager ownerSM)
    {
        ExecutionContext ec = ownerSM.getExecutionContext();
        if (iteratorStmtLocked == null)
        {
            synchronized (this) // Make sure this completes in case another thread needs the same info
            {
                // Generate the statement, and statement mapping/parameter information
                SQLStatement sqlStmt = getSQLStatementForIterator(ownerSM);
                iteratorStmtUnlocked = sqlStmt.getSQLText().toSQL();
                sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
                iteratorStmtLocked = sqlStmt.getSQLText().toSQL();
            }
        }

        Boolean serializeRead = ec.getTransaction().getSerializeRead();
        String stmt = (serializeRead != null && serializeRead ? iteratorStmtLocked : iteratorStmtUnlocked);
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
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
                        ownerIdx.getParameterPositionsForOccurrence(paramInstance), ownerSM.getObject());
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
                            return new CollectionStoreIterator(ownerSM, rs, null, this);
                        }

                        rof = new PersistentClassROF(ec, rs, false, ec.getFetchPlan(), iteratorMappingDef, elementCmd, clr.classForName(elementType));
                        return new CollectionStoreIterator(ownerSM, rs, rof, this);
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
            throw new NucleusDataStoreException(Localiser.msg("056006", stmt),e);
        }
        catch (MappedDatastoreException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056006", stmt),e);
        }
    }

    /**
     * Method to generate an SQLStatement for iterating through keys of the map.
     * Populates the iteratorMappingDef and iteratorMappingParams.
     * Creates a statement that selects the key table(s), and adds any necessary join to the containerTable
     * if that is not the key table. If the key is embedded then selects the table it is embedded in.
     * Adds a restriction on the ownerMapping of the containerTable so we can restrict to the owner object.
     * @param ownerSM StateManager for the owner object
     * @return The SQLStatement
     */
    protected SelectStatement getSQLStatementForIterator(DNStateManager ownerSM)
    {
        SelectStatement sqlStmt = null;
        ExecutionContext ec = ownerSM.getExecutionContext();

        final ClassLoaderResolver clr = ec.getClassLoaderResolver();
        final Class keyCls = clr.classForName(elementType);
        SQLTable containerSqlTbl = null;
        MapType mapType = getOwnerMemberMetaData().getMap().getMapType();
        FetchPlan fp = ec.getFetchPlan();
        if (elementCmd != null && elementCmd.getDiscriminatorStrategyForTable() != null &&
            elementCmd.getDiscriminatorStrategyForTable() != DiscriminatorStrategy.NONE)
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
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null).getStatement(ec);
            }
            else
            {
                sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, clr.classForName(elementInfo[0].getClassName()), true, null, null).getStatement(ec);
            }
            containerSqlTbl = sqlStmt.getPrimaryTable();
            iterateUsingDiscriminator = true;

            if (mapType == MapType.MAP_TYPE_VALUE_IN_KEY)
            {
                // Select key fields
                containerSqlTbl = sqlStmt.getPrimaryTable();

                iteratorMappingDef = new StatementClassMapping();
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
            }
            else
            {
                // MAP_TYPE_KEY_IN_VALUE, MAP_TYPE_JOIN
                // Join to join table and select key fields
                JavaTypeMapping keyIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                containerSqlTbl = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), keyIdMapping, containerTable, null, elementMapping, null, null, true);

                iteratorMappingDef = new StatementClassMapping();
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
            }
        }
        else
        {
            if (mapType == MapType.MAP_TYPE_VALUE_IN_KEY)
            {
                // Select of key in key table (allow union of possible key types)
                iteratorMappingDef = new StatementClassMapping();
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, keyCls, true, null, null);
                stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                iteratorMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                sqlStmt = stmtGen.getStatement(ec);
                containerSqlTbl = sqlStmt.getPrimaryTable();

                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
            }
            else
            {
                // MAP_TYPE_KEY_IN_VALUE, MAP_TYPE_JOIN
                if (elementCmd != null)
                {
                    // Select of key table, joining to join table
                    iteratorMappingDef = new StatementClassMapping();
                    UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, keyCls, true, null, null);
                    stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                    iteratorMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                    sqlStmt = stmtGen.getStatement(ec);

                    JavaTypeMapping keyIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                    containerSqlTbl = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), keyIdMapping, containerTable, null, elementMapping, null, null, true);

                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
                }
                else
                {
                    // Select of key in join table
                    sqlStmt = new SelectStatement(storeMgr, containerTable, null, null);
                    sqlStmt.setClassLoaderResolver(clr);
                    containerSqlTbl = sqlStmt.getPrimaryTable();
                    SQLTable elemSqlTblForKey = containerSqlTbl;
                    if (elementMapping.getTable() != containerSqlTbl.getTable())
                    {
                        elemSqlTblForKey = sqlStmt.getTableForDatastoreContainer(elementMapping.getTable());
                        if (elemSqlTblForKey == null)
                        {
                            // Add join to element table
                            elemSqlTblForKey = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), sqlStmt.getPrimaryTable().getTable().getIdMapping(),
                                elementMapping.getTable(), null, elementMapping.getTable().getIdMapping(), null, null, true);
                        }
                    }
                    sqlStmt.select(elemSqlTblForKey, elementMapping, null);
                }
            }
        }

        // Apply condition on owner field to filter by owner
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        SQLTable ownerSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, containerSqlTbl, ownerMapping);
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
                int[] paramPositions = new int[ownerMapping.getNumberOfColumnMappings()];
                for (int k=0;k<ownerMapping.getNumberOfColumnMappings();k++)
                {
                    paramPositions[k] = inputParamNum++;
                }
                ownerIdx.addParameterOccurrence(paramPositions);
            }
        }
        else
        {
            int[] paramPositions = new int[ownerMapping.getNumberOfColumnMappings()];
            for (int k=0;k<ownerMapping.getNumberOfColumnMappings();k++)
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