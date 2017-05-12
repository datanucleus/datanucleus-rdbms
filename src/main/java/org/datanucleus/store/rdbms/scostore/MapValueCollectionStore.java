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
import org.datanucleus.Transaction;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.MappedDatastoreException;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.JDBCUtils;
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
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.types.scostore.MapStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;

/**
 * RDBMS-specific implementation of a CollectionStore for map values.
 */
class MapValueCollectionStore<V> extends AbstractCollectionStore<V>
{
    protected final MapStore<?, V> mapStore;

    protected final JavaTypeMapping keyMapping;

    private String findKeyStmt;

    /** JDBC statement to use for retrieving keys of the map (locking). */
    private String iteratorStmtLocked = null;

    /** JDBC statement to use for retrieving keys of the map (not locking). */
    private String iteratorStmtUnlocked = null;

    private StatementClassMapping iteratorMappingDef = null;
    private StatementParameterMapping iteratorMappingParams = null;

    /**
     * Constructor where a join table is used to store the map relation.
     * @param mapTable Join table used by the map
     * @param mapStore Backing store for the map
     * @param clr The ClassLoaderResolver
     * @param storeMgr Manager for the datastore
     */
    MapValueCollectionStore(MapTable mapTable, JoinMapStore<?, V> mapStore, ClassLoaderResolver clr)
    {
        super(mapTable.getStoreManager(), clr);

        this.containerTable = mapTable;
        this.mapStore = mapStore;
        this.ownerMapping = mapTable.getOwnerMapping();
        this.keyMapping = mapTable.getKeyMapping();
        this.elementMapping = mapTable.getValueMapping();
        this.elementType = elementMapping.getType();
        this.ownerMemberMetaData = mapTable.getOwnerMemberMetaData();

        initialize(clr);
    }

    /**
     * Constructor when we have the key stored as an FK in the value, or the value stored as an FK in the key.
     * @param mapTable Table handling the map relation (can be key table or value table)
     * @param mapStore Backing store for the map
     * @param clr ClassLoader resolver
     */
    MapValueCollectionStore(DatastoreClass mapTable, FKMapStore<?, V> mapStore, ClassLoaderResolver clr)
    {
        super(mapTable.getStoreManager(), clr);

        this.containerTable = mapTable;
        this.mapStore = mapStore;
        this.ownerMapping = mapStore.getOwnerMapping();
        this.keyMapping = null;
        this.elementMapping = mapStore.getValueMapping();
        this.ownerMemberMetaData = mapStore.getOwnerMemberMetaData();

        initialize(clr);
    }

    /**
     * Initialise Method.
     * @param clr ClassLoader resolver
     */
    private void initialize(ClassLoaderResolver clr)
    {
        elementType = elementMapping.getType();
        elementsAreEmbedded = isEmbeddedMapping(elementMapping);
        elementsAreSerialised = isEmbeddedMapping(elementMapping);

        Class valueCls = clr.classForName(elementType);
        if (ClassUtils.isReferenceType(valueCls))
        {
            elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForImplementationOfReference(valueCls,null,clr);
        }
        else
        {
            elementCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(valueCls, clr);
        }
        if (elementCmd != null)
        {
            elementType = elementCmd.getFullClassName();
            elementInfo = getComponentInformationForClass(elementType, elementCmd);
        }

        if (keyMapping != null)
        {
            findKeyStmt = getFindKeyStmt();
        }
        else
        {
            findKeyStmt = null;
        }
    }

    public boolean add(ObjectProvider op, V value, int size)
    {
        throw new UnsupportedOperationException("Cannot add to a map through its values collection");
    }

    public boolean addAll(ObjectProvider op, Collection<V> values, int size)
    {
        throw new UnsupportedOperationException("Cannot add to a map through its values collection");
    }

    public boolean remove(ObjectProvider op, Object value, int size, boolean allowDependentField)
    {
        // TODO Why does this even allow the possibility of removal via the values store?
        if (!validateElementForReading(op, value))
        {
            return false;
        }

        return remove(op, value);
    }

    public boolean removeAll(ObjectProvider op, Collection values, int size)
    {
        throw new NucleusUserException("Cannot remove values from a map through its values collection");
    }

    public void clear(ObjectProvider op)
    {
        throw new NucleusUserException("Cannot clear a map through its values collection");
    }

    protected boolean remove(ObjectProvider op, Object value)
    {
        if (findKeyStmt == null)
        {
            throw new UnsupportedOperationException("Cannot remove from a map through its values collection");
        }

        Object key = null;
        boolean keyExists = false;
        ExecutionContext ec = op.getExecutionContext();

        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();

            try
            {
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, findKeyStmt);

                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    BackingStoreHelper.populateElementInStatement(ec, ps, value, jdbcPosition, elementMapping);

                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, findKeyStmt, ps);
                    try
                    {
                        if (rs.next())
                        {
                            key = keyMapping.getObject(ec, rs, MappingHelper.getMappingIndices(1,keyMapping));
                            keyExists = true;
                        }

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
            throw new NucleusDataStoreException("Request failed to check if set contains an element: " + findKeyStmt, e);
        }

        if (keyExists)
        {
            mapStore.remove(op, key);
            return true;
        }
        return false;
    }

    /**
     * Generate statement to find the first key for a value in the Map.
     * <PRE>
     * SELECT KEYCOL FROM SETTABLE
     * WHERE OWNERCOL=?
     * AND ELEMENTCOL = ?
     * </PRE>
     * @return Statement to find keys in the Map.
     */
    private String getFindKeyStmt()
    {
        StringBuilder stmt = new StringBuilder("SELECT ");
        for (int i=0; i<keyMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(keyMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
        }
        stmt.append(" FROM ");
        stmt.append(containerTable.toString());
        stmt.append(" WHERE ");
        BackingStoreHelper.appendWhereClauseForMapping(stmt, ownerMapping, null, true);
        BackingStoreHelper.appendWhereClauseForMapping(stmt, elementMapping, null, false);

        return stmt.toString();
    }

    /**
     * Accessor for an iterator for the set.
     * @param ownerOP ObjectProvider for the set. 
     * @return Iterator for the set.
     **/
    public Iterator<V> iterator(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        if (iteratorStmtLocked == null)
        {
            synchronized (this) // Make sure this completes in case another thread needs the same info
            {
                // Generate the statement, and statement mapping/parameter information
                SQLStatement sqlStmt = getSQLStatementForIterator(ownerOP);
                iteratorStmtUnlocked = sqlStmt.getSQLText().toSQL();
                sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
                iteratorStmtLocked = sqlStmt.getSQLText().toSQL();
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
                    ownerIdx.getMapping().setObject(ec, ps, ownerIdx.getParameterPositionsForOccurrence(paramInstance), ownerOP.getObject());
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
                            return new CollectionStoreIterator(ownerOP, rs, null, this);
                        }

                        rof = new PersistentClassROF(storeMgr, elementCmd, iteratorMappingDef, false, null, clr.classForName(elementType));
                        return new CollectionStoreIterator(ownerOP, rs, rof, this);
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
     * Method to generate an SQLStatement for iterating through values of the map.
     * Populates the iteratorMappingDef and iteratorMappingParams.
     * Creates a statement that selects the value table(s), and adds any necessary join to the containerTable
     * if that is not the value table. If the value is embedded then selects the table it is embedded in.
     * Adds a restriction on the ownerMapping of the containerTable so we can restrict to the owner object.
     * @param ownerOP ObjectProvider for the owner object
     * @return The SQLStatement
     */
    protected SelectStatement getSQLStatementForIterator(ObjectProvider ownerOP)
    {
        SelectStatement sqlStmt = null;
        ExecutionContext ec = ownerOP.getExecutionContext();

        final ClassLoaderResolver clr = ec.getClassLoaderResolver();
        final Class valueCls = clr.classForName(elementType);
        SQLTable containerSqlTbl = null;
        MapType mapType = getOwnerMemberMetaData().getMap().getMapType();
        FetchPlan fp = ec.getFetchPlan();
        if (elementCmd != null && elementCmd.getDiscriminatorStrategyForTable() != null && elementCmd.getDiscriminatorStrategyForTable() != DiscriminatorStrategy.NONE)
        {
            // Map<?, PC> where value has discriminator
            if (ClassUtils.isReferenceType(valueCls))
            {
                // Take the metadata for the first implementation of the reference type
                String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(elementType, clr);
                Class[] cls = new Class[clsNames.length];
                for (int j=0; j<clsNames.length; j++)
                {
                    cls[j] = clr.classForName(clsNames[j]);
                }
                SelectStatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null);
                sqlStmt = stmtGen.getStatement(ec);
            }
            else
            {
                SelectStatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, valueCls, true, null, null);
                sqlStmt = stmtGen.getStatement(ec);
            }
            iterateUsingDiscriminator = true;

            if (mapType == MapType.MAP_TYPE_VALUE_IN_KEY)
            {
                // Join to key table and select value fields
                JavaTypeMapping valueIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                containerSqlTbl = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), valueIdMapping, containerTable, null, elementMapping, null, null, true);

                iteratorMappingDef = new StatementClassMapping();
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
            }
            else if (mapType == MapType.MAP_TYPE_KEY_IN_VALUE)
            {
                // Select value fields
                containerSqlTbl = sqlStmt.getPrimaryTable();

                iteratorMappingDef = new StatementClassMapping();
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
            }
            else
            {
                // Join to join table and select value fields
                JavaTypeMapping valueIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                containerSqlTbl = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), valueIdMapping, containerTable, null, elementMapping, null, null, true);

                iteratorMappingDef = new StatementClassMapping();
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
            }
        }
        else
        {
            if (mapType == MapType.MAP_TYPE_VALUE_IN_KEY)
            {
                if (elementCmd != null)
                {
                    // TODO Allow for null value [change to select the key table and left outer join to the key]
                    // Select of value table, joining to key table
                    iteratorMappingDef = new StatementClassMapping();
                    UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, valueCls, true, null, null);
                    stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                    iteratorMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                    sqlStmt = stmtGen.getStatement(ec);

                    JavaTypeMapping valueIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                    containerSqlTbl = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), valueIdMapping, containerTable, null, elementMapping, null, null, true);

                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
                }
                else
                {
                    // Select of value in key table
                    sqlStmt = new SelectStatement(storeMgr, containerTable, null, null);
                    sqlStmt.setClassLoaderResolver(clr);
                    containerSqlTbl = sqlStmt.getPrimaryTable();
                    SQLTable elemSqlTblForValue = containerSqlTbl;
                    if (elementMapping.getTable() != containerSqlTbl.getTable())
                    {
                        elemSqlTblForValue = sqlStmt.getTableForDatastoreContainer(elementMapping.getTable());
                        if (elemSqlTblForValue == null)
                        {
                            // Add join to key table holding value
                            elemSqlTblForValue = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), sqlStmt.getPrimaryTable().getTable().getIdMapping(), 
                                elementMapping.getTable(), null, elementMapping.getTable().getIdMapping(), null, null, true);
                        }
                    }
                    sqlStmt.select(elemSqlTblForValue, elementMapping, null);
                }
            }
            else if (mapType == MapType.MAP_TYPE_KEY_IN_VALUE)
            {
                // Select of value in value table (allow union of possible value types)
                iteratorMappingDef = new StatementClassMapping();
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, valueCls, true, null, null);
                stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                iteratorMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                sqlStmt = stmtGen.getStatement(ec);
                containerSqlTbl = sqlStmt.getPrimaryTable();

                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
            }
            else
            {
                if (elementCmd != null)
                {
                    // TODO Allow for null value [change to select the join table and left outer join to the key]
                    // Select of value table, joining to key table
                    iteratorMappingDef = new StatementClassMapping();
                    UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, valueCls, true, null, null);
                    stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                    iteratorMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                    sqlStmt = stmtGen.getStatement(ec);

                    JavaTypeMapping valueIdMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                    containerSqlTbl = sqlStmt.join(JoinType.INNER_JOIN, sqlStmt.getPrimaryTable(), valueIdMapping, containerTable, null, elementMapping, null, null, true);

                    SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, iteratorMappingDef, fp, sqlStmt.getPrimaryTable(), elementCmd, fp.getMaxFetchDepth());
                }
                else
                {
                    // Select of value in join table
                    sqlStmt = new SelectStatement(storeMgr, containerTable, null, null);
                    containerSqlTbl = sqlStmt.getPrimaryTable();
                    sqlStmt.select(sqlStmt.getPrimaryTable(), elementMapping, null);
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