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
package org.datanucleus.store.rdbms.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.JoinMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.DiscriminatorLongMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.DatastoreElementContainer;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.PersistableJoinTable;
import org.datanucleus.store.rdbms.table.SecondaryDatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * Series of convenience methods to help the process of generating SQLStatements.
 */
public class SQLStatementHelper
{
    /**
     * Convenience method to return a PreparedStatement for an SQLStatement.
     * @param sqlStmt The query expression
     * @param ec execution context
     * @param mconn The connection to use
     * @param resultSetType Type of result set (if any)
     * @param resultSetConcurrency result-set concurrency (if any)
     * @return The PreparedStatement
     * @throws SQLException If an error occurs in creation
     */
    public static PreparedStatement getPreparedStatementForSQLStatement(SQLStatement sqlStmt, 
            ExecutionContext ec, ManagedConnection mconn, String resultSetType, String resultSetConcurrency)
    throws SQLException
    {
        SQLText sqlText = sqlStmt.getSQLText();
        SQLController sqlControl = sqlStmt.getRDBMSManager().getSQLController();

        // Generate the statement using the statement text
        PreparedStatement ps = sqlControl.getStatementForQuery(mconn, sqlText.toString(), resultSetType, resultSetConcurrency);

        boolean done = false;
        try
        {
            // Apply any parameter values for the statement
            sqlText.applyParametersToStatement(ec, ps);
            done = true;
        }
        finally
        {
            if (!done)
            {
                sqlControl.closeStatement(mconn, ps);
            }
        }

        return ps;
    }

    /**
     * Convenience method to apply parameter values to the provided statement.
     * @param ps The prepared statement
     * @param ec ExecutionContext
     * @param parameters The parameters
     * @param paramNameByPosition Optional map of parameter names keyed by the position
     * @param paramValuesByName Value of parameter keyed by name (or position)
     */
    public static void applyParametersToStatement(PreparedStatement ps, ExecutionContext ec,
            List<SQLStatementParameter> parameters, Map<Integer, String> paramNameByPosition, Map paramValuesByName)
    {
        if (parameters != null)
        {
            int num = 1;
            Map<String, Integer> paramNumberByName = null;
            int nextParamNumber = 0;

            Iterator<SQLStatementParameter> i = parameters.iterator();
            while (i.hasNext())
            {
                SQLStatementParameter param = i.next();
                JavaTypeMapping mapping = param.getMapping();
                RDBMSStoreManager storeMgr = mapping.getStoreManager();

                // Find the overall parameter value for this parameter
                Object value = null;
                if (paramNumberByName != null)
                {
                    // Parameters are numbered but the query has named, so use lookup
                    Integer position = paramNumberByName.get("" + param.getName());
                    if (position == null)
                    {
                        value = paramValuesByName.get(Integer.valueOf(nextParamNumber));
                        paramNumberByName.put(param.getName(), nextParamNumber);
                        nextParamNumber++;
                    }
                    else
                    {
                        value = paramValuesByName.get(position);
                    }
                }
                else
                {
                    if (paramValuesByName.containsKey(param.getName()))
                    {
                        // Named parameter has value in the input map
                        value = paramValuesByName.get(param.getName());
                    }
                    else
                    {
                        // Named parameter doesn't have value, so maybe using numbered input params
                        if (paramNameByPosition != null)
                        {
                            int paramPosition = -1;
                            Set<String> paramNamesEncountered = new HashSet<String>();
                            for (Map.Entry<Integer, String> entry : paramNameByPosition.entrySet())
                            {
                                String paramName = entry.getValue();
                                if (!paramNamesEncountered.contains(paramName))
                                {
                                    paramPosition++;
                                    paramNamesEncountered.add(paramName);
                                }
                                if (paramName.equals(param.getName()))
                                {
                                    value = paramValuesByName.get(paramPosition);
                                    break;
                                }
                            }
                            paramNamesEncountered.clear();
                            paramNamesEncountered = null;
                        }
                        else
                        {
                            try
                            {
                                value = paramValuesByName.get(Integer.valueOf(param.getName()));
                            }
                            catch (NumberFormatException nfe)
                            {
                                value = paramValuesByName.get(Integer.valueOf(nextParamNumber));
                                paramNumberByName = new HashMap<String, Integer>();
                                paramNumberByName.put(param.getName(), Integer.valueOf(nextParamNumber));
                                nextParamNumber++;
                            }
                        }
                    }
                }

                AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(mapping.getType(), ec.getClassLoaderResolver());
                if (param.getColumnNumber() >= 0 && cmd != null)
                {
                    // Apply the value for this column of the mapping
                    Object colValue = null;
                    if (value != null)
                    {
                        if (cmd.getIdentityType() == IdentityType.DATASTORE)
                        {
                            colValue = mapping.getValueForDatastoreMapping(ec.getNucleusContext(), param.getColumnNumber(), value);
                        }
                        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                        {
                            colValue = getValueForPrimaryKeyIndexOfObjectUsingReflection(value, param.getColumnNumber(), cmd, storeMgr, ec.getClassLoaderResolver());
                        }
                    }
                    mapping.getDatastoreMapping(param.getColumnNumber()).setObject(ps, num, colValue);
                }
                else
                {
                    // Apply the value as a whole
                    if (ec.getApiAdapter().isPersistable(value))
                    {
                        if (!ec.getApiAdapter().isPersistent(value) && !ec.getApiAdapter().isDetached(value))
                        {
                            // Transient persistable object, so don't use since would cause its persistence
                            mapping.setObject(ec, ps, MappingHelper.getMappingIndices(num, mapping), null);
                        }
                        else if (ec.getApiAdapter().isDetached(value))
                        {
                            // Detached, so avoid re-attaching
                            Object id = ec.getApiAdapter().getIdForObject(value);
                            PersistableIdMapping idMapping = new PersistableIdMapping((PersistableMapping)mapping);
                            idMapping.setObject(ec, ps, MappingHelper.getMappingIndices(num, idMapping), id);
                        }
                        else
                        {
                            mapping.setObject(ec, ps, MappingHelper.getMappingIndices(num, mapping), value);
                        }
                    }
                    else
                    {
                        if (mapping.getNumberOfDatastoreMappings() == 1)
                        {
                            // Set whole object and only 1 column
                            mapping.setObject(ec, ps, MappingHelper.getMappingIndices(num, mapping), value);
                        }
                        else if (mapping.getNumberOfDatastoreMappings() > 1 && param.getColumnNumber() == (mapping.getNumberOfDatastoreMappings()-1))
                        {
                            // Set whole object and this is the last parameter entry for it, so set now
                            mapping.setObject(ec, ps, MappingHelper.getMappingIndices(num - mapping.getNumberOfDatastoreMappings()+1, mapping), value);
                        }
                    }
                }
                num++;
            }
        }
    }

    /**
     * Convenience method to use reflection to extract the value of a PK field of the provided object.
     * @param value The value of the overall object
     * @param index Index of the PK field whose value we need to return
     * @param cmd Metadata for the class
     * @param storeMgr Store manager
     * @param clr ClassLoader resolver
     * @return Value of this index of the PK field
     */
    public static Object getValueForPrimaryKeyIndexOfObjectUsingReflection(Object value, int index, AbstractClassMetaData cmd, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            throw new NucleusException("This method does not support datastore-identity");
        }

        int position = 0;
        int[] pkPositions = cmd.getPKMemberPositions();
        for (int i=0;i<pkPositions.length;i++)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPositions[i]);
            Object memberValue = null;
            if (mmd instanceof FieldMetaData)
            {
                memberValue = ClassUtils.getValueOfFieldByReflection(value, mmd.getName());
            }
            else
            {
                memberValue = ClassUtils.getValueOfMethodByReflection(value, ClassUtils.getJavaBeanGetterName(mmd.getName(), false));
            }

            if (storeMgr.getApiAdapter().isPersistable(mmd.getType()))
            {
                // Compound PK field, so recurse
                // TODO Cater for cases without own table ("subclass-table")
                AbstractClassMetaData subCmd = storeMgr.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                DatastoreClass subTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
                JavaTypeMapping subMapping = subTable.getIdMapping();
                Object subValue = getValueForPrimaryKeyIndexOfObjectUsingReflection(memberValue, index-position, subCmd, storeMgr, clr);
                if (index < position + subMapping.getNumberOfDatastoreMappings())
                {
                    return subValue;
                }
                position = position + subMapping.getNumberOfDatastoreMappings();
            }
            else
            {
                // Normal PK field
                if (position == index)
                {
                    if (mmd instanceof FieldMetaData)
                    {
                        return ClassUtils.getValueOfFieldByReflection(value, mmd.getName());
                    }
                    return ClassUtils.getValueOfMethodByReflection(value, ClassUtils.getJavaBeanGetterName(mmd.getName(), false));
                }
                position++;
            }
        }

        return null;
    }

    /**
     * Method to return the SQLTable where the specified mapping (in the same table group as the provided
     * SQLTable) is defined. If the statement doesn't currently join to the required table then a join will
     * be added. If the required table is a superclass table then the join will be INNER. If the required
     * table is a secondary table then the join will be defined by the meta-data for the secondary table.
     * If this table group is NOT the candidate table group then LEFT OUTER JOIN will be used.
     * @param stmt The statement
     * @param sqlTbl SQLTable to start from for the supplied mapping (may be in super-table, or secondary-table of this)
     * @param mapping The mapping
     * @return The SQLTable for this mapping (may have been added to the statement during this method)
     */
    public static SQLTable getSQLTableForMappingOfTable(SQLStatement stmt, SQLTable sqlTbl, JavaTypeMapping mapping)
    {
        Table table = sqlTbl.getTable();
        if (table instanceof SecondaryDatastoreClass || table instanceof JoinTable)
        {
            // Secondary/join tables have no inheritance so ought to be correct
            if (mapping.getTable() != null)
            {
                // Check there is no better table already present in the TableGroup for this mapping
                // This can happen when we do a select of a join table and the element table is in the
                // same table group, so hence already is present
                SQLTable mappingSqlTbl = stmt.getTable(mapping.getTable(), sqlTbl.getGroupName());
                if (mappingSqlTbl != null)
                {
                    return mappingSqlTbl;
                }
            }
            return sqlTbl;
        }

        DatastoreClass sourceTbl = (DatastoreClass)sqlTbl.getTable();
        DatastoreClass mappingTbl = null;
        if (mapping.getTable() != null)
        {
            mappingTbl = (DatastoreClass)mapping.getTable();
        }
        else
        {
            mappingTbl = sourceTbl.getBaseDatastoreClassWithMember(mapping.getMemberMetaData());
        }
        if (mappingTbl == sourceTbl)
        {
            return sqlTbl;
        }

        // Try to find this datastore table in the same table group
        SQLTable mappingSqlTbl = stmt.getTable(mappingTbl, sqlTbl.getGroupName());
        if (mappingSqlTbl == null)
        {
            boolean forceLeftOuter = false;
            SQLTableGroup tableGrp = stmt.getTableGroup(sqlTbl.getGroupName());
            if (tableGrp.getJoinType() == JoinType.LEFT_OUTER_JOIN)
            {
                // This group isn't the candidate group, and we joined to the candidate group using
                // a left outer join originally, so use the same type for this table
                forceLeftOuter = true;
            }

            if (mappingTbl instanceof SecondaryDatastoreClass)
            {
                // Secondary table, so add inner/outer based on metadata
                boolean innerJoin = true;
                JoinMetaData joinmd = ((SecondaryDatastoreClass)mappingTbl).getJoinMetaData();
                if (joinmd != null && joinmd.isOuter() && !forceLeftOuter)
                {
                    innerJoin = false;
                }
                if (innerJoin && !forceLeftOuter)
                {
                    // Add join from {sourceTbl}.ID to {secondaryTbl}.ID
                    mappingSqlTbl = stmt.innerJoin(sqlTbl, sqlTbl.getTable().getIdMapping(), mappingTbl, null, mappingTbl.getIdMapping(), null, sqlTbl.getGroupName());
                }
                else
                {
                    // Add join from {sourceTbl}.ID to {secondaryTbl}.ID
                    mappingSqlTbl = stmt.leftOuterJoin(sqlTbl, sqlTbl.getTable().getIdMapping(), mappingTbl, null, mappingTbl.getIdMapping(), null, sqlTbl.getGroupName());
                }
            }
            else
            {
                if (forceLeftOuter)
                {
                    // Add join from {sourceTbl}.ID to {superclassTbl}.ID
                    mappingSqlTbl = stmt.leftOuterJoin(sqlTbl, sqlTbl.getTable().getIdMapping(), mappingTbl, null, mappingTbl.getIdMapping(), null, sqlTbl.getGroupName());
                }
                else
                {
                    // Add join from {sourceTbl}.ID to {superclassTbl}.ID
                    mappingSqlTbl = stmt.innerJoin(sqlTbl, sqlTbl.getTable().getIdMapping(), mappingTbl, null, mappingTbl.getIdMapping(), null, sqlTbl.getGroupName());
                }
            }
        }
        return mappingSqlTbl;
    }

    /**
     * Method to select the identity for the candidate class.
     * The supplied statement and mapping definition are updated during this method.
     * Selects the datastore id (if using datastore id) as "DN_DATASTOREID", 
     * the version (if present) as "DN_VERSION", the discriminator (if present) as "DN_DISCRIM",
     * and the application id (if using application id) as "DN_APPID_{i}"
     * @param stmt The statement
     * @param mappingDefinition Mapping definition for result columns
     * @param candidateCmd The candidate class meta-data
     */
    public static void selectIdentityOfCandidateInStatement(SelectStatement stmt, StatementClassMapping mappingDefinition, AbstractClassMetaData candidateCmd)
    {
        DatastoreClass candidateTbl = (DatastoreClass)stmt.getPrimaryTable().getTable();

        if (candidateCmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Datastore-identity surrogate column
            JavaTypeMapping idMapping = candidateTbl.getDatastoreIdMapping();
            int[] colNumbers = stmt.select(stmt.getPrimaryTable(), idMapping, "DN_DATASTOREID", false);
            if (mappingDefinition != null)
            {
                StatementMappingIndex datastoreIdIdx = new StatementMappingIndex(idMapping);
                datastoreIdIdx.setColumnPositions(colNumbers);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_DATASTORE_ID, datastoreIdIdx);
            }
        }
        else if (candidateCmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Application-identity column(s)
            int[] pkPositions = candidateCmd.getPKMemberPositions();
            String alias = "DN_APPID";
            for (int i=0;i<pkPositions.length;i++)
            {
                AbstractMemberMetaData pkMmd = candidateCmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPositions[i]);
                JavaTypeMapping pkMapping = candidateTbl.getMemberMapping(pkMmd);
                SQLTable sqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, stmt.getPrimaryTable(), pkMapping);
                if (pkPositions.length > 1)
                {
                    alias = "DN_APPID" + i;
                }
                int[] colNumbers = stmt.select(sqlTbl, pkMapping, alias, false);
                if (mappingDefinition != null)
                {
                    StatementMappingIndex appIdIdx = new StatementMappingIndex(pkMapping);
                    appIdIdx.setColumnPositions(colNumbers);
                    mappingDefinition.addMappingForMember(pkPositions[i], appIdIdx);
                }
            }
        }

        JavaTypeMapping verMapping = candidateTbl.getVersionMapping(true);
        if (verMapping != null)
        {
            // Version surrogate column (adds inner join to any required superclass table)
            SQLTable versionSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, stmt.getPrimaryTable(), verMapping);
            int[] colNumbers = stmt.select(versionSqlTbl, verMapping, "DN_VERSION", false);
            if (mappingDefinition != null)
            {
                StatementMappingIndex versionIdx = new StatementMappingIndex(verMapping);
                versionIdx.setColumnPositions(colNumbers);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_VERSION, versionIdx);
            }
        }

        JavaTypeMapping discrimMapping = candidateTbl.getDiscriminatorMapping(true);
        if (discrimMapping != null)
        {
            // Discriminator surrogate column (adds inner join to any required superclass table)
            SQLTable discrimSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, stmt.getPrimaryTable(), discrimMapping);
            int[] colNumbers = stmt.select(discrimSqlTbl, discrimMapping, "DN_DISCRIM", false);
            if (mappingDefinition != null)
            {
                StatementMappingIndex discrimIdx = new StatementMappingIndex(discrimMapping);
                discrimIdx.setColumnPositions(colNumbers);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_DISCRIMINATOR, discrimIdx);
            }
        }

        List<SelectStatement> unionStmts = stmt.getUnions();
        if (unionStmts != null)
        {
            Iterator<SelectStatement> iter = unionStmts.iterator();
            while (iter.hasNext())
            {
                SelectStatement unionStmt = iter.next();
                selectIdentityOfCandidateInStatement(unionStmt, null, candidateCmd);
            }
        }
    }

    /**
     * Method to select all fetch plan members for the candidate class.
     * The supplied statement and mapping definition are updated during this method.
     * Shortcut to calling "selectFetchPlanOfSourceClassInStatement".
     * @param stmt The statement
     * @param mappingDefinition Mapping definition for result columns
     * @param candidateCmd The candidate class meta-data
     * @param fetchPlan FetchPlan in use
     * @param maxFetchDepth Max fetch depth from this point to select (0 implies no other objects)
     */
    public static void selectFetchPlanOfCandidateInStatement(SelectStatement stmt, StatementClassMapping mappingDefinition, AbstractClassMetaData candidateCmd,
            FetchPlan fetchPlan, int maxFetchDepth)
    {
        selectFetchPlanOfSourceClassInStatement(stmt, mappingDefinition, fetchPlan, stmt.getPrimaryTable(), candidateCmd, maxFetchDepth);
    }

    /**
     * Method to select all fetch plan members for the "source" class.
     * If the passed FetchPlan is null then the default fetch group fields will be selected.
     * The source class is defined by the supplied meta-data, and the SQLTable that we are selecting from.
     * The supplied statement and mapping definition are updated during this method.
     * @param stmt The statement
     * @param mappingDefinition Mapping definition for result columns (populated with column positions
     *                          of any selected mappings if provided as input)
     * @param fetchPlan FetchPlan in use
     * @param sourceSqlTbl SQLTable for the source class that we select from
     * @param sourceCmd Meta-data for the source class
     * @param maxFetchDepth Max fetch depth from this point to select (0 implies no other objects)
     */
    public static void selectFetchPlanOfSourceClassInStatement(SelectStatement stmt, StatementClassMapping mappingDefinition, FetchPlan fetchPlan,
            SQLTable sourceSqlTbl, AbstractClassMetaData sourceCmd, int maxFetchDepth)
    {
        selectFetchPlanOfSourceClassInStatement(stmt, mappingDefinition, fetchPlan, sourceSqlTbl, sourceCmd, maxFetchDepth, null);
    }

    /**
     * Method to select all fetch plan members for the "source" class.
     * If the passed FetchPlan is null then the default fetch group fields will be selected.
     * The source class is defined by the supplied meta-data, and the SQLTable that we are selecting from.
     * The supplied statement and mapping definition are updated during this method.
     * @param stmt The statement
     * @param mappingDefinition Mapping definition for result columns (populated with column positions
     *                          of any selected mappings if provided as input)
     * @param fetchPlan FetchPlan in use
     * @param sourceSqlTbl SQLTable for the source class that we select from
     * @param sourceCmd Meta-data for the source class
     * @param maxFetchDepth Max fetch depth from this point to select (0 implies no other objects)
     * @param inputJoinType Optional join type to use for subobjects (otherwise decide join type internally)
     */
    public static void selectFetchPlanOfSourceClassInStatement(SelectStatement stmt, StatementClassMapping mappingDefinition, FetchPlan fetchPlan,
            SQLTable sourceSqlTbl, AbstractClassMetaData sourceCmd, int maxFetchDepth, JoinType inputJoinType)
    {
        DatastoreClass sourceTbl = (DatastoreClass)sourceSqlTbl.getTable();
        int[] fieldNumbers;
        if (fetchPlan != null)
        {
            // Use FetchPlan fields
            fieldNumbers = fetchPlan.getFetchPlanForClass(sourceCmd).getMemberNumbers();
        }
        else
        {
            // Use DFG fields
            fieldNumbers = sourceCmd.getDFGMemberPositions();
        }

        ClassLoaderResolver clr = stmt.getRDBMSManager().getNucleusContext().getClassLoaderResolver(null);
        for (int i=0;i<fieldNumbers.length;i++)
        {
            AbstractMemberMetaData mmd = sourceCmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
            selectMemberOfSourceInStatement(stmt, mappingDefinition, fetchPlan, sourceSqlTbl, mmd, clr, maxFetchDepth, inputJoinType);
        }

        if (sourceCmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Datastore-identity surrogate column
            JavaTypeMapping idMapping = sourceTbl.getDatastoreIdMapping();
            int[] colNumbers = stmt.select(sourceSqlTbl, idMapping, null);
            if (mappingDefinition != null)
            {
                StatementMappingIndex datastoreIdIdx = new StatementMappingIndex(idMapping);
                datastoreIdIdx.setColumnPositions(colNumbers);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_DATASTORE_ID, datastoreIdIdx);
            }
        }

        JavaTypeMapping verMapping = sourceTbl.getVersionMapping(true);
        if (verMapping != null)
        {
            // Version surrogate column (adds inner join to any required superclass table)
            SQLTable versionSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, sourceSqlTbl, verMapping);
            int[] colNumbers = stmt.select(versionSqlTbl, verMapping, null);
            if (mappingDefinition != null)
            {
                StatementMappingIndex versionIdx = new StatementMappingIndex(verMapping);
                versionIdx.setColumnPositions(colNumbers);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_VERSION, versionIdx);
            }
        }

        JavaTypeMapping discrimMapping = sourceTbl.getDiscriminatorMapping(true);
        if (discrimMapping != null)
        {
            // Discriminator surrogate column (adds inner join to any required superclass table)
            SQLTable discrimSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, sourceSqlTbl, discrimMapping);
            int[] colNumbers = stmt.select(discrimSqlTbl, discrimMapping, null);
            if (mappingDefinition != null)
            {
                StatementMappingIndex discrimIdx = new StatementMappingIndex(discrimMapping);
                discrimIdx.setColumnPositions(colNumbers);
                mappingDefinition.addMappingForMember(StatementClassMapping.MEMBER_DISCRIMINATOR, discrimIdx);
            }
        }
    }

    /**
     * Method to select the specified member (field/property) of the source table in the passed SQL 
     * statement. This populates the mappingDefinition with the column details for this member.
     * @param stmt The SQL statement
     * @param mappingDefinition Mapping definition for the results (will be populated by any
     *                          selected mappings if provided as input)
     * @param fetchPlan FetchPlan
     * @param sourceSqlTbl Table that has the member (or a super-table/secondary-table of this table)
     * @param mmd Meta-data for the field/property in the source that we are selecting
     * @param clr ClassLoader resolver
     * @param maxFetchPlanLimit Max fetch depth from this point to select (0 implies no other objects)
     * @param inputJoinType Optional join type to use for subobjects (otherwise decide join type internally)
     */
    public static void selectMemberOfSourceInStatement(SelectStatement stmt, StatementClassMapping mappingDefinition, FetchPlan fetchPlan,
            SQLTable sourceSqlTbl, AbstractMemberMetaData mmd, ClassLoaderResolver clr, int maxFetchPlanLimit, JoinType inputJoinType)
    {
        boolean selectSubobjects = false;
        if (maxFetchPlanLimit > 0)
        {
            selectSubobjects = true;
        }

        // Set table-group name for any related object we join to (naming based on member name)
        String tableGroupName = sourceSqlTbl.getGroupName() + "." + mmd.getName();
        JavaTypeMapping m = sourceSqlTbl.getTable().getMemberMapping(mmd);
        if (m != null && m.includeInFetchStatement())
        {
            RelationType relationType = mmd.getRelationType(clr);
            RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
            DatastoreAdapter dba = storeMgr.getDatastoreAdapter();
            if (!dba.validToSelectMappingInStatement(stmt, m))
            {
                // Not valid to select this mapping for this statement so return
                return;
            }

            MetaDataManager mmgr = storeMgr.getMetaDataManager();
            StatementMappingIndex stmtMapping = new StatementMappingIndex(m);
            if (m.getNumberOfDatastoreMappings() > 0)
            {
                // Select of fields with columns in source table(s)
                // Adds inner/outer join to any required superclass/secondary tables
                SQLTable sqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, sourceSqlTbl, m);

                boolean selectFK = true;
                if (selectSubobjects && (relationType == RelationType.ONE_TO_ONE_UNI ||
                    (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() == null)) && !mmd.isSerialized() && !mmd.isEmbedded())
                {
                    // Related object with FK at this side
                    selectFK = selectFetchPlanFieldsOfFKRelatedObject(stmt, mappingDefinition, fetchPlan, sourceSqlTbl, mmd, clr, 
                        maxFetchPlanLimit, m, tableGroupName, stmtMapping, sqlTbl, inputJoinType);
                }
                else if (selectSubobjects && (!mmd.isEmbedded() && !mmd.isSerialized()) && relationType == RelationType.MANY_TO_ONE_BI)
                {
                    AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                    if (mmd.getJoinMetaData() != null || relatedMmds[0].getJoinMetaData() != null)
                    {
                        // N-1 bidirectional join table relation
                        // TODO Add left outer join from {sourceTable}.ID to {joinTable}.ELEM_FK
                        Table joinTable = storeMgr.getTable(relatedMmds[0]);
                        DatastoreElementContainer collTable = (DatastoreElementContainer)joinTable;
                        JavaTypeMapping selectMapping = collTable.getOwnerMapping();
                        SQLTable joinSqlTbl = null;
                        if (stmt.getPrimaryTable().getTable() != joinTable)
                        {
                            // Join to the join table
                            JavaTypeMapping referenceMapping = collTable.getElementMapping();
                            joinSqlTbl = stmt.leftOuterJoin(sourceSqlTbl, sourceSqlTbl.getTable().getIdMapping(), collTable, null, referenceMapping, null, tableGroupName);
                        }
                        else
                        {
                            // Main table of the statement is the join table so no need to join
                            joinSqlTbl = stmt.getPrimaryTable();
                        }

                        // Select the owner mapping of the join table
                        int[] colNumbers = stmt.select(joinSqlTbl, selectMapping, null);
                        stmtMapping.setColumnPositions(colNumbers);
                        // TODO Join to 1 side from join table?
                    }
                    else
                    {
                        // N-1 bidirectional FK relation
                        // Related object with FK at this side, so join/select related object as required
                        selectFK = selectFetchPlanFieldsOfFKRelatedObject(stmt, mappingDefinition, fetchPlan, sourceSqlTbl, mmd, clr, maxFetchPlanLimit, m, tableGroupName, 
                            stmtMapping, sqlTbl, inputJoinType);
                    }
                }
                if (selectFK)
                {
                    int[] colNumbers = stmt.select(sqlTbl, m, null);
                    stmtMapping.setColumnPositions(colNumbers);
                }
            }
            else
            {
                // Select of related objects with FK in other table
                if (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() != null)
                {
                    // 1-1 bidirectional relation with FK in related table
                    AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                    AbstractMemberMetaData relatedMmd = relatedMmds[0];
                    String[] clsNames = null;
                    if (mmd.getType().isInterface())
                    {
                        if (mmd.getFieldTypes() != null && mmd.getFieldTypes().length == 1)
                        {
                            // Use field-type since only one class specified
                            Class fldTypeCls = clr.classForName(mmd.getFieldTypes()[0]);
                            if (fldTypeCls.isInterface())
                            {
                                // User has specified an interface, so find its implementations
                                clsNames = mmgr.getClassesImplementingInterface(mmd.getFieldTypes()[0], clr);
                            }
                            else
                            {
                                // Use user-provided field-type
                                clsNames = new String[] {mmd.getFieldTypes()[0]};
                            }
                        }
                        if (clsNames == null)
                        {
                            clsNames = mmgr.getClassesImplementingInterface(mmd.getTypeName(), clr);
                        }
                    }
                    else
                    {
                        String typeName = mmd.isSingleCollection() ? mmd.getCollection().getElementType() : mmd.getTypeName();
                        clsNames = new String[] { typeName };
                    }

                    DatastoreClass relatedTbl = storeMgr.getDatastoreClass(clsNames[0], clr);
                    JavaTypeMapping relatedMapping = relatedTbl.getMemberMapping(relatedMmd);
                    JavaTypeMapping relatedDiscrimMapping = relatedTbl.getDiscriminatorMapping(true);
                    Object[] discrimValues = null;
                    JavaTypeMapping relatedTypeMapping = null;
                    AbstractClassMetaData relatedCmd = relatedMmd.getAbstractClassMetaData();
                    if (relatedDiscrimMapping != null && (relatedCmd.getSuperAbstractClassMetaData() != null || !relatedCmd.getFullClassName().equals(mmd.getTypeName())))
                    {
                        // Related table has a discriminator and the field can store other types
                        List discValueList = null;
                        for (int i=0;i<clsNames.length;i++)
                        {
                            List values = getDiscriminatorValuesForMember(clsNames[i], relatedDiscrimMapping, storeMgr, clr);
                            if (discValueList == null)
                            {
                                discValueList = values;
                            }
                            else
                            {
                                discValueList.addAll(values);
                            }
                        }
                        if (discValueList != null)
                        {
                            discrimValues = discValueList.toArray(new Object[discValueList.size()]);
                        }
                    }
                    else if (relatedTbl != relatedMapping.getTable())
                    {
                        // The relation is to a base class table, and the type stored is a sub-class
                        relatedTypeMapping = relatedTbl.getIdMapping();
                    }

                    SQLTable relatedSqlTbl = null;
                    if (relatedTypeMapping == null)
                    {
                        // Join the 1-1 relation
                        JoinType joinType = getJoinTypeForOneToOneRelationJoin(sourceSqlTbl.getTable().getIdMapping(), sourceSqlTbl, inputJoinType);
                        if (joinType == JoinType.LEFT_OUTER_JOIN || joinType == JoinType.RIGHT_OUTER_JOIN)
                        {
                            inputJoinType = joinType;
                        }
                        relatedSqlTbl = addJoinForOneToOneRelation(stmt, sourceSqlTbl.getTable().getIdMapping(), sourceSqlTbl,
                            relatedMapping, relatedTbl, null, discrimValues, tableGroupName, joinType);

                        // Select the id mapping in the related table
                        int[] colNumbers = stmt.select(relatedSqlTbl, relatedTbl.getIdMapping(), null);
                        stmtMapping.setColumnPositions(colNumbers);
                    }
                    else
                    {
                        DatastoreClass relationTbl = (DatastoreClass)relatedMapping.getTable();
                        if (relatedTbl != relatedMapping.getTable())
                        {
                            if (relatedMapping.isNullable())
                            {
                                // Nullable - left outer join from {sourceTable}.ID to {relatedBaseTable}.FK
                                // and inner join from {relatedBaseTable}.ID to {relatedTable}.ID
                                // (joins the relation and restricts to the right type)
                                relatedSqlTbl = stmt.leftOuterJoin(sourceSqlTbl, sourceSqlTbl.getTable().getIdMapping(),
                                    relatedMapping.getTable(), null, relatedMapping, null, tableGroupName);
                                relatedSqlTbl = stmt.innerJoin(relatedSqlTbl, relatedMapping.getTable().getIdMapping(),
                                    relatedTbl, null, relatedTbl.getIdMapping(), null, tableGroupName);
                            }
                            else
                            {
                                // Not nullable - inner join from {sourceTable}.ID to {relatedBaseTable}.FK
                                // and inner join from {relatedBaseTable}.ID to {relatedTable}.ID
                                // (joins the relation and restricts to the right type)
                                relatedSqlTbl = stmt.innerJoin(sourceSqlTbl, sourceSqlTbl.getTable().getIdMapping(),
                                    relatedMapping.getTable(), null, relatedMapping, null, tableGroupName);
                                relatedSqlTbl = stmt.innerJoin(relatedSqlTbl, relatedMapping.getTable().getIdMapping(),
                                    relatedTbl, null, relatedTbl.getIdMapping(), null, tableGroupName);
                            }
                        }
                        else
                        {
                            // Join the 1-1 relation
                            JoinType joinType = getJoinTypeForOneToOneRelationJoin(sourceSqlTbl.getTable().getIdMapping(), sourceSqlTbl, inputJoinType);
                            if (joinType == JoinType.LEFT_OUTER_JOIN || joinType == JoinType.RIGHT_OUTER_JOIN)
                            {
                                inputJoinType = joinType;
                            }
                            relatedSqlTbl = addJoinForOneToOneRelation(stmt, sourceSqlTbl.getTable().getIdMapping(), sourceSqlTbl,
                                relatedMapping, relationTbl, null, null, tableGroupName, joinType);
                        }

                        // Select the id mapping in the subclass of the related table
                        // Note this adds an inner join from relatedTable to its subclass
                        relatedSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, relatedSqlTbl, relatedTbl.getIdMapping());
                        int[] colNumbers = stmt.select(relatedSqlTbl, relatedTbl.getIdMapping(), null);
                        stmtMapping.setColumnPositions(colNumbers);
                    }

                    if (selectSubobjects && !mmd.isSerialized() && !mmd.isEmbedded())
                    {
                        // Select the fetch-plan fields of the related object
                        StatementClassMapping subMappingDefinition = new StatementClassMapping(null, mmd.getName());
                        selectFetchPlanOfSourceClassInStatement(stmt, subMappingDefinition, fetchPlan, relatedSqlTbl, relatedMmd.getAbstractClassMetaData(), maxFetchPlanLimit-1, inputJoinType);
                        if (mappingDefinition != null)
                        {
                            mappingDefinition.addMappingDefinitionForMember(mmd.getAbsoluteFieldNumber(), subMappingDefinition);
                        }
                    }
                }
                else if (relationType == RelationType.MANY_TO_ONE_BI)
                {
                    AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                    if (mmd.getJoinMetaData() != null || relatedMmds[0].getJoinMetaData() != null)
                    {
                        // N-1 bidirectional join table relation
                        // Add left outer join from {sourceTable}.ID to {joinTable}.ELEM_FK
                        Table joinTable = storeMgr.getTable(relatedMmds[0]);
                        DatastoreElementContainer collTable = (DatastoreElementContainer)joinTable;
                        JavaTypeMapping selectMapping = collTable.getOwnerMapping();
                        SQLTable joinSqlTbl = null;
                        if (stmt.getPrimaryTable().getTable() != joinTable)
                        {
                            // Join to the join table
                            JavaTypeMapping referenceMapping = collTable.getElementMapping();
                            if (referenceMapping instanceof ReferenceMapping)
                            {
                                // Join table has a reference mapping pointing to our table, so get the submapping for the implementation
                                ReferenceMapping refMap = (ReferenceMapping)referenceMapping;
                                Class implType = clr.classForName(mmd.getClassName(true));
                                referenceMapping = refMap.getJavaTypeMappingForType(implType);
                            }
                            joinSqlTbl = stmt.leftOuterJoin(sourceSqlTbl, sourceSqlTbl.getTable().getIdMapping(),
                                collTable, null, referenceMapping, null, tableGroupName + "_JOIN");
                        }
                        else
                        {
                            // Main table of the statement is the join table so no need to join
                            joinSqlTbl = stmt.getPrimaryTable();
                        }

                        // Select the owner mapping of the join table
                        int[] colNumbers = stmt.select(joinSqlTbl, selectMapping, null);
                        stmtMapping.setColumnPositions(colNumbers);
                    }
                    // TODO Select fetch plan fields of this related object
                }
                else if (relationType == RelationType.MANY_TO_ONE_UNI)
                {
                    // Add left outer join from {sourceTable}.ID to {joinTable}.OWNER_FK
                    PersistableJoinTable joinTable = (PersistableJoinTable) storeMgr.getTable(mmd);
                    SQLTable joinSqlTbl = stmt.leftOuterJoin(sourceSqlTbl, sourceSqlTbl.getTable().getIdMapping(),
                        joinTable, null, joinTable.getOwnerMapping(), null, tableGroupName + "_JOIN");

                    int[] colNumbers = stmt.select(joinSqlTbl, joinTable.getRelatedMapping(), null);
                    stmtMapping.setColumnPositions(colNumbers);
                    // TODO Select fetch plan fields of this related object
                }
            }
            if (mappingDefinition != null)
            {
                mappingDefinition.addMappingForMember(mmd.getAbsoluteFieldNumber(), stmtMapping);
            }
        }
    }

    /**
     * Convenience method to join to and select all required FP fields of a related object where linked via an FK at this side.
     * @return Whether the caller should select the FK themselves (i.e we haven't selected anything)
     */
    private static boolean selectFetchPlanFieldsOfFKRelatedObject(SelectStatement stmt, StatementClassMapping mappingDefinition, FetchPlan fetchPlan,
            SQLTable sourceSqlTbl, AbstractMemberMetaData mmd, ClassLoaderResolver clr, int maxFetchPlanLimit, JavaTypeMapping m, String tableGroupName,
            StatementMappingIndex stmtMapping, SQLTable sqlTbl, JoinType inputJoinType)
    {
        boolean selectFK = true;
        if (mmd.fetchFKOnly())
        {
            // Only want FK fetching, and not the fields of the object (so avoid the join)
        }
        else
        {
            RDBMSStoreManager storeMgr = stmt.getRDBMSManager();

            Class type = mmd.isSingleCollection() ? clr.classForName(mmd.getCollection().getElementType()) : mmd.getType(); 
            
            // select fetch plan fields of this object
            AbstractClassMetaData relatedCmd = storeMgr.getMetaDataManager().getMetaDataForClass(type, clr);
            if (relatedCmd != null)
            {
                if (relatedCmd.isEmbeddedOnly())
                {
                    return true;
                }

                // Find the table of the related class
                DatastoreClass relatedTbl = storeMgr.getDatastoreClass(relatedCmd.getFullClassName(), clr);
                if (relatedTbl == null)
                {
                    // Class doesn't have its own table (subclass-table) so find where it persists
                    AbstractClassMetaData[] ownerParentCmds = storeMgr.getClassesManagingTableForClass(relatedCmd, clr);
                    if (ownerParentCmds.length > 1)
                    {
                        NucleusLogger.QUERY.info("Relation (" + mmd.getFullFieldName() + ") with multiple related tables (using subclass-table). Not supported so selecting FK of related object only");
                        return true;
                    }

                    relatedTbl = storeMgr.getDatastoreClass(ownerParentCmds[0].getFullClassName(), clr);
                }

                String requiredGroupName = null;
                if (sourceSqlTbl.getGroupName() != null)
                {
                    // JPQL will have table groups defined already, named as per "alias.fieldName"
                    requiredGroupName = sourceSqlTbl.getGroupName() + "." + mmd.getName();
                }
                SQLTable relatedSqlTbl = stmt.getTable(relatedTbl, requiredGroupName);
                if (relatedSqlTbl == null)
                {
                    // Join the 1-1 relation
                    JoinType joinType = getJoinTypeForOneToOneRelationJoin(m, sqlTbl, inputJoinType);
                    if (joinType == JoinType.LEFT_OUTER_JOIN || joinType == JoinType.RIGHT_OUTER_JOIN)
                    {
                        inputJoinType = joinType;
                    }
                    relatedSqlTbl = addJoinForOneToOneRelation(stmt, m, sqlTbl, relatedTbl.getIdMapping(), relatedTbl, null, null, tableGroupName, joinType);
                }

                StatementClassMapping subMappingDefinition =
                    new StatementClassMapping(mmd.getClassName(), mmd.getName());
                selectFetchPlanOfSourceClassInStatement(stmt, subMappingDefinition, fetchPlan, relatedSqlTbl, relatedCmd, maxFetchPlanLimit-1, inputJoinType);
                if (mappingDefinition != null)
                {
                    if (relatedCmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        int[] pkFields = relatedCmd.getPKMemberPositions();
                        int[] pkCols = new int[m.getNumberOfDatastoreMappings()];
                        int pkColNo = 0;
                        for (int i=0;i<pkFields.length;i++)
                        {
                            StatementMappingIndex pkIdx = subMappingDefinition.getMappingForMemberPosition(pkFields[i]);
                            int[] pkColNumbers = pkIdx.getColumnPositions();
                            for (int j=0;j<pkColNumbers.length;j++)
                            {
                                pkCols[pkColNo] = pkColNumbers[j];
                                pkColNo++;
                            }
                        }
                        selectFK = false;
                        stmtMapping.setColumnPositions(pkCols);
                    }
                    else if (relatedCmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        StatementMappingIndex pkIdx = subMappingDefinition.getMappingForMemberPosition(StatementClassMapping.MEMBER_DATASTORE_ID);
                        selectFK = false;
                        stmtMapping.setColumnPositions(pkIdx.getColumnPositions());
                    }
                    mappingDefinition.addMappingDefinitionForMember(mmd.getAbsoluteFieldNumber(), subMappingDefinition);
                }
            }
            else
            {
                // TODO 1-1 interface relation
            }
        }
        return selectFK;
    }

    /**
     * Convenience method to add a join across a 1-1 relation to the provided SQLStatement.
     * Returns the SQLTable for the other side of the relation.
     * If the join type isn't provided and ID is in the target and FK in the source and the FK is 
     * not nullable then an "inner join" is used, otherwise a "left outer join" is used to cater 
     * for null values. If the join type is supplied then it is respected.
     * @param stmt The SQLStatement
     * @param sourceMapping Mapping of the relation in the source table
     * @param sourceSqlTbl Source table in the SQLStatement
     * @param targetMapping Mapping of the relation in the target table
     * @param targetTable Target table in the datastore
     * @param targetAlias Alias for target table to use in SQLStatement
     * @param discrimValues Any discriminator values to apply to restrict the target side (if any)
     * @param targetTablegroupName Name of the tablegroup that the target SQLTable should be in (if known)
     * @param joinType Type of join to use (if known). If not known will use based on whether nullable
     * @return The SQLTable for the target once the join is added
     */
    public static SQLTable addJoinForOneToOneRelation(SQLStatement stmt,
            JavaTypeMapping sourceMapping, SQLTable sourceSqlTbl,
            JavaTypeMapping targetMapping, Table targetTable, String targetAlias,
            Object[] discrimValues, String targetTablegroupName, JoinType joinType)
    {
        if (joinType == null)
        {
            // Fallback to nullability logic since no type specified
            joinType = getJoinTypeForOneToOneRelationJoin(sourceMapping, sourceSqlTbl, joinType);
        }

        SQLTable targetSqlTbl = null;
        if (joinType == JoinType.LEFT_OUTER_JOIN)
        {
            // left outer join from {sourceTable}.{key} to {relatedTable}.{key}
            targetSqlTbl = stmt.leftOuterJoin(sourceSqlTbl, sourceMapping, targetTable, targetAlias, targetMapping, discrimValues, targetTablegroupName);
        }
        else if (joinType == JoinType.INNER_JOIN)
        {
            // inner join from {sourceTable}.{key} to {relatedTable}.{key}
            targetSqlTbl = stmt.innerJoin(sourceSqlTbl, sourceMapping, targetTable, targetAlias, targetMapping, discrimValues, targetTablegroupName);
        }
        else if (joinType == JoinType.RIGHT_OUTER_JOIN)
        {
            // right outer join from {sourceTable}.{key} to {relatedTable}.{key}
            targetSqlTbl = stmt.rightOuterJoin(sourceSqlTbl, sourceMapping, targetTable, targetAlias, targetMapping, discrimValues, targetTablegroupName);
        }
        else if (joinType == JoinType.CROSS_JOIN)
        {
            // cross join to {relatedTable}.{key}
            targetSqlTbl = stmt.crossJoin(targetTable, targetAlias, targetTablegroupName);
        }

        return targetSqlTbl;
    }

    /**
     * Convenience method to return the join type to use for the specified 1-1 relation.
     * If the join type isn't provided and ID is in the target and FK in the source and the FK is 
     * not nullable then an "inner join" is used, otherwise a "left outer join" is used to cater for null values. 
     * @param sourceMapping Mapping of the relation in the source table
     * @param sourceSqlTbl Source table in the SQLStatement
     * @param joinType Join type to use if already known; will be returned if not null
     * @return Join type that should be used for this 1-1 relation
     */
    public static JoinType getJoinTypeForOneToOneRelationJoin(JavaTypeMapping sourceMapping, SQLTable sourceSqlTbl, JoinType joinType)
    {
        if (joinType == null)
        {
            joinType = JoinType.LEFT_OUTER_JOIN; // Default to LEFT OUTER join
            if (sourceMapping != sourceSqlTbl.getTable().getIdMapping())
            {
                // ID in target, FK in source, so check for not nullable in source (what we are selecting)
                // If nullable then LEFT OUTER join, otherwise INNER join
                joinType = (sourceMapping.isNullable() ? JoinType.LEFT_OUTER_JOIN : JoinType.INNER_JOIN);
            }
        }
        return joinType;
    }

    /**
     * Convenience method to generate a BooleanExpression for the associated discriminator value for
     * the specified class.
     * @param stmt The Query Statement to be updated
     * @param className The class name
     * @param dismd MetaData for the discriminator
     * @param discriminatorMapping Mapping for the discriminator
     * @param discrimSqlTbl SQLTable for the table with the discriminator
     * @param clr ClassLoader resolver
     * @return Boolean expression for this discriminator value
     */
    public static BooleanExpression getExpressionForDiscriminatorForClass(SQLStatement stmt, String className, DiscriminatorMetaData dismd, JavaTypeMapping discriminatorMapping,
            SQLTable discrimSqlTbl, ClassLoaderResolver clr)
    {
        Object discriminatorValue = className; // Default to the "class-name" discriminator strategy
        if (dismd.getStrategy() == DiscriminatorStrategy.VALUE_MAP)
        {
            // Get the MetaData for the target class since that holds the "value"
            NucleusContext nucleusCtx = stmt.getRDBMSManager().getNucleusContext();
            AbstractClassMetaData targetCmd = nucleusCtx.getMetaDataManager().getMetaDataForClass(className, clr);
            String strValue = null;
            if (targetCmd.getInheritanceMetaData() != null && targetCmd.getInheritanceMetaData().getDiscriminatorMetaData() != null)
            {
                strValue = targetCmd.getInheritanceMetaData().getDiscriminatorMetaData().getValue();
            }
            if (strValue == null)
            {
                // No value defined for this clause of the map
                strValue = className;
            }
            if (discriminatorMapping instanceof DiscriminatorLongMapping)
            {
                try
                {
                    discriminatorValue = Integer.valueOf(strValue);
                }
                catch (NumberFormatException nfe)
                {
                    throw new NucleusUserException("Discriminator for " + className + " is not integer-based but needs to be!");
                }
            }
            else
            {
                discriminatorValue = strValue;
            }
        }

        SQLExpression discrExpr = stmt.getSQLExpressionFactory().newExpression(stmt, discrimSqlTbl, discriminatorMapping);
        SQLExpression discrVal = stmt.getSQLExpressionFactory().newLiteral(stmt, discriminatorMapping, discriminatorValue);
        return discrExpr.eq(discrVal);
    }

    /**
     * Method to return all possible discriminator values for the supplied class and its subclasses.
     * @param className Name of the class
     * @param discMapping The discriminator mapping
     * @param storeMgr StoreManager
     * @param clr ClassLoader resolver
     * @return The possible discriminator values
     */
    public static List getDiscriminatorValuesForMember(String className, JavaTypeMapping discMapping, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        List discrimValues = new ArrayList();
        DiscriminatorStrategy strategy = discMapping.getTable().getDiscriminatorMetaData().getStrategy();
        if (strategy == DiscriminatorStrategy.CLASS_NAME)
        {
            discrimValues.add(className);
            Collection<String> subclasses = storeMgr.getSubClassesForClass(className, true, clr);
            if (subclasses != null && subclasses.size() > 0)
            {
                discrimValues.addAll(subclasses);
            }
        }
        else if (strategy == DiscriminatorStrategy.VALUE_MAP)
        {
            MetaDataManager mmgr = storeMgr.getMetaDataManager();
            AbstractClassMetaData cmd = mmgr.getMetaDataForClass(className, clr);
            Collection<String> subclasses = storeMgr.getSubClassesForClass(className, true, clr);
            discrimValues.add(cmd.getInheritanceMetaData().getDiscriminatorMetaData().getValue());
            if (subclasses != null && subclasses.size() > 0)
            {
                Iterator<String> subclassesIter = subclasses.iterator();
                while (subclassesIter.hasNext())
                {
                    String subclassName = subclassesIter.next();
                    AbstractClassMetaData subclassCmd = mmgr.getMetaDataForClass(subclassName, clr);
                    discrimValues.add(subclassCmd.getInheritanceMetaData().getDiscriminatorMetaData().getValue());
                }
            }
        }

        return discrimValues;
    }
}
