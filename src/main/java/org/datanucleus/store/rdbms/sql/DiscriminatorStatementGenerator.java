/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved. 
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

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.InExpression;
import org.datanucleus.store.rdbms.sql.expression.NullLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.table.SurrogateColumnType;

/**
 * Class to generate a SelectStatement for iterating through instances of a particular type (and 
 * optionally subclasses). Based around the candidate type having subclasses the candidate has a 
 * discriminator column, and so can be used as the way of determining the type of the object.
 * Also allows select of the discriminator column to allow retrieval of the type of the object.
 * Please refer to the specific constructors for the usages.
 * Note that the statement returned by getStatement() has nothing selected, solely providing the
 * basic structure. Column selection should be performed when generating the mapping definition.
 * <h3>Supported options</h3>
 * This generator supports the following "options" :-
 * <ul>
 * <li><b>restrictDiscriminator</b> : Whether to add a WHERE clause to restrict the discriminator to only valid values (default="true")</li>
 * <li><b>allowNulls</b> : whether we allow for null objects (only happens when we have a join table collection.</li>
 * </ul>
 */
public class DiscriminatorStatementGenerator extends AbstractSelectStatementGenerator
{
    Class[] candidates = null;

    /**
     * Constructor, using the candidateTable as the primary table of the SQL SELECT.
     * Let's assume that we have class A which is the candidate and this has subclasses A1, A2
     * that are stored in the same table (with different values of the discriminator), and has subclasses
     * A3, A4 of A2 that are stored in a different table.
     * We want to find all objects of the candidate type and optionally its subclasses 
     * and we want information about what type the object is (A or A1 or A2 or A3 or A4). 
     * The query will be of the form
     * <PRE>
     * SELECT 
     * FROM A T0
     * [WHERE (T0.DISCRIMINATOR = A0value || T0.DISCRIMINATOR = A1value || 
     *         T0.DISCRIMINATOR = A2value || T0.DISCRIMINATOR = A3value || 
     *         T0.DISCRIMINATOR = A4value)]
     * </PRE>
     * The "candidateType" will provide the primary table of the SQL SELECT.
     * @param storeMgr Manager for the datastore
     * @param clr ClassLoader resolver
     * @param candidateType Base object type that we are looking for
     * @param includeSubclasses Should we include subclasses of this candidate?
     * @param candidateTableAlias Alias to use for the candidate table (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     */
    public DiscriminatorStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr,
            Class candidateType, boolean includeSubclasses, 
            DatastoreIdentifier candidateTableAlias, String candidateTableGroupName)
    {
        super(storeMgr, clr, candidateType, includeSubclasses, candidateTableAlias, candidateTableGroupName);

        setOption(OPTION_RESTRICT_DISCRIM);
    }

    /**
     * Constructor, using the candidateTable as the primary table of the SQL SELECT.
     * Differs from the other similar constructor in that it provides multiple candidates.
     * The first of these candidates should always be considered the primary.
     * @param storeMgr Manager for the datastore
     * @param clr ClassLoader resolver
     * @param candidateTypes Base object type(s) that we are looking for
     * @param includeSubclasses Should we include subclasses of this candidate(s)?
     * @param candidateTableAlias Alias to use for the candidate table (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     */
    public DiscriminatorStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr,
            Class[] candidateTypes, boolean includeSubclasses,
            DatastoreIdentifier candidateTableAlias, String candidateTableGroupName)
    {
        this(storeMgr, clr, candidateTypes[0], includeSubclasses, candidateTableAlias, candidateTableGroupName);
        this.candidates = candidateTypes;
    }

    /**
     * Constructor, using the joinTable as the primary table of the SQL SELECT and joining to the
     * table of the candidate.
     * Let's assume that we have class A with collection with elements of type B (join table A_B) 
     * and candidate B has subclasses B1, B2 that are stored in the same table (with different values 
     * of the discriminator), and has subclasses B3, B4 of B2 that are stored in a different table.
     * We want to find all objects of the candidate type and optionally its subclasses 
     * and we want information about what type the object is (B or B1 or B2 or B3 or B4). 
     * The query will be of the form
     * <PRE>
     * SELECT 
     * FROM A_B T0
     *   INNER JOIN B T1 ON T1.B_ID = A_B.B_ID_EID
     * [WHERE (T1.DISCRIMINATOR = B0value || T1.DISCRIMINATOR = B1value || 
     *         T1.DISCRIMINATOR = B2value || T1.DISCRIMINATOR = B3value || 
     *         T1.DISCRIMINATOR = B4value)]
     * </PRE>
     * The join table will provide the primary table of the SQL SELECT.
     * The join is INNER JOIN when no nulls are allowed in the collection, and LEFT OUTER JOIN
     * when it allows nulls.
     * @param storeMgr Manager for the datastore
     * @param clr ClassLoader resolver
     * @param candidateType Base object type(s) that we are looking for
     * @param includeSubclasses Should we include subclasses of this candidate?
     * @param candidateTableAlias Alias to use for the candidate table (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     * @param joinTable Join table linking owner to elements
     * @param joinTableAlias any alias to use for the join table in the SQL
     * @param joinElementMapping Mapping in the join table to link to the element
     */
    public DiscriminatorStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr,
            Class candidateType, boolean includeSubclasses,
            DatastoreIdentifier candidateTableAlias, String candidateTableGroupName,
            Table joinTable, DatastoreIdentifier joinTableAlias, 
            JavaTypeMapping joinElementMapping)
    {
        super(storeMgr, clr, candidateType, includeSubclasses, candidateTableAlias, candidateTableGroupName,
            joinTable, joinTableAlias, joinElementMapping);

        setOption(OPTION_RESTRICT_DISCRIM);
    }

    /**
     * Constructor, using the joinTable as the primary table of the SQL SELECT and joining to the
     * table of the candidate(s). Same as the other join table constructor except allows multiple candidates.
     * @param storeMgr Manager for the datastore
     * @param clr ClassLoader resolver
     * @param candidateTypes Base object type(s) that we are looking for
     * @param includeSubclasses Should we include subclasses of this candidate?
     * @param candidateTableAlias Alias to use for the candidate table (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     * @param joinTable Join table linking owner to elements
     * @param joinTableAlias any alias to use for the join table in the SQL
     * @param joinElementMapping Mapping in the join table to link to the element
     */
    public DiscriminatorStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr,
            Class[] candidateTypes, boolean includeSubclasses,
            DatastoreIdentifier candidateTableAlias, String candidateTableGroupName,
            Table joinTable, DatastoreIdentifier joinTableAlias, 
            JavaTypeMapping joinElementMapping)
    {
        this(storeMgr, clr, candidateTypes[0], includeSubclasses, candidateTableAlias, candidateTableGroupName,
            joinTable, joinTableAlias, joinElementMapping);
        candidates = candidateTypes;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.StatementGenerator#setParentStatement(org.datanucleus.store.rdbms.sql.SQLStatement)
     */
    public void setParentStatement(SQLStatement stmt)
    {
        this.parentStmt = stmt;
    }

    /**
     * Accessor for the SelectStatement.
     * @param ec ExecutionContext
     * @return The SelectStatement for iterating through objects with a discriminator column
     */
    public SelectStatement getStatement(ExecutionContext ec)
    {
        SelectStatement stmt = null;
        SQLTable discrimSqlTbl = null;
        if (joinTable == null)
        {
            // Select of candidate table
            stmt = new SelectStatement(parentStmt, storeMgr, candidateTable, candidateTableAlias, candidateTableGroupName);
            stmt.setClassLoaderResolver(clr);
            discrimSqlTbl = stmt.getPrimaryTable();
        }
        else
        {
            // Select of join table, with join to element table
            stmt = new SelectStatement(parentStmt, storeMgr, joinTable, joinTableAlias, candidateTableGroupName);
            stmt.setClassLoaderResolver(clr);

            JavaTypeMapping candidateIdMapping = candidateTable.getIdMapping();
            if (hasOption(OPTION_ALLOW_NULLS))
            {
                // Put element table in same table group since all relates to the elements
                discrimSqlTbl = stmt.join(JoinType.LEFT_OUTER_JOIN, null, joinElementMapping, candidateTable, null, candidateIdMapping, null, 
                    stmt.getPrimaryTable().getGroupName(), true);
            }
            else
            {
                // Put element table in same table group since all relates to the elements
                discrimSqlTbl = stmt.join(JoinType.INNER_JOIN, null, joinElementMapping, candidateTable, null, candidateIdMapping, null, 
                    stmt.getPrimaryTable().getGroupName(), true);
            }
        }

        JavaTypeMapping discMapping = candidateTable.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, true);
        if (discMapping != null)
        {
            // Allow for discriminator being in super-table of the candidate table
            discrimSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, discrimSqlTbl, discMapping);
        }

        DiscriminatorMetaData dismd = discrimSqlTbl.getTable().getDiscriminatorMetaData();
        boolean hasDiscriminator = (discMapping != null && dismd != null && dismd.getStrategy() != DiscriminatorStrategy.NONE);

        // Check if we can omit the discriminator restriction
        boolean restrictDiscriminator = hasOption(OPTION_RESTRICT_DISCRIM);
        /*if (includeSubclasses && hasDiscriminator && candidateTable.getDiscriminatorMapping(false) != null &&
            !storeMgr.getNucleusContext().getMetaDataManager().isPersistentDefinitionImplementation(candidateType.getName()))
        {
            String[] managedClasses = ((ClassTable)candidateTable).getManagedClasses();
            if (managedClasses.length == 1)
            {
                // Only the candidate managed by this table and the discrim is here and we're including subclasses
                // in the SELECT so don't apply any restriction on the discriminator value
                // Note : we omit the persistent interface impl case from here for now
            //    restrictDiscriminator = false;
            }
        }*/

        if (hasDiscriminator && restrictDiscriminator)
        {
            // Add the discriminator expression to restrict accepted values
            boolean multipleCandidates = false;
            BooleanExpression discExpr = null;
            if (candidates != null)
            {
                // Multiple candidates
                if (candidates.length > 1)
                {
                    multipleCandidates = true;
                }
                for (int i=0;i<candidates.length;i++)
                {
                    if (Modifier.isAbstract(candidates[i].getModifiers()))
                    {
                        // No point selecting this candidate since can't be instantiated
                        continue;
                    }

                    BooleanExpression discExprCandidate = SQLStatementHelper.getExpressionForDiscriminatorForClass(stmt, candidates[i].getName(), dismd, discMapping, discrimSqlTbl, clr);
                    if (discExpr != null)
                    {
                        discExpr = discExpr.ior(discExprCandidate);
                    }
                    else
                    {
                        discExpr = discExprCandidate;
                    }
                    if (includeSubclasses)
                    {
                        Collection<String> subclassNames = storeMgr.getSubClassesForClass(candidateType.getName(), true, clr);
                        Iterator<String> subclassIter = subclassNames.iterator();
                        if (!multipleCandidates)
                        {
                            multipleCandidates = (subclassNames.size() > 0);
                        }
                        while (subclassIter.hasNext())
                        {
                            String subclassName = subclassIter.next();
                            BooleanExpression discExprSub = SQLStatementHelper.getExpressionForDiscriminatorForClass(stmt, subclassName, dismd, discMapping, discrimSqlTbl, clr);
                            discExpr = discExpr.ior(discExprSub);
                        }
                    }
                }
            }
            else
            {
                // Single candidate
                if (!Modifier.isAbstract(candidateType.getModifiers()))
                {
                    discExpr = SQLStatementHelper.getExpressionForDiscriminatorForClass(stmt, candidateType.getName(), dismd, discMapping, discrimSqlTbl, clr);
                }
                if (includeSubclasses)
                {
                    Collection<String> subclassNames = storeMgr.getSubClassesForClass(candidateType.getName(), true, clr);
                    Iterator<String> subclassIter = subclassNames.iterator();
                    multipleCandidates = (subclassNames.size() > 0);
                    while (subclassIter.hasNext())
                    {
                        String subclassName = subclassIter.next();
                        Class subclass = clr.classForName(subclassName);
                        if ((Modifier.isAbstract(subclass.getModifiers())))
                        {
                            continue;
                        }

                        BooleanExpression discExprCandidate = SQLStatementHelper.getExpressionForDiscriminatorForClass(stmt, subclassName, dismd, discMapping, discrimSqlTbl, clr);
                        if (discExpr == null)
                        {
                            discExpr = discExprCandidate;
                        }
                        else
                        {
                            discExpr = discExpr.ior(discExprCandidate);
                        }
                    }
                }
                if (discExpr == null)
                {
                    // No possible candidates, so set expression as "1=0"
                    SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
                    JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
                    discExpr = exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
                }
            }

            if (discExpr != null)
            {
                if (hasOption(OPTION_ALLOW_NULLS))
                {
                    // Allow for null value of discriminator
                    SQLExpression expr = stmt.getSQLExpressionFactory().newExpression(stmt, discrimSqlTbl, discMapping);
                    SQLExpression val = new NullLiteral(stmt, null, null, null);
                    BooleanExpression nullDiscExpr = expr.eq(val);
                    discExpr = discExpr.ior(nullDiscExpr);
                    if (!multipleCandidates)
                    {
                        multipleCandidates = true;
                    }
                }

                // Apply the discriminator to the query statement
                if (multipleCandidates)
                {
                    discExpr.encloseInParentheses();
                }
                stmt.whereAnd(discExpr, true);
            }
        }

        JavaTypeMapping multitenancyMapping = candidateTable.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false);
        if (multitenancyMapping != null)
        {
            // Multi-tenancy restriction
            SQLTable tenantSqlTbl = stmt.getTable(multitenancyMapping.getTable(), discrimSqlTbl.getGroupName());
            SQLExpression tenantExpr = stmt.getSQLExpressionFactory().newExpression(stmt, tenantSqlTbl, multitenancyMapping);
            String[] tenantReadIds = ec.getNucleusContext().getMultiTenancyReadIds(ec);
            if (tenantReadIds != null && tenantReadIds.length > 1)
            {
                SQLExpression[] readIdExprs = new SQLExpression[tenantReadIds.length];
                for (int i=0;i<tenantReadIds.length;i++)
                {
                    readIdExprs[i] = stmt.getSQLExpressionFactory().newLiteral(stmt, multitenancyMapping, tenantReadIds[i].trim());
                }
                stmt.whereAnd(new InExpression(tenantExpr, readIdExprs), true);
            }
            else
            {
                SQLExpression tenantVal = stmt.getSQLExpressionFactory().newLiteral(stmt, multitenancyMapping, ec.getNucleusContext().getMultiTenancyId(ec));
                stmt.whereAnd(tenantExpr.eq(tenantVal), true);
            }
        }

        JavaTypeMapping softDeleteMapping = candidateTable.getSurrogateMapping(SurrogateColumnType.SOFTDELETE, false);
        if (softDeleteMapping != null && !hasOption(OPTION_INCLUDE_SOFT_DELETES))
        {
            // Soft-delete restriction
            SQLTable softDeleteSqlTbl = stmt.getTable(softDeleteMapping.getTable(), discrimSqlTbl.getGroupName());
            SQLExpression softDeleteExpr = stmt.getSQLExpressionFactory().newExpression(stmt, softDeleteSqlTbl, softDeleteMapping);
            SQLExpression softDeleteVal = stmt.getSQLExpressionFactory().newLiteral(stmt, softDeleteMapping, Boolean.FALSE);
            stmt.whereAnd(softDeleteExpr.eq(softDeleteVal), true);
        }

        return stmt;
    }
}