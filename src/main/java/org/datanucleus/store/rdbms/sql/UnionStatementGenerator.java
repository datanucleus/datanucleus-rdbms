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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.NullLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Class to generate a SelectStatement for iterating through instances of a particular type (and 
 * optionally subclasses). Based around the candidate type having subclasses and we use UNIONs to
 * return all possible types of candidate. Also allows select of a dummy column to return the
 * type for the part of the UNION that the object came from. Please refer to the specific constructors
 * for the usages.
 * <h3>Supported options</h3>
 * This generator supports
 * <ul>
 * <li><b>selectDnType</b> : adds a SELECT of a dummy column accessible as "DN_TYPE" storing the class name.</li>
 * <li><b>allowNulls</b> : whether we allow for null objects (only happens when we have a join table collection.</li>
 * </ul>
 */
public class UnionStatementGenerator extends AbstractSelectStatementGenerator
{
    /** Name of column added when using "selectDnType" */
    public static final String DN_TYPE_COLUMN = "DN_TYPE";

    /**
     * Constructor using the candidateTable as the primary table.
     * If we are querying objects of type A with subclasses A1, A2 the query will be of the form :-
     * <PRE>
     * SELECT ['mydomain.A' AS DN_TYPE]
     * FROM A THIS
     *   LEFT OUTER JOIN A1 SUBELEMENT0 ON SUBELEMENT0.A1_ID = THIS.A_ID
     *   LEFT OUTER JOIN A1 SUBELEMENT1 ON SUBELEMENT0.A2_ID = THIS.A_ID
     * WHERE SUBELEMENT0.A1_ID IS NULL
     * AND SUBELEMENT0.A2_ID IS NULL
     *
     * UNION 
     *
     * SELECT ['mydomain.A1' AS DN_TYPE] 
     * FROM A THIS
     *   INNER JOIN A1 'ELEMENT' ON 'ELEMENT'.A1_ID = THIS.A_ID
     *
     * UNION
     *
     * SELECT ['mydomain.A2' AS DN_TYPE] 
     * FROM A THIS
     *   INNER JOIN A2 'ELEMENT' ON 'ELEMENT'.A2_ID = THIS.A_ID
     * </PRE>
     * So the first part of the UNION returns the objects just present in the A table, whilst the
     * second part returns those just in table A1, and the third part returns those just in table A2.
     * @param storeMgr the store manager
     * @param clr ClassLoader resolver
     * @param candidateType the candidate that we are looking for
     * @param includeSubclasses if the subclasses of the candidate should be included in the result
     * @param candidateTableAlias Alias to use for the candidate table (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     */
    public UnionStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr,
            Class candidateType, boolean includeSubclasses,
            DatastoreIdentifier candidateTableAlias, String candidateTableGroupName)
    {
        super(storeMgr, clr, candidateType, includeSubclasses, candidateTableAlias, candidateTableGroupName);
    }

    /**
     * Constructor using a join table as the primary table.
     * If we are querying elements (B) of a collection in class A and B has subclasses B1, B2 
     * stored via a join table (A_B) the query will be of the form :-
     * <PRE>
     * SELECT ['mydomain.B' AS DN_TYPE]
     * FROM A_B T0
     *   INNER JOIN B T1 ON T0.B_ID_EID = T1.B_ID
     *   LEFT OUTER JOIN B1 T2 ON T2.B1_ID = T0.B_ID_EID
     *   LEFT OUTER JOIN B2 T3 ON T3.B2_ID = T0.B_ID_EID
     * WHERE T2.B1_ID IS NULL
     * AND T3.B2_ID IS NULL
     *
     * UNION 
     *
     * SELECT ['mydomain.B1' AS DN_TYPE] 
     * FROM A_B THIS
     *   INNER JOIN B T1 ON T1.B_ID = T0.B_ID_EID
     *   INNER JOIN B1 T2 ON T2.B1_ID = T1.B_ID
     *
     * UNION
     *
     * SELECT ['mydomain.A2' AS DN_TYPE] 
     * FROM A_B THIS
     *   INNER JOIN B T1 ON T1.B_ID = T0.B_ID_EID
     *   INNER JOIN B2 T2 ON T2.B2_ID = T1.B_ID
     * </PRE>
     * So the first part of the UNION returns the objects just present in the B table, whilst the
     * second part returns those just in table B1, and the third part returns those just in table B2.
     * When we have a join table collection we MUST select the join table since this then caters for the
     * situation of having null elements (if we had selected the root element table we wouldn't know if 
     * there was a null element in the collection).
     * @param storeMgr the store manager
     * @param clr ClassLoader resolver
     * @param candidateType the candidate that we are looking for
     * @param includeSubclasses if the subclasses of the candidate should be included in the result
     * @param candidateTableAlias Alias to use for the candidate table (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     * @param joinTable Join table linking owner to elements
     * @param joinTableAlias any alias to use for the join table in the SQL
     * @param joinElementMapping Mapping in the join table to link to the element
     */
    public UnionStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr, 
            Class candidateType, boolean includeSubclasses, DatastoreIdentifier candidateTableAlias, String candidateTableGroupName,
            Table joinTable, DatastoreIdentifier joinTableAlias, JavaTypeMapping joinElementMapping)
    {
        super(storeMgr, clr, candidateType, includeSubclasses, candidateTableAlias, candidateTableGroupName, joinTable, joinTableAlias, joinElementMapping);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.StatementGenerator#setParentStatement(org.datanucleus.store.rdbms.sql.SQLStatement)
     */
    public void setParentStatement(SQLStatement stmt)
    {
        // Never used since we can't have a unioned statement as a subquery, can we ?
        this.parentStmt = stmt;
    }

    private int maxClassNameLength = -1;

    /**
     * Accessor for the SelectStatement for the candidate [+ subclasses].
     * @param ec ExecutionContext
     * @return The SelectStatement returning objects with a UNION statement.
     */
    public SelectStatement getStatement(ExecutionContext ec)
    {
        // Find set of possible candidates (including subclasses of subclasses)
        Collection<String> candidateClassNames = new ArrayList<>();
        AbstractClassMetaData acmd = storeMgr.getMetaDataManager().getMetaDataForClass(candidateType, clr);
        candidateClassNames.add(acmd.getFullClassName());
        if (includeSubclasses)
        {
            Collection<String> subclasses = storeMgr.getSubClassesForClass(candidateType.getName(), true, clr);
            candidateClassNames.addAll(subclasses);
        }

        // Eliminate any classes that are not instantiable
        Iterator<String> iter = candidateClassNames.iterator();
        while (iter.hasNext())
        {
            String className = iter.next();
            try
            {
                Class cls = clr.classForName(className);
                if (Modifier.isAbstract(cls.getModifiers()))
                {
                    // Remove since abstract hence not instantiable
                    iter.remove();
                }
            }
            catch (Exception e)
            {
                // Remove since class not found
                iter.remove();
            }
        }

        if (hasOption(OPTION_SELECT_DN_TYPE))
        {
            // Get the length of the longest class name
            iter = candidateClassNames.iterator();
            while (iter.hasNext())
            {
                String className = iter.next();
                if (className.length() > maxClassNameLength)
                {
                    maxClassNameLength = className.length();
                }
            }
        }

        if (candidateClassNames.isEmpty())
        {
            // Either passed invalid classes, or no concrete classes with available tables present!
            throw new NucleusException("Attempt to generate SQL statement using UNIONs for " + 
                candidateType.getName() + " yet there are no concrete classes with their own table available");
        }

        SelectStatement stmt = null;
        for (String candidateClassName : candidateClassNames)
        {
            SelectStatement candidateStmt = null;
            if (joinTable == null)
            {
                // Select of candidate table
                candidateStmt = getSelectStatementForCandidate(candidateClassName, ec);
            }
            else
            {
                // Select of join table and join to element
                candidateStmt = getSQLStatementForCandidateViaJoin(candidateClassName);
            }

            if (candidateStmt != null)
            {
                if (stmt == null)
                {
                    stmt = candidateStmt;
                }
                else
                {
                    stmt.union(candidateStmt);
                }
            }
        }

        return stmt;
    }

    /**
     * Convenience method to return the SelectStatement for a particular class.
     * Returns a SelectStatement with primaryTable of the "candidateTable", and which joins to the table of the class (if different).
     * @param className The class name to generate the statement for
     * @param ec ExecutionContext
     * @return The SelectStatement
     */
    protected SelectStatement getSelectStatementForCandidate(String className, ExecutionContext ec)
    {
        DatastoreClass table = storeMgr.getDatastoreClass(className, clr);
        if (table == null)
        {
            // Subclass-table, so persisted into table(s) of subclasses
            NucleusLogger.GENERAL.info("Generation of statement to retrieve objects of type " + candidateType.getName() +
                (includeSubclasses ? " including subclasses " : "") + " attempted to include " + className + " but this has no table of its own; ignored");
            // TODO Cater for use of single subclass-table
            return null;
        }

        // Start from an SQL SELECT of the candidate table
        SelectStatement stmt = new SelectStatement(parentStmt, storeMgr, candidateTable, candidateTableAlias, candidateTableGroupName);
        stmt.setClassLoaderResolver(clr);
        stmt.setCandidateClassName(className);

        String tblGroupName = stmt.getPrimaryTable().getGroupName();
        if (table != candidateTable)
        {
            // INNER JOIN from the root candidate table to this candidates table
            JavaTypeMapping candidateIdMapping = candidateTable.getIdMapping();
            JavaTypeMapping tableIdMapping = table.getIdMapping();
            SQLTable tableSqlTbl = stmt.join(JoinType.INNER_JOIN, null, candidateIdMapping, table, null, tableIdMapping, null, stmt.getPrimaryTable().getGroupName(), true);
            tblGroupName = tableSqlTbl.getGroupName();
        }

        // Add any discriminator restriction in this table for the specified class
        // Caters for the case where we have more than 1 class stored in this table
        SQLExpressionFactory factory = storeMgr.getSQLExpressionFactory();
        JavaTypeMapping discriminatorMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
        DiscriminatorMetaData discriminatorMetaData = table.getDiscriminatorMetaData();
        if (discriminatorMapping != null && discriminatorMetaData.getStrategy() != DiscriminatorStrategy.NONE)
        {
            // Restrict to valid discriminator values where we have a discriminator specified on this table
            AbstractClassMetaData targetCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(className, clr);
            SQLExpression discExpr = factory.newExpression(stmt, stmt.getPrimaryTable(), discriminatorMapping);
            SQLExpression discValExpr = factory.newLiteral(stmt, discriminatorMapping, targetCmd.getDiscriminatorValue());
            stmt.whereAnd(discExpr.eq(discValExpr), false);
        }

        JavaTypeMapping multitenancyMapping = table.getSurrogateMapping(SurrogateColumnType.MULTITENANCY, false);
        if (multitenancyMapping != null)
        {
            // Multi-tenancy restriction
            AbstractClassMetaData cmd = table.getClassMetaData();
            SQLTable tenantSqlTbl = stmt.getTable(multitenancyMapping.getTable(), tblGroupName);
            SQLExpression tenantExpr = stmt.getSQLExpressionFactory().newExpression(stmt, tenantSqlTbl, multitenancyMapping);
            SQLExpression tenantVal = stmt.getSQLExpressionFactory().newLiteral(stmt, multitenancyMapping, ec.getNucleusContext().getMultiTenancyId(ec, cmd));
            stmt.whereAnd(tenantExpr.eq(tenantVal), true);
        }

        JavaTypeMapping softDeleteMapping = table.getSurrogateMapping(SurrogateColumnType.SOFTDELETE, false);
        if (softDeleteMapping != null && !hasOption(OPTION_INCLUDE_SOFT_DELETES))
        {
            // Soft-delete restriction
            SQLTable softDeleteSqlTbl = stmt.getTable(softDeleteMapping.getTable(), tblGroupName);
            SQLExpression softDeleteExpr = stmt.getSQLExpressionFactory().newExpression(stmt, softDeleteSqlTbl, softDeleteMapping);
            SQLExpression softDeleteVal = stmt.getSQLExpressionFactory().newLiteral(stmt, softDeleteMapping, Boolean.FALSE);
            stmt.whereAnd(softDeleteExpr.eq(softDeleteVal), true);
        }

        // Eliminate any subclasses (catered for in separate UNION statement)
        Iterator<String> subIter = storeMgr.getSubClassesForClass(className, false, clr).iterator();
        while (subIter.hasNext())
        {
            String subclassName = subIter.next();
            DatastoreClass[] subclassTables = null;
            DatastoreClass subclassTable = storeMgr.getDatastoreClass(subclassName, clr);

            if (subclassTable == null)
            {
                AbstractClassMetaData targetSubCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(subclassName, clr);
                AbstractClassMetaData[] targetSubCmds = storeMgr.getClassesManagingTableForClass(targetSubCmd, clr);
                subclassTables = new DatastoreClass[targetSubCmds.length];
                for (int i=0;i<targetSubCmds.length;i++)
                {
                    subclassTables[i] = storeMgr.getDatastoreClass(targetSubCmds[i].getFullClassName(), clr);
                }
            }
            else
            {
                subclassTables = new DatastoreClass[1];
                subclassTables[0] = subclassTable;
            }

            for (int i=0;i<subclassTables.length;i++)
            {
                if (subclassTables[i] != table)
                {
                    // Subclass of our class is stored in different table to the candidate so exclude it
                    // Adds FROM clause of "LEFT OUTER JOIN {subTable} ON ..."
                    // and WHERE clause of "{subTable}.ID = NULL"
                    JavaTypeMapping tableIdMapping = table.getIdMapping();
                    JavaTypeMapping subclassIdMapping = subclassTables[i].getIdMapping();
                    SQLTable sqlTableSubclass = stmt.join(JoinType.LEFT_OUTER_JOIN, null, tableIdMapping, subclassTables[i], null, subclassIdMapping, null, 
                        stmt.getPrimaryTable().getGroupName(), true);
                    SQLExpression subclassIdExpr = factory.newExpression(stmt, sqlTableSubclass, subclassIdMapping);
                    SQLExpression nullExpr = new NullLiteral(stmt, null, null, null);
                    stmt.whereAnd(subclassIdExpr.eq(nullExpr), false);
                }
            }
        }

        if (hasOption(OPTION_SELECT_DN_TYPE))
        {
            // Add SELECT of dummy metadata for this class ("'mydomain.MyClass' AS DN_TYPE")
            addTypeSelectForClass(stmt, className);
        }

        return stmt;
    }

    /**
     * Convenience method to return the SQLStatement for a particular class selecting a join table.
     * Returns a SQLStatement with primaryTable of the "joinTable", and which joins to the table of the class.
     * @param className The class name to generate the statement for
     * @return The SQLStatement
     */
    protected SelectStatement getSQLStatementForCandidateViaJoin(String className)
    {
        DatastoreClass table = storeMgr.getDatastoreClass(className, clr);
        if (table == null)
        {
            // Only support if there is a single table where the class is actually persisted
            // TODO Cater for use of single subclass-table
            throw new NucleusException("We do not currently support a UNION statement for class=" + className + " since it has no table of its own");
        }

        // Start from an SQL SELECT of the join table
        SelectStatement stmt = new SelectStatement(parentStmt, storeMgr, joinTable, joinTableAlias, candidateTableGroupName);
        stmt.setClassLoaderResolver(clr);
        stmt.setCandidateClassName(className);

        // INNER/LEFT OUTER JOIN from the join table to the root candidate table
        // If we allow nulls we do a left outer join here, otherwise an inner join
        SQLTable candidateSQLTable = null;
        if (candidateTable != null)
        {
            // We have a root candidate table, so join to that
            JavaTypeMapping candidateIdMapping = candidateTable.getIdMapping();
            if (hasOption(OPTION_ALLOW_NULLS))
            {
                // Put element table in same table group since all relates to the elements
                candidateSQLTable = stmt.join(JoinType.LEFT_OUTER_JOIN, null, joinElementMapping, candidateTable, null, candidateIdMapping, null, 
                    stmt.getPrimaryTable().getGroupName(), true);
            }
            else
            {
                // Put element table in same table group since all relates to the elements
                candidateSQLTable = stmt.join(JoinType.INNER_JOIN, null, joinElementMapping, candidateTable, null, candidateIdMapping, 
                    null, stmt.getPrimaryTable().getGroupName(), true);
            }

            // Join the root candidate table to this particular candidate table
            if (table != candidateTable)
            {
                // INNER JOIN from the root candidate table to this candidates table
                stmt.join(JoinType.INNER_JOIN, candidateSQLTable, candidateIdMapping, table, null, table.getIdMapping(), null, stmt.getPrimaryTable().getGroupName(), true);
            }
        }
        else
        {
            // No root candidate table, so join direct to this candidate
            JavaTypeMapping candidateIdMapping = table.getIdMapping();
            if (hasOption(OPTION_ALLOW_NULLS))
            {
                // Put element table in same table group since all relates to the elements
                candidateSQLTable = stmt.join(JoinType.LEFT_OUTER_JOIN, null, joinElementMapping, table, null, candidateIdMapping, null, 
                    stmt.getPrimaryTable().getGroupName(), true);
            }
            else
            {
                // Put element table in same table group since all relates to the elements
                candidateSQLTable = stmt.join(JoinType.INNER_JOIN, null, joinElementMapping, table, null, candidateIdMapping, null, stmt.getPrimaryTable().getGroupName(), true);
            }
        }

        // Add any discriminator restriction in the table for the specified class
        // Caters for the case where we have more than 1 class stored in this table
        SQLExpressionFactory factory = storeMgr.getSQLExpressionFactory();
        JavaTypeMapping discriminatorMapping = table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
        DiscriminatorMetaData discriminatorMetaData = table.getDiscriminatorMetaData();
        if (discriminatorMapping != null && discriminatorMetaData.getStrategy() != DiscriminatorStrategy.NONE)
        {
            // Restrict to valid discriminator value where we have a discriminator specified on this table
            BooleanExpression discExpr = SQLStatementHelper.getExpressionForDiscriminatorForClass(stmt, className, discriminatorMetaData, discriminatorMapping, stmt.getPrimaryTable(), clr);
            stmt.whereAnd(discExpr, false);
        }

        // Eliminate any subclasses (catered for in separate UNION statement)
        Iterator<String> subIter = storeMgr.getSubClassesForClass(className, false, clr).iterator();
        while (subIter.hasNext())
        {
            String subclassName = subIter.next();
            DatastoreClass[] subclassTables = null;
            DatastoreClass subclassTable = storeMgr.getDatastoreClass(subclassName, clr);

            if (subclassTable == null)
            {
                AbstractClassMetaData targetSubCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(subclassName, clr);
                AbstractClassMetaData[] targetSubCmds = storeMgr.getClassesManagingTableForClass(targetSubCmd, clr);
                subclassTables = new DatastoreClass[targetSubCmds.length];
                for (int i=0;i<targetSubCmds.length;i++)
                {
                    subclassTables[i] = storeMgr.getDatastoreClass(targetSubCmds[i].getFullClassName(), clr);
                }
            }
            else
            {
                subclassTables = new DatastoreClass[1];
                subclassTables[0] = subclassTable;
            }

            for (int i=0;i<subclassTables.length;i++)
            {
                if (subclassTables[i] != table)
                {
                    // Subclass of our class is stored in different table to the candidate so exclude it
                    // Adds FROM clause of "LEFT OUTER JOIN {subTable} ON ..."
                    // and WHERE clause of "{subTable}.ID = NULL"
                    JavaTypeMapping subclassIdMapping = subclassTables[i].getIdMapping();
                    SQLTable sqlTableSubclass = stmt.join(JoinType.LEFT_OUTER_JOIN, null, joinElementMapping, subclassTables[i], null, subclassIdMapping, null, 
                        stmt.getPrimaryTable().getGroupName(), true);
                    SQLExpression subclassIdExpr = factory.newExpression(stmt, sqlTableSubclass, subclassIdMapping);
                    SQLExpression nullExpr = new NullLiteral(stmt, null, null, null);
                    stmt.whereAnd(subclassIdExpr.eq(nullExpr), false);
                }
            }
        }

        if (hasOption(OPTION_SELECT_DN_TYPE))
        {
            // Add SELECT of dummy metadata for this class ("'mydomain.MyClass' AS DN_TYPE")
            addTypeSelectForClass(stmt, className);
        }

        return stmt;
    }

    /**
     * Convenience method to add a SELECT of a dummy column accessible as "DN_TYPE" storing the class name.
     * @param stmt SQLStatement
     * @param className Name of the class
     */
    private void addTypeSelectForClass(SelectStatement stmt, String className)
    {
        // Add SELECT of dummy metadata for this class ("'mydomain.MyClass' AS DN_TYPE")
        /*if (hasOption(OPTION_ALLOW_NULLS))
        {
            NullLiteral nullLtl = new NullLiteral(stmt, null, null, null);
            stmt.select(nullLtl, NUC_TYPE_COLUMN);
        }
        else
        {*/
        // Add SELECT of dummy column accessible as "DN_TYPE" containing the classname
        JavaTypeMapping m = storeMgr.getMappingManager().getMapping(String.class);
        String nuctypeName = className;
        if (maxClassNameLength > nuctypeName.length())
        {
            nuctypeName = StringUtils.leftAlignedPaddedString(nuctypeName, maxClassNameLength);
        }
        StringLiteral lit = new StringLiteral(stmt, m, nuctypeName, null);
        stmt.select(lit, DN_TYPE_COLUMN);
        /*}*/
    }
}