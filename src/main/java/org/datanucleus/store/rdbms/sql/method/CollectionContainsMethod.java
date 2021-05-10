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
package org.datanucleus.store.rdbms.sql.method;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.query.compiler.CompilationComponent;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLJoin;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.CollectionExpression;
import org.datanucleus.store.rdbms.sql.expression.CollectionLiteral;
import org.datanucleus.store.rdbms.sql.expression.EnumExpression;
import org.datanucleus.store.rdbms.sql.expression.IllegalExpressionOperationException;
import org.datanucleus.store.rdbms.sql.expression.InExpression;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.store.rdbms.sql.expression.UnboundExpression;
import org.datanucleus.store.rdbms.table.CollectionTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Method for evaluating {collExpr1}.contains({elemExpr}).
 * Returns a BooleanExpression.
 */
public class CollectionContainsMethod implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0 || args.size() > 1)
        {
            throw new NucleusException(Localiser.msg("060016", "contains", "CollectionExpression", 1));
        }

        CollectionExpression collExpr = (CollectionExpression)expr;
        AbstractMemberMetaData mmd = collExpr.getJavaTypeMapping().getMemberMetaData();
        SQLExpression elemExpr = args.get(0);
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        if (elemExpr.isParameter())
        {
            // Element is a parameter so make sure its type is set
            if (mmd != null && mmd.getCollection() != null)
            {
                Class elementCls = stmt.getQueryGenerator().getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
                stmt.getQueryGenerator().bindParameter(elemExpr.getParameterName(), elementCls);
            }
        }

        ClassLoaderResolver clr = stmt.getQueryGenerator().getClassLoaderResolver();
        if (collExpr instanceof CollectionLiteral)
        {
            // Literal collection
            CollectionLiteral lit = (CollectionLiteral)collExpr;
            Collection coll = (Collection)lit.getValue();
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            if (coll == null || coll.isEmpty())
            {
                return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
            }

            if (collExpr.isParameter())
            {
                stmt.getQueryGenerator().useParameterExpressionAsLiteral((CollectionLiteral)collExpr);
            }

            boolean useInExpression = false;
            List<SQLExpression> collElementExprs = lit.getElementExpressions();
            if (collElementExprs != null && !collElementExprs.isEmpty())
            {
                // Make sure the the collection element(s) are compatible with the elemExpr
                boolean incompatible = true;
                Class elemtype = clr.classForName(elemExpr.getJavaTypeMapping().getType());
                Iterator<SQLExpression> collElementExprIter = collElementExprs.iterator();
                while (collElementExprIter.hasNext())
                {
                    SQLExpression collElementExpr = collElementExprIter.next();
                    Class collElemType = clr.classForName(collElementExpr.getJavaTypeMapping().getType());
                    if (elementTypeCompatible(elemtype, collElemType))
                    {
                        incompatible = false;
                        break;
                    }
                }
                if (incompatible)
                {
                    // The provided element type isn't assignable to any of the input collection elements!
                    return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
                }

                // Check if we should compare using an "IN (...)" expression
                SQLExpression collElementExpr = collElementExprs.get(0);
                if (collElementExpr instanceof StringExpression || collElementExpr instanceof NumericExpression ||
                    collElementExpr instanceof TemporalExpression || collElementExpr instanceof CharacterExpression || collElementExpr instanceof EnumExpression)
                {
                    useInExpression = true;
                }
            }
            if (useInExpression)
            {
                // Return "elem IN (val1, val2, ...)"
                SQLExpression[] exprs = (collElementExprs != null ? collElementExprs.toArray(new SQLExpression[collElementExprs.size()]) : null);
                return new InExpression(elemExpr, exprs);
            }

            // Return "elem == val1 || elem == val2 || elem == val3 ..."
            BooleanExpression bExpr = null;
            if (collElementExprs != null)
            {
                for (int i=0; i<collElementExprs.size(); i++)
                {
                    if (bExpr == null)
                    {
                        bExpr = (collElementExprs.get(i)).eq(elemExpr); 
                    }
                    else
                    {
                        bExpr = bExpr.ior((collElementExprs.get(i)).eq(elemExpr)); 
                    }
                }
            }
            if (bExpr != null)
            {
                bExpr.encloseInParentheses();
            }
            return bExpr;
        }

        if (mmd == null)
        {
            throw new NucleusUserException("Cannot perform Collection.contains when the field metadata is not provided");
        }
        if (mmd.isSerialized())
        {
            throw new NucleusUserException("Cannot perform Collection.contains when the collection is being serialised");
        }

        ApiAdapter api = stmt.getRDBMSManager().getApiAdapter();
        Class elementType = clr.classForName(mmd.getCollection().getElementType());
        if (!api.isPersistable(elementType) && mmd.getJoinMetaData() == null)
        {
            throw new NucleusUserException("Cannot perform Collection.contains when the collection<Non-Persistable> is not in a join table");
        }

        if (stmt.getQueryGenerator().getCompilationComponent() == CompilationComponent.FILTER)
        {
            boolean useSubquery = getNeedsSubquery(stmt, collExpr, elemExpr);
            JoinType joinType = JoinType.INNER_JOIN;
            if (elemExpr instanceof UnboundExpression)
            {
                // See if the user has defined what should be used
                String varName = ((UnboundExpression)elemExpr).getVariableName();
                String extensionName = "datanucleus.query.jdoql." + varName + ".join";
                String extensionValue = (String) stmt.getQueryGenerator().getValueForExtension(extensionName);
                if (extensionValue != null)
                {
                    if (extensionValue.equalsIgnoreCase("SUBQUERY"))
                    {
                        useSubquery = true;
                    }
                    else if (extensionValue.equalsIgnoreCase("INNERJOIN"))
                    {
                        useSubquery = false;
                    }
                    else if (extensionValue.equalsIgnoreCase("LEFTOUTERJOIN"))
                    {
                        joinType = JoinType.LEFT_OUTER_JOIN;
                    }
                }
            }

            if (useSubquery)
            {
                return containsAsSubquery(stmt, collExpr, elemExpr);
            }

            return containsAsJoin(stmt, collExpr, elemExpr, joinType);
        }

        return containsAsSubquery(stmt, collExpr, elemExpr);
    }

    /**
     * Convenience method to decide if we handle the contains() by using a subquery, or otherwise via an inner join. If there is an OR or a NOT in the query then uses a subquery.
     * @param stmt SQLStatement
     * @param collExpr SQL Expression for the collection
     * @param elemExpr SQL Expression for the element
     * @return Whether to use a subquery
     */
    protected boolean getNeedsSubquery(SQLStatement stmt, SQLExpression collExpr, SQLExpression elemExpr)
    {
        if (elemExpr instanceof UnboundExpression)
        {
            // TODO Check if *this* "contains" is negated, not just any of them (and remove above check)
            // TODO Check if any OR relates to the element expression (variable) only, if so then INNER JOIN
            NucleusLogger.QUERY.debug(">> collection.contains collExpr=" + collExpr + " elemExpr=" + elemExpr +
                " elem.variable=" + ((UnboundExpression)elemExpr).getVariableName() + 
                " need to implement check on whether there is a !coll or an OR using just this variable");
        }

        boolean needsSubquery = false;
        Boolean hasOR = (Boolean)stmt.getQueryGenerator().getProperty("Filter.OR");
        if (hasOR != null && hasOR.booleanValue())
        {
            needsSubquery = true;
        }
        Boolean hasNOT = (Boolean)stmt.getQueryGenerator().getProperty("Filter.NOT");
        if (hasNOT != null && hasNOT.booleanValue())
        {
            needsSubquery = true;
        }
        return needsSubquery;
    }

    /**
     * Method to return an expression for Collection.contains using INNER JOIN to the element.
     * This is only for use when there are no "!contains" and no "OR" operations.
     * Creates SQL by adding INNER JOIN to the join table (where it exists), and also to the element table
     * adding an AND condition on the element (with value of the elemExpr).
     * Returns a BooleanExpression "TRUE" (since the INNER JOIN will guarantee if the element is
     * contained of not).
     * @param stmt SQLStatement
     * @param collExpr Collection expression
     * @param elemExpr Expression for the element
     * @param joinType Join type
     * @return Contains expression
     */
    protected SQLExpression containsAsJoin(SQLStatement stmt, CollectionExpression collExpr, SQLExpression elemExpr, SQLJoin.JoinType joinType)
    {
        boolean elemIsUnbound = (elemExpr instanceof UnboundExpression);
        String varName = null;
        String elemAlias = null;
        String elemType = null;
        if (elemIsUnbound)
        {
            varName = ((UnboundExpression)elemExpr).getVariableName();
            NucleusLogger.QUERY.debug("collection.contains(" + elemExpr + ") binding unbound variable " + varName + " using INNER JOIN");
        }
        else if (!stmt.getQueryGenerator().hasExplicitJoins())
        {
            if (stmt.getJoinTypeForTable(elemExpr.getSQLTable()) == JoinType.CROSS_JOIN)
            {
                elemAlias = stmt.removeCrossJoin(elemExpr.getSQLTable());
                elemIsUnbound = true;
                elemType = elemExpr.getJavaTypeMapping().getType();
                NucleusLogger.QUERY.debug("collection.contains(" + elemExpr + ") was previously bound as CROSS JOIN but changing to INNER JOIN");
            }
        }

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        ClassLoaderResolver clr = stmt.getQueryGenerator().getClassLoaderResolver();
        AbstractMemberMetaData mmd = collExpr.getJavaTypeMapping().getMemberMetaData();
        AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
        CollectionTable joinTbl = (CollectionTable)storeMgr.getTable(mmd);
        if (elemIsUnbound)
        {
            Class varType = stmt.getQueryGenerator().getTypeOfVariable(varName);
            if (varType != null)
            {
                elemType = varType.getName();
                elemCmd = storeMgr.getMetaDataManager().getMetaDataForClass(elemType, clr);
            }
        }
        if (elemType == null)
        {
            elemType = mmd.getCollection().getElementType();
        }

        if (joinTbl != null)
        {
            // JoinTable Collection - join from owner to join, then from join to element
            if (elemCmd == null)
            {
                // Collection<Non-PC>
                SQLTable joinSqlTbl = stmt.join(joinType, collExpr.getSQLTable(), collExpr.getSQLTable().getTable().getIdMapping(), joinTbl, elemAlias, joinTbl.getOwnerMapping(), null, null);

                SQLExpression elemIdExpr = exprFactory.newExpression(stmt, joinSqlTbl, joinTbl.getElementMapping());
                if (elemIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(varName, null, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to element
                    addRestrictionOnElement(stmt, elemIdExpr, elemExpr);
                }
            }
            else
            {
                // Collection<PC>
                SQLTable joinSqlTbl = stmt.join(joinType, collExpr.getSQLTable(), collExpr.getSQLTable().getTable().getIdMapping(), joinTbl, null, joinTbl.getOwnerMapping(), null, null);

                if (!mmd.getCollection().isEmbeddedElement())
                {
                    DatastoreClass elemTbl = storeMgr.getDatastoreClass(elemType, clr);
                    SQLTable elemSqlTbl = null;
                    if (joinTbl.getElementMapping() instanceof ReferenceMapping &&
                        ((ReferenceMapping)joinTbl.getElementMapping()).getMappingStrategy() == ReferenceMapping.PER_IMPLEMENTATION_MAPPING)
                    {
                        JavaTypeMapping elemMapping = null;
                        JavaTypeMapping[] elemImplMappings = ((ReferenceMapping)joinTbl.getElementMapping()).getJavaTypeMapping();
                        for (int i=0;i<elemImplMappings.length;i++)
                        {
                            if (elemImplMappings[i].getType().equals(elemCmd.getFullClassName()))
                            {
                                elemMapping = elemImplMappings[i];
                                break;
                            }
                        }
                        elemSqlTbl = stmt.join(joinType, joinSqlTbl, elemMapping, joinTbl.getElementMapping(), elemTbl, elemAlias, elemTbl.getIdMapping(), null, null, null, true, null);
                    }
                    else
                    {
                        elemSqlTbl = stmt.join(joinType, joinSqlTbl, joinTbl.getElementMapping(), elemTbl, elemAlias, elemTbl.getIdMapping(), null, null);
                    }

                    SQLExpression elemIdExpr = exprFactory.newExpression(stmt, elemSqlTbl, elemTbl.getIdMapping());
                    if (elemIsUnbound)
                    {
                        // Bind the variable in the QueryGenerator
                        stmt.getQueryGenerator().bindVariable(varName, elemCmd, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
                    }
                    else
                    {
                        // Add restrict to element
                        addRestrictionOnElement(stmt, elemIdExpr, elemExpr);
                    }
                }
                else
                {
                    SQLExpression elemIdExpr = exprFactory.newExpression(stmt, joinSqlTbl, joinTbl.getElementMapping());
                    if (elemIsUnbound)
                    {
                        // Bind the variable in the QueryGenerator
                        stmt.getQueryGenerator().bindVariable(varName, elemCmd, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
                    }
                    else
                    {
                        // Add restrict to element
                        addRestrictionOnElement(stmt, elemIdExpr, elemExpr);
                    }
                }
            }
        }
        else
        {
            // FK Collection - join from owner to element
            DatastoreClass elemTbl = storeMgr.getDatastoreClass(mmd.getCollection().getElementType(), clr);
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = elemTbl.getMemberMapping(mmd.getRelatedMemberMetaData(clr)[0]);
            }
            else
            {
                ownerMapping = elemTbl.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
            }

            SQLTable elemSqlTbl = stmt.join(joinType, collExpr.getSQLTable(), collExpr.getSQLTable().getTable().getIdMapping(), elemTbl, elemAlias, ownerMapping, null, null);

            if (elemIsUnbound)
            {
                SQLExpression elemIdExpr = null;
                if (!elemType.equals(mmd.getCollection().getElementType()))
                {
                    // Variable is defined as a subclass of the declared type so add extra join to variable type
                    DatastoreClass varTbl = storeMgr.getDatastoreClass(elemType, clr);
                    SQLTable varSqlTbl = stmt.join(joinType, elemSqlTbl, elemTbl.getIdMapping(), varTbl, null, varTbl.getIdMapping(), null, null);
                    elemIdExpr = exprFactory.newExpression(stmt, varSqlTbl, varTbl.getIdMapping());
                }
                else
                {
                    elemIdExpr = exprFactory.newExpression(stmt, elemSqlTbl, elemTbl.getIdMapping());
                }

                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(varName, elemCmd, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to element
                SQLExpression elemIdExpr = exprFactory.newExpression(stmt, elemSqlTbl, elemTbl.getIdMapping());
                addRestrictionOnElement(stmt, elemIdExpr, elemExpr);
            }
        }

        JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
        return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, true));
    }

    /**
     * Method to return an expression for Collection.contains using a subquery "EXISTS".
     * This is for use when there are "!contains" or "OR" operations in the filter.
     * Creates the following SQL,
     * <ul>
     * <li><b>Collection of NonPC using join table</b>
     * <pre>
     * SELECT 1 FROM JOIN_TBL A0_SUB 
     * WHERE A0_SUB.JOIN_OWN_ID = A0.ID AND A0_SUB.JOIN_ELEM_ID = {elemExpr}
     * </pre>
     * </li>
     * <li><b>Collection of PC using join table</b>
     * <pre>
     * SELECT 1 FROM ELEM_TABLE A0_SUB INNER JOIN JOIN_TBL B0 ON ... 
     * WHERE B0.JOIN_OWN_ID = A0.ID AND A0_SUB.ID = {elemExpr}
     * </pre>
     * </li>
     * <li><b>Collection of PC using FK</b>
     * <pre>
     * SELECT 1 FROM ELEM_TABLE A0_SUB
     * WHERE A0_SUB.OWN_ID = A0.ID AND A0_SUB.ID = {elemExpr}
     * </pre>
     * </li>
     * </ul>
     * and returns a BooleanSubqueryExpression ("EXISTS (subquery)")
     * @param stmt SQLStatement
     * @param collExpr Collection expression
     * @param elemExpr Expression for the element
     * @return Contains expression
     */
    protected SQLExpression containsAsSubquery(SQLStatement stmt, CollectionExpression collExpr, SQLExpression elemExpr)
    {
        boolean elemIsUnbound = (elemExpr instanceof UnboundExpression);
        String varName = null;
        if (elemIsUnbound)
        {
            varName = ((UnboundExpression)elemExpr).getVariableName();
            NucleusLogger.QUERY.debug("collection.contains(" + elemExpr + ") binding unbound variable " + varName + " using SUBQUERY");
        }

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        ClassLoaderResolver clr = stmt.getQueryGenerator().getClassLoaderResolver();
        AbstractMemberMetaData mmd = collExpr.getJavaTypeMapping().getMemberMetaData();
        AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
        CollectionTable joinTbl = (CollectionTable)storeMgr.getTable(mmd);
        String elemType = mmd.getCollection().getElementType();
        if (elemIsUnbound)
        {
            Class varType = stmt.getQueryGenerator().getTypeOfVariable(varName);
            if (varType != null)
            {
                elemType = varType.getName();
                elemCmd = storeMgr.getMetaDataManager().getMetaDataForClass(elemType, clr);
            }
        }
        SelectStatement subStmt = null;
        if (joinTbl != null)
        {
            // JoinTable Collection
            if (elemCmd == null)
            {
                // Collection<Non-PC>
                subStmt = new SelectStatement(stmt, storeMgr, joinTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Restrict to collection owner
                JavaTypeMapping ownerMapping = ((JoinTable)joinTbl).getOwnerMapping();
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, collExpr.getSQLTable(), collExpr.getSQLTable().getTable().getIdMapping());
                subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

                SQLExpression elemIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), joinTbl.getElementMapping());
                if (elemIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(varName, null, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to element
                    addRestrictionOnElement(subStmt, elemIdExpr, elemExpr);
                }
            }
            else
            {
                // Collection<PC>
                DatastoreClass elemTbl = storeMgr.getDatastoreClass(elemType, clr);
                subStmt = new SelectStatement(stmt, storeMgr, elemTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Join to join table
                SQLTable joinSqlTbl = subStmt.join(JoinType.INNER_JOIN, subStmt.getPrimaryTable(), elemTbl.getIdMapping(), joinTbl, null, joinTbl.getElementMapping(),
                    null, null, true, null);

                // Restrict to collection owner
                JavaTypeMapping ownerMapping = ((JoinTable)joinTbl).getOwnerMapping();
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, joinSqlTbl, ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, collExpr.getSQLTable(), collExpr.getSQLTable().getTable().getIdMapping());
                subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

                SQLExpression elemIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(),elemTbl.getIdMapping());
                if (elemIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(varName, elemCmd, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to element
                    addRestrictionOnElement(subStmt, elemIdExpr, elemExpr);
                }
            }
        }
        else
        {
            // FK Collection
            DatastoreClass elemTbl = storeMgr.getDatastoreClass(mmd.getCollection().getElementType(), clr);
            subStmt = new SelectStatement(stmt, storeMgr, elemTbl, null, null);
            subStmt.setClassLoaderResolver(clr);
            JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
            subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

            // Restrict to collection owner
            JavaTypeMapping ownerMapping = null;
            if (mmd.getMappedBy() != null)
            {
                ownerMapping = elemTbl.getMemberMapping(mmd.getRelatedMemberMetaData(clr)[0]);
            }
            else
            {
                ownerMapping = elemTbl.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
            }
            SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
            SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, collExpr.getSQLTable(), collExpr.getSQLTable().getTable().getIdMapping());
            subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

            if (elemIsUnbound)
            {
                SQLExpression elemIdExpr = null;
                if (!elemType.equals(mmd.getCollection().getElementType()))
                {
                    // Variable is defined as a subclass of the declared type so add extra join to variable type
                    DatastoreClass varTbl = storeMgr.getDatastoreClass(elemType, clr);
                    SQLTable varSqlTbl = subStmt.join(JoinType.INNER_JOIN, subStmt.getPrimaryTable(), elemTbl.getIdMapping(), null, varTbl, null, varTbl.getIdMapping(),
                        null, null, null, true, null);
                    elemIdExpr = exprFactory.newExpression(subStmt, varSqlTbl, varTbl.getIdMapping());
                }
                else
                {
                    elemIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), elemTbl.getIdMapping());
                }

                // Bind the variable in the QueryGenerator
                stmt.getQueryGenerator().bindVariable(varName, elemCmd, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
            }
            else
            {
                // Add restrict to element
                SQLExpression elemIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), elemTbl.getIdMapping());
                addRestrictionOnElement(subStmt, elemIdExpr, elemExpr);
            }
        }

        return new BooleanSubqueryExpression(stmt, "EXISTS", subStmt);
    }

    protected void addRestrictionOnElement(SQLStatement stmt, SQLExpression elemIdExpr, SQLExpression elemExpr)
    {
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        try
        {
            stmt.whereAnd(elemIdExpr.eq(elemExpr), true);
        }
        catch (IllegalExpressionOperationException ieoe)
        {
            NucleusLogger.QUERY.warn("Collection.contains element expression supplied is inconsistent with element type of this collection : " + ieoe.getMessage());
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            BooleanExpression notContainedExpr = exprFactory.newLiteral(stmt, m, false).eq(exprFactory.newLiteral(stmt, m, true));
            stmt.whereAnd(notContainedExpr, true);
        }
    }

    protected boolean elementTypeCompatible(Class elementType, Class collectionElementType)
    {
        if (!elementType.isPrimitive() && collectionElementType.isPrimitive() && !collectionElementType.isAssignableFrom(elementType) && !elementType.isAssignableFrom(collectionElementType))
        {
            return false;
        }
        else if (elementType.isPrimitive())
        {
            if (elementType == boolean.class && collectionElementType == Boolean.class)
            {
                return true;
            }
            else if (elementType == byte.class && collectionElementType == Byte.class)
            {
                return true;
            }
            else if (elementType == char.class && collectionElementType == Character.class)
            {
                return true;
            }
            else if (elementType == double.class && collectionElementType == Double.class)
            {
                return true;
            }
            else if (elementType == float.class && collectionElementType == Float.class)
            {
                return true;
            }
            else if (elementType == int.class && collectionElementType == Integer.class)
            {
                return true;
            }
            else if (elementType == long.class && collectionElementType == Long.class)
            {
                return true;
            }
            else if (elementType == short.class && collectionElementType == Short.class)
            {
                return true;
            }
            return false;
        }

        return true;
    }
}