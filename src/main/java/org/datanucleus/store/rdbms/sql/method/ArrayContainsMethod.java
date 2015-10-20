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

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.ArrayExpression;
import org.datanucleus.store.rdbms.sql.expression.ArrayLiteral;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.ByteExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.EnumExpression;
import org.datanucleus.store.rdbms.sql.expression.InExpression;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.store.rdbms.sql.expression.UnboundExpression;
import org.datanucleus.store.rdbms.table.ArrayTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.util.NucleusLogger;

/**
 * Method for evaluating {arrExpr}.contains(elemExpr).
 * Returns a BooleanExpression.
 */
public class ArrayContainsMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() != 1)
        {
            throw new NucleusException("Incorrect arguments for Array.contains(SQLExpression)");
        }

        ArrayExpression arrExpr = (ArrayExpression)expr;
        SQLExpression elemExpr = args.get(0);

        if (elemExpr.isParameter())
        {
            // Element is a parameter so make sure its type is set
            AbstractMemberMetaData mmd = arrExpr.getJavaTypeMapping().getMemberMetaData();
            if (mmd != null)
            {
                stmt.getQueryGenerator().bindParameter(elemExpr.getParameterName(), mmd.getType().getComponentType());
            }
        }

        if (expr instanceof ArrayLiteral)
        {
            if (elemExpr instanceof UnboundExpression)
            {
                Class elemCls = clr.classForName(arrExpr.getJavaTypeMapping().getType()).getComponentType();
                elemExpr = stmt.getQueryGenerator().bindVariable((UnboundExpression)elemExpr, elemCls);
            }

            ArrayLiteral lit = (ArrayLiteral)expr;
            Object array = lit.getValue();
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            if (array == null || Array.getLength(array) == 0)
            {
                return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
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
                    collElementExpr instanceof TemporalExpression || collElementExpr instanceof CharacterExpression ||
                    collElementExpr instanceof ByteExpression || collElementExpr instanceof EnumExpression)
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

            // TODO If elemExpr is a parameter and collExpr is derived from a parameter ?
            BooleanExpression bExpr = null;
            List<SQLExpression> elementExprs = lit.getElementExpressions();
            for (int i=0; i<elementExprs.size(); i++)
            {
                SQLExpression arrElemExpr = elementExprs.get(i);
                if (bExpr == null)
                {
                    bExpr = arrElemExpr.eq(elemExpr); 
                }
                else
                {
                    bExpr = bExpr.ior(arrElemExpr.eq(elemExpr)); 
                }
            }
            if (bExpr != null)
            {
                bExpr.encloseInParentheses();
            }
            return bExpr;
        }
        else if (arrExpr.getElementExpressions() != null)
        {
            if (elemExpr instanceof UnboundExpression)
            {
                Class elemCls = clr.classForName(arrExpr.getJavaTypeMapping().getType()).getComponentType();
                elemExpr = stmt.getQueryGenerator().bindVariable((UnboundExpression)elemExpr, elemCls);
            }

            // Array defined in query that has some expressions for elements, so just do OR chain
            BooleanExpression bExpr = null;
            List<SQLExpression> elementExprs = arrExpr.getElementExpressions();
            for (int i=0; i<elementExprs.size(); i++)
            {
                SQLExpression arrElemExpr = elementExprs.get(i);
                if (bExpr == null)
                {
                    bExpr = arrElemExpr.eq(elemExpr); 
                }
                else
                {
                    bExpr = bExpr.ior(arrElemExpr.eq(elemExpr)); 
                }
            }
            if (bExpr != null)
            {
                bExpr.encloseInParentheses();
            }
            return bExpr;
        }
        else
        {
            // TODO Support inner join variant
            return containsAsSubquery(arrExpr, elemExpr);
        }
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
     * </ul>
     * and returns a BooleanSubqueryExpression ("EXISTS (subquery)")
     * @param arrExpr Collection expression
     * @param elemExpr Expression for the element
     * @return Contains expression
     */
    protected SQLExpression containsAsSubquery(ArrayExpression arrExpr, SQLExpression elemExpr)
    {
        boolean elemIsUnbound = (elemExpr instanceof UnboundExpression);
        String varName = null;
        if (elemIsUnbound)
        {
            varName = ((UnboundExpression)elemExpr).getVariableName();
            NucleusLogger.QUERY.debug(">> Array.contains binding unbound variable " + varName +
                " using SUBQUERY");
        }

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        AbstractMemberMetaData mmd = arrExpr.getJavaTypeMapping().getMemberMetaData();
        AbstractClassMetaData elemCmd =
            mmd.getArray().getElementClassMetaData(clr, storeMgr.getMetaDataManager());
        ArrayTable joinTbl = (ArrayTable)storeMgr.getTable(mmd);
        SQLStatement subStmt = null;
        if (joinTbl != null)
        {
            // JoinTable array
            if (elemCmd == null)
            {
                // Array<Non-PC>
                subStmt = new SelectStatement(stmt, storeMgr, joinTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Restrict to array owner
                JavaTypeMapping ownerMapping = ((JoinTable)joinTbl).getOwnerMapping();
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, arrExpr.getSQLTable(), arrExpr.getSQLTable().getTable().getIdMapping());
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
                    subStmt.whereAnd(elemIdExpr.eq(elemExpr), true);
                }
            }
            else
            {
                // Array<PC>
                DatastoreClass elemTbl = storeMgr.getDatastoreClass(mmd.getArray().getElementType(), clr);
                subStmt = new SelectStatement(stmt, storeMgr, elemTbl, null, null);
                subStmt.setClassLoaderResolver(clr);
                JavaTypeMapping oneMapping = storeMgr.getMappingManager().getMapping(Integer.class);
                subStmt.select(exprFactory.newLiteral(subStmt, oneMapping, 1), null);

                // Join to join table
                SQLTable joinSqlTbl = subStmt.innerJoin(subStmt.getPrimaryTable(), elemTbl.getIdMapping(), joinTbl, null, joinTbl.getElementMapping(), null, null);

                // Restrict to array owner
                JavaTypeMapping ownerMapping = ((JoinTable)joinTbl).getOwnerMapping();
                SQLExpression ownerExpr = exprFactory.newExpression(subStmt, joinSqlTbl, ownerMapping);
                SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, arrExpr.getSQLTable(), arrExpr.getSQLTable().getTable().getIdMapping());
                subStmt.whereAnd(ownerExpr.eq(ownerIdExpr), true);

                SQLExpression elemIdExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(),
                    elemTbl.getIdMapping());
                if (elemIsUnbound)
                {
                    // Bind the variable in the QueryGenerator
                    stmt.getQueryGenerator().bindVariable(varName, elemCmd, elemIdExpr.getSQLTable(), elemIdExpr.getJavaTypeMapping());
                }
                else
                {
                    // Add restrict to element
                    subStmt.whereAnd(elemIdExpr.eq(elemExpr), true);
                }
            }
        }
        else
        {
            // TODO Support FK array ?
            throw new NucleusException("Dont support evaluation of ARRAY.contains when no join table is used");
        }

        return new BooleanSubqueryExpression(stmt, "EXISTS", subStmt);
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