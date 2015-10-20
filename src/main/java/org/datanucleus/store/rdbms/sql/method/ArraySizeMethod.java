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
package org.datanucleus.store.rdbms.sql.method;

import java.lang.reflect.Array;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.expression.ArrayLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {arrExpr1}.size().
 * Returns a NumericExpression. Equates to a sub-query on the table for the array
 * returning the COUNT(*) of the number of elements. Something like
 * <PRE>
 * SELECT COUNT(*) FROM ARRTABLE A0_SUB WHERE A0_SUB.OWNER_ID_OID = A0.OWNER_ID
 * </PRE>
 * where A0 is the candidate table in the outer query, and ARRTABLE is the join table (if using 
 * join collection) or the element table (if using FK array).
 */
public class ArraySizeMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args != null && args.size() > 0)
        {
            throw new NucleusException(Localiser.msg("060015", "size/length", "ArrayExpression"));
        }

        if (expr instanceof ArrayLiteral)
        {
            // Just return the array length since we have the value
            return exprFactory.newLiteral(stmt, exprFactory.getMappingForType(int.class, false), Integer.valueOf(Array.getLength(((ArrayLiteral)expr).getValue())));
        }

        AbstractMemberMetaData ownerMmd = expr.getJavaTypeMapping().getMemberMetaData();
        String elementType = ownerMmd.getArray().getElementType();
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();

        // TODO Allow for interface elements, etc
        JavaTypeMapping ownerMapping = null;
        Table arrayTbl = null;
        if (ownerMmd.getMappedBy() != null)
        {
            // Bidirectional
            AbstractMemberMetaData elementMmd = ownerMmd.getRelatedMemberMetaData(clr)[0];
            if (ownerMmd.getJoinMetaData() != null || elementMmd.getJoinMetaData() != null)
            {
                // JoinTable
                arrayTbl = storeMgr.getTable(ownerMmd);
                ownerMapping = ((JoinTable)arrayTbl).getOwnerMapping();
            }
            else
            {
                // ForeignKey
                arrayTbl = storeMgr.getDatastoreClass(elementType, clr);
                ownerMapping = arrayTbl.getMemberMapping(elementMmd);
            }
        }
        else
        {
            // Unidirectional
            if (ownerMmd.getJoinMetaData() != null)
            {
                // JoinTable
                arrayTbl = storeMgr.getTable(ownerMmd);
                ownerMapping = ((JoinTable)arrayTbl).getOwnerMapping();
            }
            else
            {
                // ForeignKey
                arrayTbl = storeMgr.getDatastoreClass(elementType, clr);
                ownerMapping = ((DatastoreClass)arrayTbl).getExternalMapping(ownerMmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
            }
        }

        SQLStatement subStmt = new SelectStatement(stmt, storeMgr, arrayTbl, null, null);
        subStmt.setClassLoaderResolver(clr);
        JavaTypeMapping mapping = storeMgr.getMappingManager().getMappingWithDatastoreMapping(String.class, false, false, clr);
        SQLExpression countExpr = exprFactory.newLiteral(subStmt, mapping, "COUNT(*)");
        ((StringLiteral)countExpr).generateStatementWithoutQuotes();
        subStmt.select(countExpr, null);

        SQLExpression elementOwnerExpr = exprFactory.newExpression(subStmt, subStmt.getPrimaryTable(), ownerMapping);
        SQLExpression ownerIdExpr = exprFactory.newExpression(stmt, expr.getSQLTable(), expr.getSQLTable().getTable().getIdMapping());
        subStmt.whereAnd(elementOwnerExpr.eq(ownerIdExpr), true);

        JavaTypeMapping subqMapping = exprFactory.getMappingForType(Integer.class, false);
        SQLExpression subqExpr = new NumericSubqueryExpression(stmt, subStmt);
        subqExpr.setJavaTypeMapping(subqMapping);
        return subqExpr;
    }
}