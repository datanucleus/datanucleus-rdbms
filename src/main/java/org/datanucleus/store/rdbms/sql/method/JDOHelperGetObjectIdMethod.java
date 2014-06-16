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

import java.util.List;

import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceIdMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.expression.IllegalExpressionOperationException;
import org.datanucleus.store.rdbms.sql.expression.NullLiteral;
import org.datanucleus.store.rdbms.sql.expression.ObjectExpression;
import org.datanucleus.store.rdbms.sql.expression.ObjectLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLLiteral;

/**
 * Expression handler to evaluate JDOHelper.getObjectId({expression}).
 * Returns an ObjectExpression or NullLiteral.
 */
public class JDOHelperGetObjectIdMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression ignore, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0)
        {
            throw new NucleusUserException("Cannot invoke JDOHelper.getObjectId without an argument");
        }

        SQLExpression expr = args.get(0);
        if (expr == null)
        {
            return new NullLiteral(stmt, null, null, null);
        }
        if (expr instanceof SQLLiteral)
        {
            RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
            ApiAdapter api = storeMgr.getApiAdapter();
            Object id = api.getIdForObject(((SQLLiteral)expr).getValue());
            if (id == null)
            {
                return new NullLiteral(stmt, null, null, null);
            }

            JavaTypeMapping m = getMappingForClass(id.getClass());
            return new ObjectLiteral(stmt, m, id, null);
        }
        else if (ObjectExpression.class.isAssignableFrom(expr.getClass()))
        {
            // When the expression represents a PC object need to extract out as the identity
            if (expr.getJavaTypeMapping() instanceof PersistableMapping)
            {
                JavaTypeMapping mapping = new PersistableIdMapping((PersistableMapping)expr.getJavaTypeMapping());
                return new ObjectExpression(stmt, expr.getSQLTable(), mapping);
            }
            else if (expr.getJavaTypeMapping() instanceof ReferenceMapping)
            {
                JavaTypeMapping mapping = new ReferenceIdMapping((ReferenceMapping)expr.getJavaTypeMapping());
                return new ObjectExpression(stmt, expr.getSQLTable(), mapping);
            }
            return expr;
        }

        throw new IllegalExpressionOperationException("JDOHelper.getObjectId", expr);
    }
}