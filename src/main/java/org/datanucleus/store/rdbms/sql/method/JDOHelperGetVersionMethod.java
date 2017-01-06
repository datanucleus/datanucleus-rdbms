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
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.expression.IllegalExpressionOperationException;
import org.datanucleus.store.rdbms.sql.expression.NullLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ObjectExpression;
import org.datanucleus.store.rdbms.sql.expression.ObjectLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLLiteral;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;

/**
 * Expression handler to evaluate JDOHelper.getVersion({expression}).
 * Returns an ObjectLiteral, NullLiteral, NumericExpression or TemporalExpression.
 */
public class JDOHelperGetVersionMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression ignore, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0)
        {
            throw new NucleusUserException("Cannot invoke JDOHelper.getVersion without an argument");
        }

        SQLExpression expr = args.get(0);
        if (expr == null)
        {
            throw new NucleusUserException("Cannot invoke JDOHelper.getVersion on null expression");
        }
        if (expr instanceof SQLLiteral)
        {
            RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
            ApiAdapter api = storeMgr.getApiAdapter();
            Object obj = ((SQLLiteral)expr).getValue();
            if (obj == null || !api.isPersistable(obj))
            {
                return new NullLiteral(stmt, null, null, null);
            }

            Object ver = stmt.getRDBMSManager().getApiAdapter().getVersionForObject(obj);
            JavaTypeMapping m = getMappingForClass(ver.getClass());
            return new ObjectLiteral(stmt, m, ver, null);
        }
        else if (ObjectExpression.class.isAssignableFrom(expr.getClass()))
        {
            if (((ObjectExpression)expr).getJavaTypeMapping() instanceof PersistableMapping)
            {
                JavaTypeMapping mapping = ((ObjectExpression)expr).getJavaTypeMapping();
                DatastoreClass table = (DatastoreClass) expr.getSQLTable().getTable();
                if (table.getIdMapping() == mapping) // Version of candidate
                {
                    mapping = table.getSurrogateMapping(SurrogateColumnType.VERSION, true);
                    if (mapping == null)
                    {
                        throw new NucleusUserException("Cannot use JDOHelper.getVersion on object that has no version information");
                    }

                    if (table.getVersionMetaData().getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                    {
                        return new NumericExpression(stmt, expr.getSQLTable(), mapping);
                    }

                    return new TemporalExpression(stmt, expr.getSQLTable(), mapping);
                }

                throw new NucleusUserException("Dont currently support JDOHelper.getVersion(ObjectExpression) for expr=" + expr + " on table=" + expr.getSQLTable());
                // TODO Implement this
            }
            return expr;
        }

        throw new IllegalExpressionOperationException("JDOHelper.getVersion", expr);
    }
}
