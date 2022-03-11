/**********************************************************************
 Copyright 2021 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 except in compliance with the License. You may obtain a copy of the License at

 https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under the
 License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied. See the License for the specific language governing permissions and
 limitations under the License.

 Contributors:
 2021 Yunus Durmus - Spanner support
 **********************************************************************/
package org.datanucleus.store.rdbms.sql.method;

import java.util.List;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Method for evaluating DAYOFYEAR({dateExpr}) for CloudSpanner.
 * Returns a NumericExpression that equates to <pre>extract(DAYOFYEAR FROM expr)</pre>
 */
public class TemporalDayOfYearMethod extends TemporalBaseMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        SQLExpression invokedExpr = getInvokedExpression(expr, args, "DAYOFYEAR");

        invokedExpr.toSQLText().prepend("DAYOFYEAR FROM ");
        return new NumericExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(int.class, true), "EXTRACT", List.of(invokedExpr));
    }
}
