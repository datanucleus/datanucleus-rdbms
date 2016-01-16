/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.store.rdbms.sql.expression.TypeConverterExpression;
import org.datanucleus.store.rdbms.sql.method.AbstractSQLMethod;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {localdateExpr}.getMonthValue().
 * Returns a NumericExpression that equates to <pre>MONTH(dateExpr)</pre>
 */
public class LocalDateGetMonthValueMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List args)
    {
        if (!(expr instanceof TypeConverterExpression) || !(((TypeConverterExpression)expr).getDelegate() instanceof TemporalExpression))
        {
            throw new NucleusException(Localiser.msg("060001", "getMonth()", expr));
        }

        List funcArgs = new ArrayList();
        funcArgs.add(expr);
        return new NumericExpression(stmt, getMappingForClass(int.class), "MONTH", funcArgs);
    }
}
