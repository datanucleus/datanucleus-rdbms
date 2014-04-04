/**********************************************************************
Copyright (c) 2014 Renato Garcia and others. All rights reserved.
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

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.AggregateNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;

/**
 * Some databases will use the same type for the return value as the argument, which can be an issue
 * when averaging on integral types since it will drop the decimals.
 * This class will convert the arg to a CAST( <arg> AS double)
 */
public class AvgWithCastFunction extends AvgFunction
{
    @Override
    protected SQLExpression getAggregateExpression(List args, JavaTypeMapping m)
    {
        Class argType = ((SQLExpression) args.get(0)).getJavaTypeMapping().getJavaType();

        List<SQLExpression> checkedArgs = null;

        // Only add the CAST if the argument is a non-floating point
        if (!argType.equals(Double.class)
                && !argType.equals(Float.class))
        {
            checkedArgs = new ArrayList<>();
            checkedArgs.add(new StringExpression(stmt, m, "CAST", args, asList("double")));
        }
        else
        {
            checkedArgs = args;
        }

        return new AggregateNumericExpression(stmt, m, getFunctionName(), checkedArgs);
    }
}
