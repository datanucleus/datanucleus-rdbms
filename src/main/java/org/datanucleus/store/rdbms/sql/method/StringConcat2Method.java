/**********************************************************************
Copyright (c) 2013 Daniel Dai and others. All rights reserved.
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
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.method.AbstractSQLMethod;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {strExpr}.concat(strExpr1s).
 * Returns a StrignExpression that equates to
 * <ul>
 * <li><pre>concat(strExpr, strExpr1)</pre>.</li>
 * </ul>
 */
public class StringConcat2Method extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() != 1)
        {
            throw new NucleusException(Localiser.msg("060003", "concat", "StringExpression", 0,
                "StringExpression/CharacterExpression/Parameter"));
        }
            
        SQLExpression otherExpr = args.get(0);
        if (!(otherExpr instanceof StringExpression) &&
                !(otherExpr instanceof CharacterExpression) &&
                !(otherExpr instanceof ParameterLiteral))
        {
            throw new NucleusException(Localiser.msg("060003", "concat", "StringExpression", 0,
                "StringExpression/CharacterExpression/Parameter"));
        }
        
        ArrayList funcArgs = new ArrayList();
        funcArgs.add(expr);
        funcArgs.add(otherExpr);
        return new StringExpression(stmt, getMappingForClass(String.class), "CONCAT", funcArgs);
    }	
}