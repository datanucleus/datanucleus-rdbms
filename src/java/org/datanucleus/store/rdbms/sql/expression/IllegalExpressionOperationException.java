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
package org.datanucleus.store.rdbms.sql.expression;

import org.datanucleus.exceptions.NucleusUserException;

/**
 * Exception thrown when trying to perform an illegal/unsupported operation
 * on an SQL expression.
 */
public class IllegalExpressionOperationException extends NucleusUserException
{
    /**
     * Constructor.
     * @param operation the operation. It may include a descriptive localised error message
     * @param operand the operand
     */
    public IllegalExpressionOperationException(String operation, SQLExpression operand)
    {
        super("Cannot perform operation \"" + operation + "\" on " + operand);
    }

    /**
     * Constructor.
     * @param operation the operation. It may include a descriptive error message
     * @param operand1 the left-hand operand
     * @param operand2 the right-hand operand
     */
    public IllegalExpressionOperationException(SQLExpression operand1, String operation, SQLExpression operand2)
    {
        super("Cannot perform operation \"" + operation + "\" on " + operand1 + " and " + operand2);
    }
}