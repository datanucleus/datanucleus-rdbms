/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Case expression such as
 * <pre>CASE WHEN expr1 THEN val1 WHEN expr2 THEN val2 ELSE valN END</pre>
 * where each of the "val1", "val2", ... "valN" are NumericExpressions.
 */
public class CaseNumericExpression extends NumericExpression
{
    public CaseNumericExpression(SQLExpression[] whenExprs, SQLExpression[] actionExprs, SQLExpression elseExpr)
    {
        super(whenExprs[0].getSQLStatement(), (SQLTable)null, (JavaTypeMapping)null);

        st.clearStatement();
        st.append("CASE");
        if (actionExprs == null || whenExprs.length != actionExprs.length || whenExprs.length == 0)
        {
            throw new IllegalArgumentException("CaseExpression must have equal number of WHEN and THEN expressions");
        }

        mapping = actionExprs[0].getJavaTypeMapping();
        for (int i=0;i<whenExprs.length;i++)
        {
            SQLExpression actionExpr = actionExprs[i];
            if (actionExpr instanceof ParameterLiteral)
            {
                // Swap for a NumericExpression that is a parameter
                ParameterLiteral paramLit = (ParameterLiteral)actionExpr;
                actionExpr = stmt.getSQLExpressionFactory().newLiteralParameter(stmt, stmt.getSQLExpressionFactory().getMappingForType(Number.class), 
                    paramLit.getValue(), paramLit.getParameterName());
            }
            st.append(" WHEN ").append(whenExprs[i]).append(" THEN ").append(actionExpr);
        }

        if (elseExpr != null)
        {
            SQLExpression actionExpr = elseExpr;
            if (actionExpr instanceof ParameterLiteral)
            {
                // Swap for a NumericExpression that is a parameter
                ParameterLiteral paramLit = (ParameterLiteral)actionExpr;
                actionExpr = stmt.getSQLExpressionFactory().newLiteralParameter(stmt, stmt.getSQLExpressionFactory().getMappingForType(Number.class), 
                    paramLit.getValue(), paramLit.getParameterName());
            }
            st.append(" ELSE ").append(actionExpr);
        }
        st.append(" END");
        st.encloseInParentheses();
    }
}