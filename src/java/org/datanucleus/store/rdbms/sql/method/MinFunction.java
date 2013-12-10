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

/**
 * Expression handler to invoke the SQL MIN aggregation function.
 * For use in evaluating MIN({expr}) where the RDBMS supports this function.
 * Returns an Expression "MIN({orderableExpr})", where the orderable expression is either numeric or temporal
 */
public class MinFunction extends SimpleOrderableAggregateMethod
{
    protected String getFunctionName()
    {
        return "MIN";
    }
}