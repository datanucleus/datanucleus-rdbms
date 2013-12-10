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

/**
 * Representation of an SQL Literal in a query.
 */
public interface SQLLiteral
{
    /**
     * Accessor to the literal value
     * @return the value of the literal
     */
    public Object getValue();

    /**
     * Method to set this literal as not being a parameter.
     * If the literal if not currently a parameter then does nothing.
     * Updates any underlying SQL to have the value.
     */
    public void setNotParameter();
}