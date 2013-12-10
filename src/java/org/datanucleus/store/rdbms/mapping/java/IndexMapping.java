/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved.
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
    Andy Jefferson - removed getUpdateInputParameter,getInsertionInputParameter
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

/**
 * Mapping for Index Columns.
 * This class is for internal use only. It should not be used in user mappings nor extended. 
 */
public final class IndexMapping extends SingleFieldMapping
{
    /**
     * Accessor for whether to include this column in any fetch statement
     * @return Whether to include the column when fetching.
     */
    public boolean includeInFetchStatement()
    {
        return false;
    }

    /**
     * Accessor for the type represented here, returning the class itself
     * @return This class.
     */
    public Class getJavaType()
    {
        return Integer.class;
    }
}