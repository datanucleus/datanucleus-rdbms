/**********************************************************************
Copyright (c) 2021 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.mapping.column;

import org.datanucleus.state.ObjectProvider;

/**
 * Interface implemented by any ColumnMapping that requires a post-set (insert/update) step.
 * For example, with Oracle CLOB/BLOB the INSERT will just put "EMPTY_CLOB" or "EMPTY_BLOB" and this will SELECT the column and update it.
 */
public interface ColumnMappingPostSet
{
    /**
     * Perform any post "set" processing on this column, using the provided value.
     * @param sm StateManager for object being set
     * @param value The value to use on the set
     */
    void setPostProcessing(ObjectProvider sm, Object value);
}