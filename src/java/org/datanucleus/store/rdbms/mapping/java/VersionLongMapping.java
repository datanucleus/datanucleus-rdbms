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
package org.datanucleus.store.rdbms.mapping.java;

import org.datanucleus.store.rdbms.table.Table;

/**
 * Version using a Long delegate.
 */
public final class VersionLongMapping extends VersionMapping
{
    public VersionLongMapping(Table datastoreContainer, JavaTypeMapping delegate)
    {
        super(datastoreContainer, delegate);
    }
}