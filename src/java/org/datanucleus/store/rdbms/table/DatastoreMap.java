/**********************************************************************
Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.table;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;

/**
 * Class representing a map mapped in a datastore (join table).
 */
public interface DatastoreMap extends Table
{
    /**
     * Accessor for the "owner" mapping for the container.
     * @return The mapping for the owner.
     */
    JavaTypeMapping getOwnerMapping();

    /**
     * Accessor for the "key" mapping for the map.
     * @return The mapping for the key.
     */
    JavaTypeMapping getKeyMapping();

    /**
     * Accessor for the "value" mapping for the map.
     * @return The mapping for the value.
     */
    JavaTypeMapping getValueMapping();
}