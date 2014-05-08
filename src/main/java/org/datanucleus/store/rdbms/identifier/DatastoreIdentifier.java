/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.identifier;

/**
 * Representation of an datastore identifier in an ORM datastore.
 * This can be something like a table name, or column name etc.
 */
public interface DatastoreIdentifier
{
    /**
     * Provides the identifier with no quotes.
     * @return The name
     */
    String getIdentifierName();

    /**
     * Sets the catalog name
     * @param catalogName The catalog name
     */
    void setCatalogName(String catalogName);

    /**
     * Sets the schema name
     * @param schemaName The schema name
     */
    void setSchemaName(String schemaName);

    /**
     * Accessor for the catalog name
     * @return The catalog name
     */
    String getCatalogName();

    /**
     * Accessor for the schema name
     * @return The schema name
     */
    String getSchemaName();

    /**
     * Accessor for the fully-qualified name.
     * @param adapterCase Whether to return the name in adapter case (upper/lower and with quotes etc)
     * @return Fully qualified name
     */
    String getFullyQualifiedName(boolean adapterCase);

    /**
     * Method to output the name of the identifier. This will be quoted where necessary.
     * @return The identifier name
     */
    String toString();
}