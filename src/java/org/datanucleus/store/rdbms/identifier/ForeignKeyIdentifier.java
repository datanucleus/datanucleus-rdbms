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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.identifier;


/**
 * Identifier for a FK.
 */
class ForeignKeyIdentifier extends DatastoreIdentifierImpl
{
    /**
     * Constructor for a foreign key identifier
     * @param factory Identifier factory
     * @param sqlIdentifier the sql identifier
     */    
    public ForeignKeyIdentifier(IdentifierFactory factory, String sqlIdentifier)
    {
        super(factory, sqlIdentifier);
    }
}