/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.datanucleus.store.rdbms.identifier.IdentifierType;

/**
 * Provides methods for adapting SQL language elements to the DB2/AS400 database.
 *
 * @see BaseDatastoreAdapter
 */
public class DB2AS400Adapter extends DB2Adapter
{
    /**
     * Constructs a DB2/AS400 adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public DB2AS400Adapter(DatabaseMetaData metadata)
    {
        super(metadata);
    }

    /**
     * Accessor for the transaction isolation level to use during schema creation.
     * @return The transaction isolation level for schema generation process
     */
    public int getTransactionIsolationForSchemaCreation()
    {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    /**
     * Method to return the maximum length of a datastore identifier of the specified type.
     * If no limit exists then returns -1
     * @param identifierType Type of identifier (see IdentifierFactory.TABLE, etc)
     * @return The max permitted length of this type of identifier
     */
    public int getDatastoreIdentifierMaxLength(IdentifierType identifierType)
    {
        if (identifierType == IdentifierType.CANDIDATE_KEY)
        {
            return maxConstraintNameLength;
        }
        else if (identifierType == IdentifierType.FOREIGN_KEY)
        {
            return maxConstraintNameLength;
        }
        else if (identifierType == IdentifierType.INDEX)
        {
            return maxIndexNameLength;
        }
        else if (identifierType == IdentifierType.PRIMARY_KEY)
        {
            return maxConstraintNameLength;
        }
        else
        {
            return super.getDatastoreIdentifierMaxLength(identifierType);
        }
    }
}