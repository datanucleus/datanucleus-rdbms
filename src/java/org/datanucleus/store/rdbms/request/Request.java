/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved.
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
2003 Andy Jefferson - commented
2006 Andy Jefferson - removed unused method. Moved PreparedStatement work to RDBMSManager
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.request;

import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.table.AbstractClassTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.util.Localiser;

/**
 * Base class representing a request to perform an action on the datastore.
 * All requests have 2 methods - constructor and execute. They build an SQL statement, and execute it.
 */
public abstract class Request
{
    /** Localisation of messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    protected DatastoreClass table;
    protected PrimaryKey key;

    /**
     * Constructor, taking the table to use for the request.
     * @param table The Table to use for the request.
     **/
    public Request(DatastoreClass table)
    {
        this.table = table;
        this.key = ((AbstractClassTable)table).getPrimaryKey();
    }

    /**
     * Method to execute the request - to be implemented by deriving classes.
     * @param op The ObjectProvider for the object in question. 
     */
    public abstract void execute(ObjectProvider op);
}
