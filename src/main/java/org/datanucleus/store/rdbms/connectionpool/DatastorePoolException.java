/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.connectionpool;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.util.Localiser;

/**
 * Exception thrown when encountering an error creating a data source pool.
 */
public class DatastorePoolException extends NucleusException
{
    private static final long serialVersionUID = -7936864514567835075L;

    /**
     * Constructor.
     * @param poolName Name of the connection pool
     * @param driverName Name of the driver
     * @param url URL for the datastore
     * @param nested The root exception
     */
    public DatastorePoolException(String poolName, String driverName, String url, Exception nested)
    {
        super(Localiser.msg("047002", poolName, driverName, url, nested.getMessage()), nested);
    }
}