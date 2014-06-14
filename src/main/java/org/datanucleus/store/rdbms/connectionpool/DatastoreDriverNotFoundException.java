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
 * Exception thrown when a datastore driver class (e.g JDBC driver) is not found.
 */
public class DatastoreDriverNotFoundException extends NucleusException
{
    private static final long serialVersionUID = 2004483035679438362L;

    /**
     * Constructor.
     * @param driverClassName Class name for the datastore driver
     */
    public DatastoreDriverNotFoundException(String driverClassName)
    {
        super(Localiser.msg("047000", driverClassName));
    }
}