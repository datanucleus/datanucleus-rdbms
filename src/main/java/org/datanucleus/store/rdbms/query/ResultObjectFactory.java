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
package org.datanucleus.store.rdbms.query;

import java.sql.ResultSet;

/**
 * An object that reads result set rows and returns corresponding object(s) from them.
 * Different queries accomplish this in different ways so a query supplies a suitable ResultObjectFactory to each QueryResult when it is executed. 
 * The QueryResult only uses it to turn ResultSet rows into objects and otherwise manages the ResultSet itself.
 * <p>
 * For example an implementation of this interface could return a single Persistent object per row (PersistentClassROF).
 * Another implementation could return all columns of the result set as separate objects.
 * </p>
 * @param <T> Type of the returned object
 */
public interface ResultObjectFactory<T>
{
    /**
     * Accessor for the JDBC ResultSet being processed.
     * @return The ResultSet
     */
    ResultSet getResultSet();

    /**
     * Instantiates object(s) from the current row of the given result set.
     * @return The object(s) for this row of the ResultSet.
     */
    T getObject();

    /**
     * Specify whether when processing the results we should ignore the L1 cache.
     * @param ignore Whether to ignore the L1 cache
     */
    void setIgnoreCache(boolean ignore);

    /**
     * Specify whether when processing the results we should just update fields that are not currently loaded.
     * @param update Whether to update all fields rather than just the non-loaded fields
     */
    void setUpdateAllFields(boolean update);
}