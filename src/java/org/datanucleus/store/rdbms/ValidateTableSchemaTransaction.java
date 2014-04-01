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
package org.datanucleus.store.rdbms;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Schema transaction to validate the specified table.
 * This is useful where we have made an update to the columns in a table and want to
 * apply the updates to the datastore.
 */
public class ValidateTableSchemaTransaction extends AbstractSchemaTransaction
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    /** Table to be validated. */
    protected TableImpl table;

    /**
     * Constructor.
     * @param rdbmsMgr Store Manager
     * @param isolationLevel Connection isolation level
     * @param table The table to validate
     */
    public ValidateTableSchemaTransaction(RDBMSStoreManager rdbmsMgr, int isolationLevel, TableImpl table)
    {
        super(rdbmsMgr, isolationLevel);
        this.table = table;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.AbstractSchemaTransaction#run(org.datanucleus.ClassLoaderResolver)
     */
    protected void run(ClassLoaderResolver clr)
    throws SQLException
    {
        synchronized (rdbmsMgr)
        {
            List autoCreateErrors = new ArrayList();
            try
            {
                table.validate(getCurrentConnection(), false, true, autoCreateErrors);
            }
            catch (Exception e)
            {
                NucleusLogger.DATASTORE_SCHEMA.error(
                    "Exception thrown during update of schema for table " + table, e);
                throw new NucleusException(
                    "Exception thrown during update of schema for table " + table, e);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.AbstractSchemaTransaction#toString()
     */
    public String toString()
    {
        return LOCALISER.msg("050048", table, rdbmsMgr.getCatalogName(), rdbmsMgr.getSchemaName());
    }
}