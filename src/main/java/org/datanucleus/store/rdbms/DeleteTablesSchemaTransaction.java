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

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.StoreDataManager;
import org.datanucleus.store.rdbms.schema.RDBMSSchemaHandler;
import org.datanucleus.store.rdbms.table.ClassTable;
import org.datanucleus.store.rdbms.table.ClassView;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.store.rdbms.table.ViewImpl;
import org.datanucleus.store.schema.StoreSchemaData;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Schema transaction for deleting all known tables/views.
 */
public class DeleteTablesSchemaTransaction extends AbstractSchemaTransaction
{
    StoreDataManager storeDataMgr = null;

    Writer writer;

    /**
     * Constructor.
     * @param rdbmsMgr Store manager
     * @param isolationLevel Connection isolation level
     * @param dataMgr StoreData manager
     */
    public DeleteTablesSchemaTransaction(RDBMSStoreManager rdbmsMgr, int isolationLevel, StoreDataManager dataMgr)
    {
        super(rdbmsMgr, isolationLevel);
        this.storeDataMgr = dataMgr; // StoreDataManager used by the RDBMSManager
    }

    public void setWriter(Writer writer)
    {
        this.writer = writer;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.AbstractSchemaTransaction#run(org.datanucleus.ClassLoaderResolver)
     */
    protected void run(ClassLoaderResolver clr)
    throws SQLException
    {
        synchronized (rdbmsMgr)
        {
            boolean success = true;
            try
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("050045", rdbmsMgr.getDefaultCatalogName(), rdbmsMgr.getDefaultSchemaName()));

                // Create definition of tables/views to drop
                Set<Table> tables = new HashSet<>();
                Set<Table> views = new HashSet<>();
                for (Iterator i = storeDataMgr.getManagedStoreData().iterator(); i.hasNext();)
                {
                    RDBMSStoreData data = (RDBMSStoreData) i.next();
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("050046",data.getName()));
                    }

                    // If the class has a table/view to remove, add it to the list
                    if (data.hasTable())
                    {
                        if (data.mapsToView())
                        {
                            views.add(data.getTable());
                        }
                        else
                        {
                            tables.add(data.getTable());
                        }
                    }
                }

                // Remove views
                for (Table tblToDrop : views)
                {
                    ViewImpl view = (ViewImpl)tblToDrop;
                    if (writer != null)
                    {
                        try
                        {
                            if (view instanceof ClassView)
                            {
                                writer.write("-- ClassView " + view.toString() + " for classes " + StringUtils.objectArrayToString(((ClassView)view).getManagedClasses()) + "\n");
                            }
                        }
                        catch (IOException ioe)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("error writing DDL into file", ioe);
                        }
                    }

                    // Drop view if exists in the datastore
                    StoreSchemaData info = rdbmsMgr.getSchemaHandler().getSchemaData(getCurrentConnection(), RDBMSSchemaHandler.TYPE_COLUMNS, new Object[] {view});
                    if (info != null)
                    {
                        view.drop(getCurrentConnection());
                    }
                }

                // Remove table constraints
                Map<TableImpl, Boolean> schemaExistsForTableMap = new HashMap();
                for (Table tblToDrop : tables)
                {
                    TableImpl tbl = (TableImpl)tblToDrop;
                    if (writer != null)
                    {
                        try
                        {
                            if (tbl instanceof ClassTable)
                            {
                                writer.write("-- Constraints for ClassTable " + tbl.toString() + " for classes " + StringUtils.objectArrayToString(((ClassTable)tbl).getManagedClasses()) + "\n");
                            }
                            else if (tbl instanceof JoinTable)
                            {
                                writer.write("-- Constraints for JoinTable " + tbl.toString() + " for join relationship\n");
                            }
                        }
                        catch (IOException ioe)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("error writing DDL into file", ioe);
                        }
                    }

                    // Drop constraints if exists in the datastore
                    boolean exists = false;
                    try
                    {
                        // Check table type as way of detecting existence
                        String tableType = ((RDBMSSchemaHandler)rdbmsMgr.getSchemaHandler()).getTableType(getCurrentConnection(), tbl);
                        if (tableType != null)
                        {
                            exists = true;
                        }
                    }
                    catch (Exception e)
                    {
                        exists = false;
                    }

                    schemaExistsForTableMap.put(tbl, exists);
                    if (exists)
                    {
                        tbl.dropConstraints(getCurrentConnection());
                    }
                }

                // Remove tables
                for (Table tblToDrop : tables)
                {
                    TableImpl tbl = (TableImpl)tblToDrop;
                    if (writer != null)
                    {
                        try
                        {
                            if (tbl instanceof ClassTable)
                            {
                                writer.write("-- ClassTable " + tbl.toString() + " for classes " + StringUtils.objectArrayToString(((ClassTable)tbl).getManagedClasses()) + "\n");
                            }
                            else if (tbl instanceof JoinTable)
                            {
                                writer.write("-- JoinTable " + tbl.toString() + " for join relationship\n");
                            }
                        }
                        catch (IOException ioe)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("error writing DDL into file", ioe);
                        }
                    }

                    // Drop table if exists in the datastore
                    Boolean schemaExists = schemaExistsForTableMap.get(tbl);
                    if (schemaExists != null && schemaExists == Boolean.TRUE)
                    {
                        tbl.drop(getCurrentConnection());
                    }
                }
            }
            catch (Exception e)
            {
                success = false;
                String errorMsg = Localiser.msg("050047", e);
                NucleusLogger.DATASTORE_SCHEMA.error(errorMsg);
                throw new NucleusUserException(errorMsg, e);
            }
            if (!success)
            {
                throw new NucleusException("DeleteTables operation failed");
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.AbstractSchemaTransaction#toString()
     */
    public String toString()
    {
        return Localiser.msg("050045", rdbmsMgr.getDefaultCatalogName(), rdbmsMgr.getDefaultSchemaName());
    }
}