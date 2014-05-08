/**********************************************************************
Copyright (c) 2003 Andy Jefferson and others. All rights reserved. 
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
2004 Andy Jefferson - added type, version
2004 Andy Jefferson - changed to use StoreData. Added owner
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.autostart;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.autostart.AbstractAutoStartMechanism;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.exceptions.DatastoreInitialisationException;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.datanucleus.transaction.TransactionIsolation;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Implementation of an Auto-Start Mechanism for DataNucleus. 
 * This implementation stores the classes supported in a table in the datastore. 
 * It is initialised and read at startup, and is continually updated during the lifetime of the 
 * calling application.
 */
public class SchemaAutoStarter extends AbstractAutoStartMechanism
{
    private static final Localiser LOCALISER_RDBMS = Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    protected SchemaTable schemaTable = null;

    protected RDBMSStoreManager storeMgr = null;

    protected ManagedConnection mconn;

    /**
     * Constructor.
     * @param store_mgr The RDBMSManager managing the store that we are auto-starting.
     * @param clr The ClassLoaderResolver
     */
    public SchemaAutoStarter(StoreManager store_mgr, ClassLoaderResolver clr)
    {
        super();

        storeMgr = (RDBMSStoreManager)store_mgr;

        // Create the auto-start table in the datastore, using any user-specified name
        String tableName = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_SCHEMA_TABLE_NAME);
        schemaTable = new SchemaTable(storeMgr, tableName);
        schemaTable.initialize(clr);

        // We need to autocreate the table and validate it being correct.
        ManagedConnection mconn = storeMgr.getConnection(TransactionIsolation.NONE);
        Connection conn = (Connection) mconn.getConnection();
        try
        {
            // Create the table if it doesn't exist, and validate it
            schemaTable.exists(conn, true);
            if (storeMgr.getDdlWriter() != null)
            {
                // when DDL is only written to a file instead of being sent to DB, validating it
                // will throw a MissingTableException
                try
                {
                    schemaTable.validate(conn, true, false, null);
                }
                catch (MissingTableException mte)
                {
                    // if table had not existed, the DDL for creating it has been written above in
                    // schemaTable.checkExists(conn,true),
                    // any other validation exception will be handled below
                }
            }
            else
            {
                schemaTable.validate(conn, true, false, null);
            }
        }
        catch (Exception e)
        {
            // Validation error - table is different to what we expect
            NucleusLogger.DATASTORE_SCHEMA.error(LOCALISER_RDBMS.msg("049001",storeMgr.getSchemaName(),e));

            try
            {
                if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER_RDBMS.msg("049002",this.schemaTable.toString()));
                }

                try
                {
                    // Drop the table in case it had incorrect structure before
                    schemaTable.drop(conn);
                }
                catch (SQLException sqe)
                {
                    // Do nothing. Maybe the table didn't exist in the first place
                }

                // Create the table
                schemaTable.exists(conn, true);
                schemaTable.validate(conn, true, false, null);
            }
            catch (Exception e2)
            {
                NucleusLogger.DATASTORE_SCHEMA.error(LOCALISER_RDBMS.msg("049001",storeMgr.getSchemaName(),e2));
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Accessor for the data for the classes supported.
     * @return Collection of classes supported (StoreData). Collection of StoreData elements
     * @throws DatastoreInitialisationException if an error occurs in datastore communication
     */
    public Collection getAllClassData()
    throws DatastoreInitialisationException
    {
        try
        {
            assertIsOpen();
            Collection data=null;
            try
            {
                data = schemaTable.getAllClasses(mconn);
            }
            catch (SQLException sqe2)
            {
                NucleusLogger.DATASTORE_SCHEMA.error(LOCALISER_RDBMS.msg("049000",sqe2));
                //TODO in case of exception should we throw a Fatal Exception???
            }
            return data;
        }
        catch (Exception e)
        {
            throw new DatastoreInitialisationException(LOCALISER_RDBMS.msg("049010",e),e);
        }
    }

    /**
     * Assert that the mechanism is open from writings
     */
    private void assertIsOpen()
    {
        if (mconn == null)
        {
            throw new NucleusException(LOCALISER_RDBMS.msg("049008")).setFatal();
        }
    }

    /**
     * Assert that the mechanism is closed from writing.
     */
    private void assertIsClosed()
    {
        if (mconn != null)
        {
            throw new NucleusException(LOCALISER_RDBMS.msg("049009")).setFatal();
        }
    }
    
    /**
     * Starts a transaction for writing (add/delete) classes to the auto start mechanism 
     */
    public void open()
    {
        assertIsClosed();
        mconn = storeMgr.getConnection(TransactionIsolation.NONE);
    }
    
    /**
     * Closes a transaction for writing (add/delete) classes to the auto start mechanism 
     */
    public void close()
    {
        assertIsOpen();
        try
        {
            mconn.release();
            mconn = null;
        }
        catch (NucleusException sqe2)
        {
            NucleusLogger.DATASTORE_SCHEMA.error(LOCALISER_RDBMS.msg("050005",sqe2));
        }
    }    
    
    /**
     * Whether it's open for writing (add/delete) classes to the auto start mechanism
     * @return whether this is open for writing 
     */
    public boolean isOpen()
    {
        return mconn != null;
    }
    
    /**
     * Method to add a class to the supported list.
     * @param data Data for the class to add.
     **/
    public void addClass(StoreData data)
    {
        RDBMSStoreData tableData = (RDBMSStoreData)data; // We only support MappedStoreData
        assertIsOpen();
        try
        {
            schemaTable.addClass(tableData, mconn);
        }
        catch (SQLException sqe2)
        {
            String msg = LOCALISER_RDBMS.msg("049003", data.getName(), sqe2);
            NucleusLogger.DATASTORE_SCHEMA.error(msg);
            throw new NucleusDataStoreException(msg, sqe2);
        }
    }

    /**
     * Method to drop support for a class.
     * @param class_name The class 
     */
    public void deleteClass(String class_name)
    {
        assertIsOpen();
        try
        {
            schemaTable.deleteClass(class_name,mconn);
        }
        catch (SQLException sqe2)
        {
            NucleusLogger.DATASTORE_SCHEMA.error(LOCALISER_RDBMS.msg("049005", class_name, sqe2));
        }
    }

    /**
     * Method to drop support for all current classes.
     */
    public void deleteAllClasses()
    {
        assertIsOpen();
        try
        {
            schemaTable.deleteAllClasses(mconn);
        }
        catch (SQLException sqe2)
        {
            NucleusLogger.DATASTORE_SCHEMA.error(LOCALISER_RDBMS.msg("049006", sqe2));
        }
    }

    /**
     * Utility to output the storage description for this mechanism.
     * @return The storage description
     */
    public String getStorageDescription()
    {
        return LOCALISER_RDBMS.msg("049007", this.schemaTable.toString());
    }
}