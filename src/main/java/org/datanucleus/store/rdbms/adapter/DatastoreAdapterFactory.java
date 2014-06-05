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
2006 Thomas Marti - Implemented autodetect mechanism with priorities 
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Factory of RDBMS datastore adapters. 
 * Acts as a registry of adapters to RDBMS that can be selected for use. 
 * Uses a singleton pattern, and the factory can be accessed using the getInstance() method.
 * <H3>Autodetection</H3>
 * Provides a level of autodetection of the adapter to use for a particular RDBMS.
 * It uses the DatabaseMetaData to extract the "product name" and matches
 * this against a series of "adapter aliases" that we define internally or that can 
 * be contributed by plugins that extend the <code>org.datanucleus.store.rdbms.datastoreadapter</code> 
 * extension point. These extension can define a priority, so if multiple adapter match for the 
 * given database connection the one with the highest priority is chosen.
 * <H3>Specification of adapter class</H3>
 * You can call {@link #getDatastoreAdapter(ClassLoaderResolver, Connection, String, PluginManager)}
 * passing the adapter class name directly if you know which you want to use. This allows for 
 * user-defined database adapters.
 */
public class DatastoreAdapterFactory
{
    /**
     * Accessor for the RDBMSAdapterFactory.
     * @return The manager of type information
     */
    public static DatastoreAdapterFactory getInstance()
    {
        return new DatastoreAdapterFactory();
    }

    /**
     * Protected constructor to prevent outside instantiation
     */
    protected DatastoreAdapterFactory()
    {
    }

    /**
     * Accessor for an adapter, given a Connection to the datastore.
     * @param clr ClassLoader resolver for resolving the adapter class
     * @param conn The Connection
     * @param adapterClassName Name of the class of the database adapter to use
     * @param pluginMgr the Plug-in manager
     * @return The database adapter for this connection.
     * @throws SQLException Thrown if a DB error occurs.
     */
    public DatastoreAdapter getDatastoreAdapter(ClassLoaderResolver clr, Connection conn,  String adapterClassName, PluginManager pluginMgr)
    throws SQLException
    {
        DatastoreAdapter adapter = null;
        DatabaseMetaData metadata = conn.getMetaData();

        // Get a new adapter
        adapter = getNewDatastoreAdapter(clr, metadata, adapterClassName, pluginMgr);
        if (adapter == null)
        {
            // Nothing suitable found so warn the user and continue with the generic adapter
            NucleusLogger.DATASTORE.warn(Localiser.msg("051000"));
            adapter = new BaseDatastoreAdapter(metadata);
        }

        return adapter;
    }

    /**
     * Accessor for the adapter for a specified datastore product.
     * @param clr ClassLoader resolver for resolving the adapter class
     * @param metadata Database MetaData for the RDBMS
     * @param adapterClassName Name of the class of the database adapter (null implies use autodetect)
     * @param pluginMgr the Plug-in manager
     * @return Instance of the database adapter
     */
    protected DatastoreAdapter getNewDatastoreAdapter(ClassLoaderResolver clr, DatabaseMetaData metadata, String adapterClassName, PluginManager pluginMgr)
    {
        if (metadata == null)
        {
            return null;
        }

        String productName = null;
        if (adapterClassName == null)
        {
            // No adapter specified, so use "autodetection" based on the metadata to find the most suitable
            try
            {
                productName = metadata.getDatabaseProductName();
                if (productName == null)
                {
                    NucleusLogger.DATASTORE.error(Localiser.msg("051024"));
                    return null;
                }
            }
            catch (SQLException sqe)
            {
                NucleusLogger.DATASTORE.error(Localiser.msg("051025", sqe));
                return null;
            }
        }

        // Instantiate the adapter class
        final Object adapter_obj;
        try
        {
            
            Class adapterClass = getAdapterClass(pluginMgr, adapterClassName, productName, clr);
            if (adapterClass==null)
            {
            	return null;
            }
            final Object[] ctr_args = new Object[]{metadata};
            final Class[] ctr_args_classes = new Class[]{DatabaseMetaData.class};

            // Create an instance of the datastore adapter
            final Constructor ctr = adapterClass.getConstructor(ctr_args_classes);
            try
            {
                adapter_obj = ctr.newInstance(ctr_args);
            }
            catch (InvocationTargetException ite)
            {
                if (ite.getTargetException() != null && ite.getTargetException() instanceof NucleusDataStoreException)
                {
                    throw (NucleusDataStoreException) ite.getTargetException();
                }
                return null;
            }
            catch (Exception e)
            {
                NucleusLogger.DATASTORE.error(Localiser.msg("051026", adapterClassName, e));
                return null;
            }
        }
        catch (ClassNotResolvedException ex)
        {
            NucleusLogger.DATASTORE.error(Localiser.msg("051026", adapterClassName, ex));
            return null;
        }
        catch (NoSuchMethodException nsme)
        {
            NucleusLogger.DATASTORE.error(Localiser.msg("051026", adapterClassName, nsme));
            return null;
        }

        return (DatastoreAdapter) adapter_obj;
    }

    /**
     * Accessor for the adapter class for a specified datastore product.
     * @param pluginMgr the Plug-in manager
     * @param adapterClassName Name of the class of the database adapter (null implies use autodetect)
     * @param productName the database product name
     * @param clr ClassLoader resolver for resolving the adapter class
     * @return the adapter class 
     */
    protected Class getAdapterClass(PluginManager pluginMgr, String adapterClassName, String productName, ClassLoaderResolver clr)
    {
        ConfigurationElement[] elems = pluginMgr.getConfigurationElementsForExtension("org.datanucleus.store.rdbms.datastoreadapter", null, null);
        if (elems != null)
        {
            for (int i=0;i<elems.length;i++)
            {
                if (adapterClassName != null)
                {
                    //if the user selected adapter is defined by one of the plug-ins use the classloader of the plug-in
                    if (elems[i].getAttribute("class-name").equals(adapterClassName))
                    {
                        return pluginMgr.loadClass(elems[i].getExtension().getPlugin().getSymbolicName(), elems[i].getAttribute("class-name"));
                    }
                }
                else
                {
                    String vendorId = elems[i].getAttribute("vendor-id");
                    if (productName.toLowerCase().indexOf(vendorId.toLowerCase()) >= 0)
                    {
                        return pluginMgr.loadClass(elems[i].getExtension().getPlugin().getSymbolicName(), elems[i].getAttribute("class-name"));
                    }
                }
            }
        }

        if (adapterClassName != null)
        {
            //if the user selected adapter was not found in one of the plug-ins, load from the ClassLoaderResolver
            return clr.classForName(adapterClassName, false);
        }

        return null;
    }
}