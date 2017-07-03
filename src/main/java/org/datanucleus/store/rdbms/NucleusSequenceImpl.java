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
package org.datanucleus.store.rdbms;

import java.util.Map;
import java.util.Properties;

import org.datanucleus.ExecutionContext;
import org.datanucleus.Configuration;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.valuegenerator.ValueGenerationConnectionProvider;
import org.datanucleus.store.valuegenerator.ValueGenerationManager;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.transaction.TransactionUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Basic implementation of a DataNucleus datastore sequence for RDBMS.
 * Utilises the <b>org.datanucleus.store.valuegenerator</b> classes to generate sequence values.
 */
public class NucleusSequenceImpl extends org.datanucleus.store.NucleusSequenceImpl
{
    /**
     * Constructor.
     * @param objectMgr The ExecutionContext managing the sequence
     * @param storeMgr Manager of the store where we obtain the sequence
     * @param seqmd MetaData defining the sequence
     */
    public NucleusSequenceImpl(ExecutionContext objectMgr, RDBMSStoreManager storeMgr, SequenceMetaData seqmd)
    {
        super(objectMgr, storeMgr, seqmd);
    }

    /**
     * Method to set the value generator.
     * Uses "sequence" if the datastore supports it, otherwise "increment".
     */
    public void setGenerator()
    {
        // Allocate the ValueGenerationManager for this sequence
        String valueGeneratorName = null;
        if (((RDBMSStoreManager)storeManager).getDatastoreAdapter().supportsOption(DatastoreAdapter.SEQUENCES))
        {
            valueGeneratorName = "sequence";
        }
        else
        {
            valueGeneratorName = "increment";
        }

        // Create the controlling properties for this sequence
        Properties props = new Properties();
        Map<String, String> seqExtensions = seqMetaData.getExtensions();
        if (seqExtensions != null && seqExtensions.size() > 0)
        {
            props.putAll(seqExtensions);
        }
        props.put(ValueGenerator.PROPERTY_SEQUENCE_NAME, seqMetaData.getDatastoreSequence());
        if (seqMetaData.getAllocationSize() > 0)
        {
            props.put(ValueGenerator.PROPERTY_KEY_CACHE_SIZE, "" + seqMetaData.getAllocationSize());
        }
        if (seqMetaData.getInitialValue() > 0)
        {
            props.put(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE, "" + seqMetaData.getInitialValue());
        }

        // Get a ValueGenerationManager to create the generator
        ValueGenerationManager mgr = storeManager.getValueGenerationManager();
        ValueGenerationConnectionProvider connProvider = new ValueGenerationConnectionProvider()
            {
                ManagedConnection mconn;

                public ManagedConnection retrieveConnection()
                {
                    // Obtain a new connection
                    // Note : it may be worthwhile to use the PM's connection here however where a Sequence doesnt yet
                    // exist the connection would then be effectively dead until the end of the tx
                    // The way around this would be to find a way of checking for existence of the sequence
                    Configuration conf = ec.getNucleusContext().getConfiguration();
                    int isolationLevel = TransactionUtils.getTransactionIsolationLevelForName(conf.getStringProperty(PropertyNames.PROPERTY_VALUEGEN_TXN_ISOLATION));
                    this.mconn = ((RDBMSStoreManager)storeManager).getConnectionManager().getConnection(isolationLevel);
                    return mconn;
                }

                public void releaseConnection()
                {
                    try
                    {
                        // Release the connection
                        mconn.release();
                    }
                    catch (NucleusException e)
                    {
                        NucleusLogger.PERSISTENCE.error(Localiser.msg("017007", e));
                        throw e;
                    }
                }
            };
        Class cls = null;
        ConfigurationElement elem = ec.getNucleusContext().getPluginManager().getConfigurationElementForExtension("org.datanucleus.store_valuegenerator", 
                new String[]{"name", "datastore"}, 
                new String[] {valueGeneratorName, storeManager.getStoreManagerKey()});
        if (elem != null)
        {
            cls = ec.getNucleusContext().getPluginManager().loadClass(elem.getExtension().getPlugin().getSymbolicName(), elem.getAttribute("class-name"));
        }
        if (cls == null)
        {
            throw new NucleusException("Cannot create ValueGenerator for strategy "+valueGeneratorName);
        }
        generator = mgr.createValueGenerator(seqMetaData.getName(), cls, props, storeManager, connProvider);

        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("017003", seqMetaData.getName(), valueGeneratorName));
        }
    }
}