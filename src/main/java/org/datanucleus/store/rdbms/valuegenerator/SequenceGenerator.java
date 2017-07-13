/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved. 
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
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - changed to provide Sequence generator for all databases
2004 Andy Jefferson - removed the MetaData requirement
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.valuegenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.valuegenerator.AbstractConnectedGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * ValueGenerator utilising datastore (RDBMS) sequences. 
 * It uses a statement like <pre>"select {sequence}.nextval from dual"</pre> to get the next value in the sequence. 
 * It is datastore-dependent since there is no RDBMS-independent statement.
 * SequenceGenerator works with Longs, so clients using this generator must cast the ID to Long.
 * <P>
 * <B>Optional user properties</B>
 * <UL>
 * <LI><U>sequence-catalog-name</U> - catalog for the sequence</LI>
 * <LI><U>sequence-schema-name</U> - schema for the sequence</LI>
 * <LI><U>key-initial-value</U> - the initial value for the sequence</LI>
 * <LI><U>key-cache-size</U> - number of unique identifiers to cache</LI>
 * <LI><U>key-min-value</U> - determines the minimum value a sequence can generate</LI>
 * <LI><U>key-max-value</U> - determines the maximum value a sequence can generate</LI>
 * <LI><U>key-database-cache-size</U> - specifies how many sequence numbers are to be preallocated and stored in memory for faster access</LI>
 * </UL>
 * TODO Change structure to not override obtainGenerationBlock so we can follow the superclass process and commonise more code.
 */
public final class SequenceGenerator extends AbstractConnectedGenerator<Long>
{
    /** Connection to the datastore. */
    protected ManagedConnection connection;

    /** Flag for whether we know that the repository exists. */
    protected boolean repositoryExists = false;

    /** Name of the sequence that we are creating values for */
    protected String sequenceName = null;

    /**
     * Constructor.
     * @param storeMgr StoreManager
     * @param name Symbolic name for the generator
     * @param props Properties controlling the behaviour of the generator
     */
    public SequenceGenerator(StoreManager storeMgr, String name, Properties props)
    {
        super(storeMgr, name, props);
        allocationSize = 1;
        if (properties != null)
        {
            if (properties.containsKey(ValueGenerator.PROPERTY_KEY_CACHE_SIZE))
            {
                try
                {
                    allocationSize = Integer.parseInt((String)properties.get(ValueGenerator.PROPERTY_KEY_CACHE_SIZE));
                }
                catch (Exception e)
                {
                    throw new ValueGenerationException(Localiser.msg("040006", properties.get(ValueGenerator.PROPERTY_KEY_CACHE_SIZE)));
                }
            }
            if (!properties.containsKey(ValueGenerator.PROPERTY_SEQUENCE_NAME))
            {
                throw new ValueGenerationException(Localiser.msg("040007", properties.get(ValueGenerator.PROPERTY_SEQUENCE_NAME)));
            }
        }

        // Set the name of the sequence (including catalog/schema as required)
        String inputSeqCatalogName = null;
        String inputSeqSchemaName = null;
        String inputSeqName = null;
        if (properties != null)
        {
            inputSeqCatalogName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_CATALOG);
            if (inputSeqCatalogName == null)
            {
                inputSeqCatalogName = properties.getProperty(ValueGenerator.PROPERTY_CATALOG_NAME);
            }

            inputSeqSchemaName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_SCHEMA);
            if (inputSeqSchemaName == null)
            {
                inputSeqSchemaName = properties.getProperty(ValueGenerator.PROPERTY_SCHEMA_NAME);
            }

            inputSeqName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCE_NAME);
        }

        RDBMSStoreManager rdbmsMgr = (RDBMSStoreManager)storeMgr;
        DatastoreAdapter dba = rdbmsMgr.getDatastoreAdapter();
        DatastoreIdentifier identifier = rdbmsMgr.getIdentifierFactory().newSequenceIdentifier(inputSeqName);
        if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS) && inputSeqCatalogName != null)
        {
            identifier.setCatalogName(inputSeqCatalogName);
        }
        if (dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS) && inputSeqSchemaName != null)
        {
            identifier.setSchemaName(inputSeqSchemaName);
        }
        this.sequenceName = identifier.getFullyQualifiedName(true);
    }

    /**
     * Accessor for the storage class for values generated with this generator.
     * @return Storage class (in this case Long.class)
     */
    public static Class getStorageClass()
    {
        return Long.class;
    }

    /**
     * Reserve a block of ids.
     * @param size Block size
     * @return The reserved block
     */
    protected synchronized ValueGenerationBlock<Long> reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        List oid = new ArrayList();
        RDBMSStoreManager srm = (RDBMSStoreManager)storeMgr;
        SQLController sqlControl = srm.getSQLController();
        try
        {
            // Get next available id
            DatastoreAdapter dba = srm.getDatastoreAdapter();

            String stmt = dba.getSequenceNextStmt(sequenceName);
            ps = sqlControl.getStatementForQuery(connection, stmt);
            rs = sqlControl.executeStatementQuery(null, connection, stmt, ps);
 
            Long nextId = Long.valueOf(0);
            if (rs.next())
            {
                nextId = Long.valueOf(rs.getLong(1));
                oid.add(nextId);
            }
            for (int i=1; i<size; i++)
            {
                // size must match key-increment-by otherwise it will
                // cause duplicates keys
                nextId = Long.valueOf(nextId.longValue()+1);
                oid.add(nextId);
            }
            if (NucleusLogger.VALUEGENERATION.isDebugEnabled())
            {
                NucleusLogger.VALUEGENERATION.debug(Localiser.msg("040004", "" + size));
            }
            return new ValueGenerationBlock<>(oid);
        }
        catch (SQLException e)
        {
            throw new ValueGenerationException(Localiser.msg("061001", e.getMessage()), e);
        }
        finally
        {
            try
            {
                if (rs != null)
                {
                    rs.close();
                }
                if (ps != null)
                {
                    sqlControl.closeStatement(connection, ps);
                }
            }
            catch (SQLException e)
            {
                // non-recoverable error
            }
        }
    }

    /**
     * Method to return if the repository already exists.
     * @return Whether the repository exists
     */
    protected boolean repositoryExists()
    {
        String sequenceCatalogName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_CATALOG);
        if (sequenceCatalogName == null)
        {
            sequenceCatalogName = properties.getProperty(ValueGenerator.PROPERTY_CATALOG_NAME);
        }
        if (!StringUtils.isWhitespace(sequenceCatalogName))
        {
            IdentifierFactory idFactory = ((RDBMSStoreManager)storeMgr).getIdentifierFactory();
            sequenceCatalogName = idFactory.getIdentifierInAdapterCase(sequenceCatalogName);
        }

        String sequenceSchemaName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_SCHEMA);
        if (sequenceSchemaName == null)
        {
            sequenceSchemaName = properties.getProperty(ValueGenerator.PROPERTY_SCHEMA_NAME);
        }
        if (!StringUtils.isWhitespace(sequenceSchemaName))
        {
            IdentifierFactory idFactory = ((RDBMSStoreManager)storeMgr).getIdentifierFactory();
            sequenceSchemaName = idFactory.getIdentifierInAdapterCase(sequenceSchemaName);
        }

        String seqName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCE_NAME);
        IdentifierFactory idFactory = ((RDBMSStoreManager)storeMgr).getIdentifierFactory();
        seqName = idFactory.getIdentifierInAdapterCase(seqName);
        return ((RDBMSStoreManager)storeMgr).getDatastoreAdapter().sequenceExists((Connection) connection.getConnection(), sequenceCatalogName, sequenceSchemaName, seqName);
    }

    /**
     * Method to create the sequence.
     * @return Whether it was created successfully.
     */
    protected boolean createRepository()
    {
        PreparedStatement ps = null;
        RDBMSStoreManager srm = (RDBMSStoreManager)storeMgr;
        DatastoreAdapter dba = srm.getDatastoreAdapter();
        SQLController sqlControl = srm.getSQLController();

        if (!srm.getSchemaHandler().isAutoCreateTables())
        {
            throw new NucleusUserException(Localiser.msg("040010", sequenceName));
        }

        Integer min = properties.containsKey(ValueGenerator.PROPERTY_KEY_MIN_VALUE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_MIN_VALUE)) : null;
        Integer max = properties.containsKey(ValueGenerator.PROPERTY_KEY_MAX_VALUE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_MAX_VALUE)) : null;
        Integer start = properties.containsKey(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE)) : null;
        Integer incr = properties.containsKey(ValueGenerator.PROPERTY_KEY_CACHE_SIZE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_CACHE_SIZE)) : null;
        Integer cacheSize = properties.containsKey(ValueGenerator.PROPERTY_KEY_DATABASE_CACHE_SIZE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_DATABASE_CACHE_SIZE)) : null;
        String stmt = dba.getSequenceCreateStmt(sequenceName, min, max, start, incr, cacheSize);
        try
        {
            ps = sqlControl.getStatementForUpdate(connection, stmt, false);
            sqlControl.executeStatementUpdate(null, connection, stmt, ps, true);
        }
        catch (SQLException e)
        {
            NucleusLogger.DATASTORE.error(e);
            throw new ValueGenerationException(Localiser.msg("061000",e.getMessage()) + stmt);
        }
        finally
        {
            try
            {
                if (ps != null)
                {
                    sqlControl.closeStatement(connection, ps);
                }
            }
            catch (SQLException e)
            {
                // non-recoverable error
            }           
        }
        return true;
    }

    /**
     * Get a new PoidBlock with the specified number of ids.
     * @param number The number of additional ids required
     * @return the PoidBlock
     */
    protected ValueGenerationBlock<Long> obtainGenerationBlock(int number)
    {
        ValueGenerationBlock<Long> block = null;

        // Try getting the block
        boolean repository_exists=true; // TODO Ultimately this can be removed when "repositoryExists()" is implemented
        try
        {
            connection = connectionProvider.retrieveConnection();

            if (!repositoryExists)
            {
                // Make sure the repository is present before proceeding
                repositoryExists = repositoryExists();
                if (!repositoryExists)
                {
                    createRepository();
                    repositoryExists = true;
                }
            }

            try
            {
                if (number < 0)
                {
                    block = reserveBlock();
                }
                else
                {
                    block = reserveBlock(number);
                }
            }
            catch (ValueGenerationException vge)
            {
                NucleusLogger.VALUEGENERATION.info(Localiser.msg("040003", vge.getMessage()));
                if (NucleusLogger.VALUEGENERATION.isDebugEnabled())
                {
                    NucleusLogger.VALUEGENERATION.debug("Caught exception", vge);
                }

                // attempt to obtain the block of unique identifiers is invalid
                repository_exists = false;
            }
            catch (RuntimeException ex)
            {
                NucleusLogger.VALUEGENERATION.info(Localiser.msg("040003", ex.getMessage()));
                if (NucleusLogger.VALUEGENERATION.isDebugEnabled())
                {
                    NucleusLogger.VALUEGENERATION.debug("Caught exception", ex);
                }

                // attempt to obtain the block of unique identifiers is invalid
                repository_exists = false;
            }
        }
        finally
        {
            if (connection != null)
            {
                connectionProvider.releaseConnection();
                connection = null;
            }
        }

        // If repository didn't exist, try creating it and then get block
        if (!repository_exists)
        {
            try
            {
                connection = connectionProvider.retrieveConnection();

                NucleusLogger.VALUEGENERATION.info(Localiser.msg("040005"));
                if (!createRepository())
                {
                    throw new ValueGenerationException(Localiser.msg("040002"));
                }

                if (number < 0)
                {
                    block = reserveBlock();
                }
                else
                {
                    block = reserveBlock(number);
                }
            }
            finally
            {
                connectionProvider.releaseConnection();
                connection = null;
            }
        }
        return block;
    }
}