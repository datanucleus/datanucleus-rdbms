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
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * This generator utilises datastore sequences. 
 * It uses a statement like <pre>"select {sequence}.nextval from dual"</pre> to get the next value in the
 * sequence. It is datastore-dependent since there is no RDBMS-independent statement.
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
 * <LI><U>key-database-cache-size</U> - specifies how many sequence numbers are to be
 * preallocated and stored in memory for faster access</LI>
 * </UL>
 */
public final class SequenceGenerator extends AbstractRDBMSGenerator<Long>
{
    /** Name of the sequence that we are creating values for */
    protected String sequenceName = null;

    /**
     * Constructor.
     * @param name Symbolic name for the generator
     * @param props Properties controlling the behaviour of the generator
     */
    public SequenceGenerator(String name, Properties props)
    {
        super(name, props);
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

            String stmt = dba.getSequenceNextStmt(getSequenceName());
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
     * Accessor for the sequence name to use (fully qualified with catalog/schema).
     * @return The sequence name
     */
    protected String getSequenceName()
    {
        if (sequenceName == null)
        {
            // Set the name of the sequence (including catalog/schema as required)
            String inputSeqCatalogName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_CATALOG);
            if (inputSeqCatalogName == null)
            {
                inputSeqCatalogName = properties.getProperty(ValueGenerator.PROPERTY_CATALOG_NAME);
            }
            String inputSeqSchemaName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_SCHEMA);
            if (inputSeqSchemaName == null)
            {
                inputSeqSchemaName = properties.getProperty(ValueGenerator.PROPERTY_SCHEMA_NAME);
            }
            String inputSeqName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCE_NAME);

            RDBMSStoreManager srm = (RDBMSStoreManager)storeMgr;
            DatastoreAdapter dba = srm.getDatastoreAdapter();
            DatastoreIdentifier identifier = srm.getIdentifierFactory().newSequenceIdentifier(inputSeqName);
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
        return sequenceName;
    }

    /**
     * Indicator for whether the generator requires its own repository.
     * This class needs a repository so returns true.
     * @return Whether a repository is required.
     */
    protected boolean requiresRepository()
    {
        return true;
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
            throw new NucleusUserException(Localiser.msg("040010", getSequenceName()));
        }

        Integer min = properties.containsKey(ValueGenerator.PROPERTY_KEY_MIN_VALUE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_MIN_VALUE)) : null;
        Integer max = properties.containsKey(ValueGenerator.PROPERTY_KEY_MAX_VALUE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_MAX_VALUE)) : null;
        Integer start = properties.containsKey(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE)) : null;
        Integer incr = properties.containsKey(ValueGenerator.PROPERTY_KEY_CACHE_SIZE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_CACHE_SIZE)) : null;
        Integer cacheSize = properties.containsKey(ValueGenerator.PROPERTY_KEY_DATABASE_CACHE_SIZE) ? Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_DATABASE_CACHE_SIZE)) : null;
        String stmt = dba.getSequenceCreateStmt(getSequenceName(), min, max, start, incr, cacheSize);
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
}