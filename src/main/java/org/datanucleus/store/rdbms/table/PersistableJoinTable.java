/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.UniqueMetaData;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a join table for the case where we have an N-1 unidirectional relation
 * stored in a join table. It's a minority interest situation, but worth inclusion.
 * The "owner" in this case is the side with the relation (the "N" side). The "related" is the other side.
 */
public class PersistableJoinTable extends JoinTable
{

    /** Mapping from the join table to the "related". This will be a PersistableMapping. */
    protected JavaTypeMapping relatedMapping;

    /**
     * Constructor.
     * @param ownerTable Table of the owner of this member.
     * @param tableName The Table SQL identifier
     * @param mmd Member meta data for the "element" member.
     * @param storeMgr Manager for the datastore.
     */
    public PersistableJoinTable(Table ownerTable, DatastoreIdentifier tableName, AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr)
    {
        super(ownerTable, tableName, mmd, storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.DatastoreContainerObject#getMemberMapping(org.datanucleus.metadata.AbstractMemberMetaData)
     */
    public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd)
    {
        return null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Table#initialize(org.datanucleus.ClassLoaderResolver)
     */
    public void initialize(ClassLoaderResolver clr)
    {
        boolean pkRequired = requiresPrimaryKey();

        // Prepare mapping creations
        final ColumnMetaData[] ownerColmd;
        final ColumnMetaData[] relatedColmd;
        final boolean hasColumnMetaData = mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0;
        if (mmd.getJoinMetaData() != null && mmd.getJoinMetaData().getColumnMetaData() != null && mmd.getJoinMetaData().getColumnMetaData().length > 0)
        {
            // Column mappings defined at this side
            // When specified at this side they use the <join> tag or
            // @Join annotation with column mappings
            ownerColmd = mmd.getJoinMetaData().getColumnMetaData();
            relatedColmd = hasColumnMetaData ? mmd.getColumnMetaData() : null;
        }
        else
        {
            // Column mappings defined not defined - we auto-create them from other side
            ownerColmd = hasColumnMetaData ? mmd.getColumnMetaData() : null;
            relatedColmd = null;
        }
        // Add owner mapping
        ownerMapping = ColumnCreator.createColumnsForJoinTables(clr.classForName(mmd.getClassName(true)), mmd,
            ownerColmd, storeMgr, this, pkRequired, false, FieldRole.ROLE_OWNER, clr, null);
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            logMapping(mmd.getFullFieldName()+".[OWNER]", ownerMapping);
        }

        // Add related mapping
        relatedMapping = ColumnCreator.createColumnsForJoinTables(mmd.getType(), mmd,
            relatedColmd, storeMgr, this, pkRequired, false, FieldRole.ROLE_PERSISTABLE_RELATION, clr, null);
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            logMapping(mmd.getFullFieldName()+".[RELATED]", relatedMapping);
        }

        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057023", this));
        }
        storeMgr.registerTableInitialized(this);
        state = TABLE_STATE_INITIALIZED;
    }

    /**
     * Accessor for the expected foreign keys for this table.
     * @param clr The ClassLoaderResolver
     * @return The expected foreign keys.
     */
    @Override
    public List<ForeignKey> getExpectedForeignKeys(ClassLoaderResolver clr)
    {
        assertIsInitialized();

        // Find the mode that we're operating in for FK addition
        boolean autoMode = false;
        if (storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE).equals("DataNucleus"))
        {
            autoMode = true;
        }

        List<ForeignKey> foreignKeys = new ArrayList();
        try
        {
            // FK from join table to owner table
            DatastoreClass referencedTable = storeMgr.getDatastoreClass(mmd.getClassName(true), clr);
            if (referencedTable != null)
            {
                // Single owner table, so add a single FK to the owner as appropriate
                // Take <foreign-key> from <join>
                ForeignKey fk = null;
                ForeignKeyMetaData fkmd = null;
                if (mmd.getJoinMetaData() != null)
                {
                    fkmd = mmd.getJoinMetaData().getForeignKeyMetaData();
                }
                if (fkmd != null || autoMode)
                {
                    fk = new ForeignKey(ownerMapping, dba, referencedTable, true);
                    fk.setForMetaData(fkmd);
                }
                if (fk != null)
                {
                    foreignKeys.add(fk);
                }
            }

            // FK from join table to related table
            referencedTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
            if (referencedTable != null)
            {
                // Take <foreign-key> from <field>
                ForeignKey fk = null;
                ForeignKeyMetaData fkmd = mmd.getForeignKeyMetaData();
                if (fkmd != null || autoMode)
                {
                    fk = new ForeignKey(relatedMapping, dba, referencedTable, true);
                    fk.setForMetaData(fkmd);
                }
                if (fk != null)
                {
                    foreignKeys.add(fk);
                }
            }
        }
        catch (NoTableManagedException e)
        {
            // expected when no table exists
        }

        return foreignKeys;
    }

    /**
     * Accessor for the indices for this table. 
     * This includes both the user-defined indices (via MetaData), and the ones required by 
     * foreign keys (required by relationships).
     * @param clr The ClassLoaderResolver
     * @return The indices
     */
    @Override
    protected Set<Index> getExpectedIndices(ClassLoaderResolver clr)
    {
        // The indices required by foreign keys (BaseTable)
        Set<Index> indices = super.getExpectedIndices(clr);
        return indices;
    }

    /**
     * Accessor for the candidate keys for this table.
     * @return The indices
     */
    @Override
    protected List<CandidateKey> getExpectedCandidateKeys()
    {
        // The indices required by foreign keys (BaseTable)
        List<CandidateKey> candidateKeys = super.getExpectedCandidateKeys();

        if (mmd.getJoinMetaData() != null && mmd.getJoinMetaData().getUniqueMetaData() != null)
        {
            // User has defined a unique key on the join table
            UniqueMetaData unimd = mmd.getJoinMetaData().getUniqueMetaData();
            if (unimd.getNumberOfColumns() > 0)
            {
                String[] columnNames = unimd.getColumnNames();
                CandidateKey uniKey = new CandidateKey(this, null);
                IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
                for (String columnName : columnNames)
                {
                    Column col = getColumn(idFactory.newColumnIdentifier(columnName));
                    if (col != null)
                    {
                        uniKey.addColumn(col);
                    }
                    else
                    {
                        throw new NucleusUserException("Unique key on join-table " + this + " has column " + columnName + " that is not found");
                    }
                }
                candidateKeys.add(uniKey);
            }
        }

        return candidateKeys;
    }

    /**
     * Accessor for the mapping of the "related" in the join table.
     * @return The column mapping for the related side.
     */
    public JavaTypeMapping getRelatedMapping()
    {
        assertIsInitialized();
        return relatedMapping;
    }
}