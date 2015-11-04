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
package org.datanucleus.store.rdbms.sql;

import java.util.HashSet;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.RDBMSStoreManager;

/**
 * Abstract generator of SQLStatements.
 * Based around a candidate(s) and optionally including subclasses.
 * If the candidate type has no table of its own (using "subclass-table") and there isn't a single
 * subclass with its own table then throws a NucleusException since there is no root table to select.
 * Accepts options controlling the generation of the SQL.
 */
public abstract class AbstractSelectStatementGenerator implements SelectStatementGenerator
{
    /** Manager for the datastore. */
    protected final RDBMSStoreManager storeMgr;

    /** ClassLoader resolver. */
    protected final ClassLoaderResolver clr;

    /** Parent statement. */
    protected SQLStatement parentStmt = null;

    /** Candidate type to query. */
    protected Class candidateType;

    /** Whether to include iteration through subclasses of the candidate. */
    protected final boolean includeSubclasses;

    /** Table where the candidate objects are stored. */
    protected DatastoreClass candidateTable;

    /** Alias for the candidate table in the SQL statement. */
    protected DatastoreIdentifier candidateTableAlias;

    /** Name of the table-group to use for the candidate(s) (optional, see SQLStatement). */
    protected String candidateTableGroupName = null;

    /** Join table for the case where we are selecting the join table and returning elements. */
    Table joinTable = null;

    /** Identifier for any join table (optional). */
    DatastoreIdentifier joinTableAlias = null;

    /** Mapping in join table to join to the element. */
    JavaTypeMapping joinElementMapping = null;

    /** Selected options controlling the generation of the SQL statement. */
    Set<String> options = new HashSet<String>();

    /**
     * Constructor for the case where we select the candidate table.
     * @param storeMgr Store Manager
     * @param clr ClassLoader resolver
     * @param candidateType Candidate root type
     * @param subclasses Whether to include subclasses
     * @param candidateTableAlias Alias for the candidate (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     */
    public AbstractSelectStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr,
            Class candidateType, boolean subclasses, DatastoreIdentifier candidateTableAlias,
            String candidateTableGroupName)
    {
        this.storeMgr = storeMgr;
        this.clr = clr;
        this.candidateType = candidateType;
        this.includeSubclasses = subclasses;
        this.candidateTableGroupName = candidateTableGroupName;

        String candidateClassName = candidateType.getName();
        AbstractClassMetaData acmd = 
            storeMgr.getMetaDataManager().getMetaDataForClass(candidateType, clr);

        if (!storeMgr.getMappedTypeManager().isSupportedMappedType(candidateClassName))
        {
            if (acmd == null)
            {
            	throw new NucleusUserException("Attempt to create SQL statement for type without metadata! : " + candidateType.getName());
            }
            candidateTable = storeMgr.getDatastoreClass(acmd.getFullClassName(), clr);
            if (candidateTable == null)
            {
                // Class must be using "subclass-table" (no table of its own) so find where it is
                AbstractClassMetaData[] subcmds = storeMgr.getClassesManagingTableForClass(acmd, clr);
                if (subcmds == null || subcmds.length > 1)
                {
                    throw new NucleusException("Attempt to generate SQL statement for instances of " + 
                        candidateType.getName() + 
                        " but has no table of its own and not single subclass with table so unsupported");
                }

                candidateTable = storeMgr.getDatastoreClass(subcmds[0].getFullClassName(), clr);
            }
        }
        this.candidateTableAlias = candidateTableAlias;
    }

    /**
     * Constructor for the case where we select the join table and join to the candidate table.
     * @param storeMgr Store Manager
     * @param clr ClassLoader resolver
     * @param candidateType Candidate root type
     * @param subclasses Whether to include subclasses
     * @param candidateTableAlias Alias for the candidate (optional)
     * @param candidateTableGroupName Name of the table group for the candidate(s) (optional)
     * @param joinTable Join table
     * @param joinTableAlias Alias for the join table
     * @param joinElementMapping Mapping to the candidate from the join table
     */
    public AbstractSelectStatementGenerator(RDBMSStoreManager storeMgr, ClassLoaderResolver clr,
            Class candidateType, boolean subclasses, DatastoreIdentifier candidateTableAlias, 
            String candidateTableGroupName,
            Table joinTable, DatastoreIdentifier joinTableAlias, 
            JavaTypeMapping joinElementMapping)
    {
        this(storeMgr, clr, candidateType, subclasses, candidateTableAlias, candidateTableGroupName);
        this.joinTable = joinTable;
        this.joinTableAlias = joinTableAlias;
        this.joinElementMapping = joinElementMapping;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.StatementGenerator#setOption(java.lang.String)
     */
    public SelectStatementGenerator setOption(String name)
    {
        options.add(name);
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.StatementGenerator#unsetOption(java.lang.String)
     */
    public SelectStatementGenerator unsetOption(String name)
    {
        options.remove(name);
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.StatementGenerator#hasOption(java.lang.String)
     */
    public boolean hasOption(String name)
    {
        return options.contains(name);
    }
}