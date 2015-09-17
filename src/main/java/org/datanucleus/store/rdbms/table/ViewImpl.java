/**********************************************************************
Copyright (c) 2002 Mike Martin and others. All rights reserved.
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
2004 Andy Jefferson - rewritten to remove many levels of inheritance
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.MissingColumnException;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.datanucleus.store.rdbms.exceptions.NotAViewException;
import org.datanucleus.store.rdbms.exceptions.PrimaryKeyColumnNotAllowedException;
import org.datanucleus.store.rdbms.exceptions.UnexpectedColumnException;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.RDBMSSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a View in a datastore (RDBMS).
 **/
public abstract class ViewImpl extends AbstractTable
{
    /**
     * Constructor, taking the table identifier.
     * @param name The identifier for the table.
     * @param storeMgr The Store Manager
     */
    public ViewImpl(DatastoreIdentifier name, RDBMSStoreManager storeMgr)
    {
        super(name, storeMgr);
    }

    /**
     * Pre-initialise. For things that must be initialised right after constructor.
     * @param clr the ClassLoaderResolver
     */
    public void preInitialize(final ClassLoaderResolver clr)
    {
        assertIsUninitialized();
    }

    /**
     * Post initialise. For things that must be set after all classes have been initialised before.
     * @param clr the ClassLoaderResolver
     */
    public void postInitialize(final ClassLoaderResolver clr)
    {
        assertIsInitialized();
    }

    /**
     * Method to validate the view in the datastore. Validates the existence of the table, and then the specifications of the Columns.
     * @param conn The JDBC Connection
     * @param validateColumnStructure Whether to validate down to column structure, or just their existence
     * @param autoCreate Whether to update the view to fix errors (not used).
     * @param autoCreateErrors Errors found during the auto-create process
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls 
     */
    public boolean validate(Connection conn, boolean validateColumnStructure, boolean autoCreate, Collection autoCreateErrors)
    throws SQLException
    {
        assertIsInitialized();

        // Check existence and validity
        RDBMSSchemaHandler handler = (RDBMSSchemaHandler)storeMgr.getSchemaHandler();
        String tableType = handler.getTableType(conn, this);
        if (tableType == null)
        {
            throw new MissingTableException(getCatalogName(), getSchemaName(), this.toString());
        }
        else if (!tableType.equals("VIEW")) // TODO Allow "MATERIALIZED VIEW" that some RDBMS support (e.g PostgreSQL)
        {
            throw new NotAViewException(this.toString(), tableType);
        }

        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("031004",this));
        }

        // Validate the column(s)
        Map<DatastoreIdentifier, Column> unvalidated = new HashMap(columnsByIdentifier);
        Iterator i = storeMgr.getColumnInfoForTable(this, conn).iterator();
        while (i.hasNext())
        {
            RDBMSColumnInfo ci = (RDBMSColumnInfo)i.next();
            DatastoreIdentifier colIdentifier = storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, ci.getColumnName());
            Column col = unvalidated.get(colIdentifier);
            if (col == null)
            {
                if (!hasColumnName(colIdentifier))
                {
                    throw new UnexpectedColumnException(this.toString(), ci.getColumnName(), this.getSchemaName(), this.getCatalogName());
                }
                // Otherwise it's a duplicate column name in the metadata and we ignore it.  Cloudscape is known to do this, although I think that's probably a bug.
            }
            else
            {
                if (validateColumnStructure)
                {
                    col.validate(ci);
                    unvalidated.remove(colIdentifier);
                }
                else
                {
                    unvalidated.remove(colIdentifier);
                }
            }
        }
        if (unvalidated.size() > 0)
        {
            throw new MissingColumnException(this, unvalidated.values());
        }

        state = TABLE_STATE_VALIDATED;
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("045000", (System.currentTimeMillis() - startTime)));
        }

        return false;
    }

    /**
     * Internal method to generate the SQL statements for dropping the view.
     * @return The List of SQL statements.
     */
    protected List getSQLDropStatements()
    {
        assertIsInitialized();

        ArrayList stmts = new ArrayList();
        stmts.add(dba.getDropViewStatement(this));

        return stmts;
    }

    /**
     * Method to add a Column to the View.
     * @param col The column
     */
    protected synchronized void addColumnInternal(Column col)
    {
        if (col.isPrimaryKey())
        {
            throw new PrimaryKeyColumnNotAllowedException(this.toString(), col.getIdentifier().toString());
        }

        super.addColumnInternal(col);
    }
}