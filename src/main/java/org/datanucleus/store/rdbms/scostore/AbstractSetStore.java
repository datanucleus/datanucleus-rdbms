/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved.
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
2002 Mike Martin (TJDO)
2003 Andy Jefferson - coding standards
2003 Andy Jefferson - changed to use Logger
2004 Andy Jefferson - moved statements from subclasses to this class.
2005 Andy Jefferson - added embedded PC element capability
2005 Andy Jefferson - added dependent-element when removed from collection
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.types.scostore.SetStore;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Abstract representation of the backing store for a Set/Collection.
 * Can be used for a join table set, or a map key set.
 */
public abstract class AbstractSetStore<E> extends AbstractCollectionStore<E> implements SetStore<E>
{
    /**
     * Constructor.
     * @param storeMgr Manager for the store
     * @param clr The ClassLoaderResolver
     */
    protected AbstractSetStore(RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
    }

    @Override
    public abstract Iterator<E> iterator(DNStateManager sm);

    @Override
    public boolean remove(DNStateManager sm, Object element, int size, boolean allowDependentField)
    {
        if (!validateElementForReading(sm, element))
        {
            NucleusLogger.DATASTORE.debug("Attempt to remove element=" + StringUtils.toJVMIDString(element) + " but doesn't exist in this Set.");
            return false;
        }

        Object elementToRemove = element;
        ExecutionContext ec = sm.getExecutionContext();
        if (ec.getApiAdapter().isDetached(element))
        {
            // Element passed in is detached so find attached version (DON'T attach this object)
            elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false, element.getClass().getName());
        }

        // Remove the element
        boolean modified = false;
        String removeStmt = getRemoveStmt(elementToRemove);
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, removeStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    jdbcPosition = BackingStoreHelper.populateOwnerInStatement(sm, ec, ps, jdbcPosition, this);
                    jdbcPosition = BackingStoreHelper.populateElementForWhereClauseInStatement(ec, ps, elementToRemove, jdbcPosition, elementMapping);
                    if (relationDiscriminatorMapping != null)
                    {
                        jdbcPosition = BackingStoreHelper.populateRelationDiscriminatorInStatement(ec, ps, jdbcPosition, this);
                    }

                    int[] rowsDeleted = sqlControl.executeStatementUpdate(ec, mconn, removeStmt, ps, true);
                    modified = (rowsDeleted[0] == 1);
                }
                finally
                {
                    sqlControl.closeStatement(mconn, ps);
                }
            }
            finally
            {
                mconn.release();
            }
        }
        catch (SQLException e)
        {
            String msg = Localiser.msg("056012",removeStmt);
            NucleusLogger.DATASTORE.error(msg, e);
            throw new NucleusDataStoreException(msg, e);
        }

        CollectionMetaData collmd = ownerMemberMetaData.getCollection();
        boolean dependent = collmd.isDependentElement();
        if (ownerMemberMetaData.isCascadeRemoveOrphans())
        {
            dependent = true;
        }
        if (allowDependentField && dependent && !collmd.isEmbeddedElement())
        {
            // Delete the element if it is dependent
            sm.getExecutionContext().deleteObjectInternal(elementToRemove);
        }

        return modified;
    }
}