/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.table.ClassTable;
import org.datanucleus.flush.FlushNonReferential;
import org.datanucleus.flush.FlushOrdered;
import org.datanucleus.flush.OperationQueue;

/**
 * Flush process extending the core "ordered flush" to catch particular situations present
 * in a referential datastore and attempt to optimise them.
 */
public class FlushReferential extends FlushOrdered
{
    /* (non-Javadoc)
     * @see org.datanucleus.FlushOrdered#execute(org.datanucleus.ExecutionContext, java.util.Collection, java.util.Collection, org.datanucleus.flush.OperationQueue)
     */
    @Override
    public List<NucleusOptimisticException> execute(ExecutionContext ec, Collection<ObjectProvider> primarySMs, Collection<ObjectProvider> secondarySMs, OperationQueue smQueue)
    {
        List<NucleusOptimisticException> flushExcps = null;

        // Phase 1 : Find all objects that have no relations or external FKs and process first
        Set<ObjectProvider> unrelatedSMs = null;
        if (primarySMs != null)
        {
            Iterator<ObjectProvider> opIter = primarySMs.iterator();
            while (opIter.hasNext())
            {
                ObjectProvider sm = opIter.next();
                if (!sm.isEmbedded() && isClassSuitableForBatching(ec, sm.getClassMetaData()))
                {
                    if (unrelatedSMs == null)
                    {
                        unrelatedSMs = new HashSet<>();
                    }
                    unrelatedSMs.add(sm);
                    opIter.remove();
                }
            }
        }
        if (secondarySMs != null)
        {
            Iterator<ObjectProvider> smIter = secondarySMs.iterator();
            while (smIter.hasNext())
            {
                ObjectProvider sm = smIter.next();
                if (!sm.isEmbedded() && isClassSuitableForBatching(ec, sm.getClassMetaData()))
                {
                    if (unrelatedSMs == null)
                    {
                        unrelatedSMs = new HashSet<>();
                    }
                    unrelatedSMs.add(sm);
                    smIter.remove();
                }
            }
        }
        if (unrelatedSMs != null)
        {
            // Process DELETEs, then INSERTs, then UPDATEs
            FlushNonReferential groupedFlush = new FlushNonReferential();
            flushExcps = groupedFlush.flushDeleteInsertUpdateGrouped(unrelatedSMs, ec);
        }

        // Phase 2 : Fallback to FlushOrdered handling for remaining objects
        List<NucleusOptimisticException> excps = super.execute(ec, primarySMs, secondarySMs, smQueue);

        // Return any exceptions
        if (excps != null)
        {
            if (flushExcps == null)
            {
                flushExcps = excps;
            }
            else
            {
                flushExcps.addAll(excps);
            }
        }
        return flushExcps;
    }

    private boolean isClassSuitableForBatching(ExecutionContext ec, AbstractClassMetaData cmd)
    {
        if (cmd.hasRelations(ec.getClassLoaderResolver()))
        {
            return false;
        }

        RDBMSStoreManager storeMgr = (RDBMSStoreManager) ec.getStoreManager();
        ClassTable table = (ClassTable)storeMgr.getDatastoreClass(cmd.getFullClassName(), ec.getClassLoaderResolver());
        while (true)
        {
            if (!isTableSuitableForBatching(table))
            {
                return false;
            }

            table = (ClassTable) table.getSuperDatastoreClass();
            if (table == null)
            {
                // No more tables for this class
                break;
            }
        }
        return true;
    }

    private boolean isTableSuitableForBatching(ClassTable table)
    {
        if (table.hasExternalFkMappings())
        {
            return false;
        }
        else if (table.isObjectIdDatastoreAttributed())
        {
            return false;
        }
        return true;
    }
}