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
     * @see org.datanucleus.FlushOrdered#execute(org.datanucleus.ExecutionContext, java.util.List, java.util.List, org.datanucleus.flush.OperationQueue)
     */
    @Override
    public List<NucleusOptimisticException> execute(ExecutionContext ec, List<ObjectProvider> primaryOPs, 
        List<ObjectProvider> secondaryOPs, OperationQueue opQueue)
    {
        List<NucleusOptimisticException> flushExcps = null;

        // Phase 1 : Find all objects that have no relations or external FKs and process first
        Set<ObjectProvider> unrelatedOPs = null;
        if (primaryOPs != null)
        {
            Iterator<ObjectProvider> opIter = primaryOPs.iterator();
            while (opIter.hasNext())
            {
                ObjectProvider op = opIter.next();
                if (!op.isEmbedded() && isClassSuitableForBatching(ec, op.getClassMetaData()))
                {
                    if (unrelatedOPs == null)
                    {
                        unrelatedOPs = new HashSet<>();
                    }
                    unrelatedOPs.add(op);
                    opIter.remove();
                }
            }
        }
        if (secondaryOPs != null)
        {
            Iterator<ObjectProvider> opIter = secondaryOPs.iterator();
            while (opIter.hasNext())
            {
                ObjectProvider op = opIter.next();
                if (!op.isEmbedded() && isClassSuitableForBatching(ec, op.getClassMetaData()))
                {
                    if (unrelatedOPs == null)
                    {
                        unrelatedOPs = new HashSet<>();
                    }
                    unrelatedOPs.add(op);
                    opIter.remove();
                }
            }
        }
        if (unrelatedOPs != null)
        {
            // Process DELETEs, then INSERTs, then UPDATEs
            FlushNonReferential groupedFlush = new FlushNonReferential();
            flushExcps = groupedFlush.flushDeleteInsertUpdateGrouped(unrelatedOPs, ec);
        }

        // Phase 2 : Fallback to FlushOrdered handling for remaining objects
        List<NucleusOptimisticException> excps = super.execute(ec, primaryOPs, secondaryOPs, opQueue);

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