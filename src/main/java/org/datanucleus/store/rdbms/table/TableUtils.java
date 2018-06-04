/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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
import java.util.Collection;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ForeignKeyAction;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.UniqueMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;

/**
 * Class containing a series of convenience methods for the generation of tables and constraints.
 */
public class TableUtils
{
    private TableUtils(){}
    /**
     * Convenience method to add foreign-keys for the specified reference field.
     * Adds FKs from the column(s) in this table to the ID column(s) of the PC table of the implementation type.
     * @param fieldMapping The field mapping (in this table)
     * @param mmd MetaData for this field
     * @param autoMode Whether we are in auto-create mode
     * @param storeMgr Store Manager
     * @param clr ClassLoader resolver
     * @return The foreign key(s) created
     */
    public static Collection getForeignKeysForReferenceField(JavaTypeMapping fieldMapping, 
                                                             AbstractMemberMetaData mmd,
                                                             boolean autoMode,
                                                             RDBMSStoreManager storeMgr,
                                                             ClassLoaderResolver clr)
    {
        final ReferenceMapping refMapping = (ReferenceMapping)fieldMapping;
        JavaTypeMapping[] refJavaTypeMappings = refMapping.getJavaTypeMapping();
        List fks = new ArrayList();
        for (int i=0;i<refJavaTypeMappings.length;i++)
        {
            // If the implementation is of a PC class, look to add a FK to the PC class table
            final JavaTypeMapping implMapping = refJavaTypeMappings[i];
            if (storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(implMapping.getType(), clr) != null &&
                implMapping.getNumberOfColumnMappings() > 0)
            {
                DatastoreClass referencedTable = storeMgr.getDatastoreClass(implMapping.getType(), clr);
                if (referencedTable != null)
                {
                    ForeignKeyMetaData fkmd = mmd.getForeignKeyMetaData();
                    if ((fkmd != null && fkmd.getDeleteAction() != ForeignKeyAction.NONE) || autoMode)
                    {
                        // Either has been specified by user, or using autoMode, so add FK
                        ForeignKey fk = new ForeignKey(implMapping, storeMgr.getDatastoreAdapter(), referencedTable, true);
                        fk.setForMetaData(fkmd); // Does nothing when no FK MetaData
                        fks.add(fk);
                    }
                }
            }
        }
        return fks;
    }

    /**
     * Convenience method to add a foreign key for a PC field.
     * Adds a FK from the PC column(s) in this table to the ID columns in the PC's table.
     * @param fieldMapping Mapping for the PC field
     * @param mmd MetaData for the field
     * @param autoMode Whether we are in auto-create mode
     * @param storeMgr Store Manager
     * @param clr ClassLoader resolver
     * @return The ForeignKey (if any)
     */
    public static ForeignKey getForeignKeyForPCField(JavaTypeMapping fieldMapping, 
                                                     AbstractMemberMetaData mmd,
                                                     boolean autoMode,
                                                     RDBMSStoreManager storeMgr,
                                                     ClassLoaderResolver clr)
    {
        DatastoreClass referencedTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
        if (referencedTable == null)
        {
            // PC type uses subclass-table
            AbstractClassMetaData refCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            if (refCmd != null)
            {
                AbstractClassMetaData[] refCmds = storeMgr.getClassesManagingTableForClass(refCmd, clr);
                if (refCmds != null && refCmds.length == 1)
                {
                    referencedTable = storeMgr.getDatastoreClass(refCmds[0].getFullClassName(), clr);
                }
                else
                {
                    // "subclass-table" with more than 1 subclass with table (not supported)
                }
            }
        }
        if (referencedTable != null)
        {
            ForeignKeyMetaData fkmd = mmd.getForeignKeyMetaData();
            if ((fkmd != null && (fkmd.getDeleteAction() != ForeignKeyAction.NONE || fkmd.getFkDefinitionApplies())) || autoMode)
            {
                // Either has been specified by user, or using autoMode, so add FK
                ForeignKey fk = new ForeignKey(fieldMapping, storeMgr.getDatastoreAdapter(), referencedTable, true);
                fk.setForMetaData(fkmd); // Does nothing when no FK MetaData
                if (fkmd != null && fkmd.getName() != null)
                {
                    fk.setName(fkmd.getName());
                }
                return fk;
            }
        }
        return null;
    }

    /**
     * Convenience method to create an Index for a field.
     * @param table Container for the index
     * @param imd The Index MetaData
     * @param fieldMapping Mapping for the field
     * @return The Index
     */
    public static Index getIndexForField(Table table, IndexMetaData imd, JavaTypeMapping fieldMapping)
    {
        if (fieldMapping.getNumberOfColumnMappings() == 0)
        {
            // No columns in this mapping so we can hardly index it!
            return null;
        }
        if (!table.getStoreManager().getDatastoreAdapter().validToIndexMapping(fieldMapping))
        {
            return null;
        }

        // Verify if a unique index is needed
        boolean unique = (imd == null ? false : imd.isUnique());

        Index index = new Index(table, unique, (imd != null ? imd.getExtensions() : null));

        // Set the index name if required
        if (imd != null && imd.getName() != null)
        {
            IdentifierFactory idFactory = table.getStoreManager().getIdentifierFactory();
            DatastoreIdentifier idxId = idFactory.newIdentifier(IdentifierType.INDEX, imd.getName());
            index.setName(idxId.toString());
        }

        // Field-level index so use all columns for the field
        int countFields = fieldMapping.getNumberOfColumnMappings();
        for (int j=0; j<countFields; j++)
        {
            index.addColumn(fieldMapping.getColumnMapping(j).getColumn());
        }

        return index;
    }

    /**
     * Convenience method to return the candidate key (if any) for a field.
     * @param table The table
     * @param umd The Unique MetaData
     * @param fieldMapping Mapping for the field
     * @return The Candidate Key
     */
    public static CandidateKey getCandidateKeyForField(Table table, UniqueMetaData umd, JavaTypeMapping fieldMapping)
    {
        CandidateKey ck = new CandidateKey(table, umd != null ? umd.getExtensions() : null);

        // Set the key name if required
        if (umd.getName() != null)
        {
            IdentifierFactory idFactory = table.getStoreManager().getIdentifierFactory();
            DatastoreIdentifier ckId = idFactory.newIdentifier(IdentifierType.CANDIDATE_KEY, umd.getName());
            ck.setName(ckId.toString());
        }

        // Field-level index so use all columns for the field
        int countFields = fieldMapping.getNumberOfColumnMappings();
        for (int j=0; j<countFields; j++)
        {
            ck.addColumn(fieldMapping.getColumnMapping(j).getColumn());
        }

        return ck;
    }
}