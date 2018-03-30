/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved. 
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
2004 Andy Jefferson - Updated to use CorrespondentColumnsMapping to match up column specs.
2004 Andy Jefferson - changed class.forName to use ClassLoaderResolver.classForName
2005 Andy Jefferson - fix for use of "subclass-table" owners.
2008 Andy Jefferson - fix interface impls used so we always get superclass(es) first
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.util.Collection;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.DuplicateColumnException;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.mapping.CorrespondentColumnsMapper;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.util.Localiser;

/**
 * Helper class to create columns.
 * Used for generating columns for join table fields.
 */
public final class ColumnCreator
{
    /**
     * Private constructor to prevent instantiation.
     */
    private ColumnCreator()
    {
        // private constructor
    }

    /**
     * Convenience method to add the column for an index mapping.
     * @param mapping The mapping
     * @param storeMgr Manager for the store
     * @param clr ClassLoaderResolver
     * @param table Table where we create the column
     * @param colmd The column MetaData
     * @param pk Whether this column is (part of) the PK.
     * @return The added column
     */
    public static Column createIndexColumn(JavaTypeMapping mapping, RDBMSStoreManager storeMgr,
            ClassLoaderResolver clr, Table table, ColumnMetaData colmd, boolean pk)
    {
        DatastoreIdentifier identifier = null;
        if (colmd != null && colmd.getName() != null)
        {
            // User defined name
            identifier = storeMgr.getIdentifierFactory().newColumnIdentifier(colmd.getName());
        }
        else
        {
            // No name so generate one
            identifier = storeMgr.getIdentifierFactory().newAdapterIndexFieldIdentifier();
        }

        Column column = table.addColumn(mapping.getType(), identifier, mapping, colmd);
        storeMgr.getMappingManager().createDatastoreMapping(mapping, column, mapping.getJavaType().getName());
        if (pk)
        {
            column.setPrimaryKey();
        }

        return column;
    }

    /**
     * Method to create the mapping for a join table for collection element, array element, map key,
     * map value. Supports non-embedded, non-serialised, and must be PC.
     * @param javaType The java type of the field
     * @param mmd Metadata for the field/property
     * @param columnMetaData MetaData defining the columns
     * @param storeMgr Store Manager
     * @param table The table to add the mapping to
     * @param primaryKey Whether this field is the PK
     * @param nullable Whether this field is to be nullable
     * @param fieldRole The role of the mapping within this field
     * @param clr ClassLoader resolver
     * @param ownerTable Table of the owner of this field (optional, for the case where the field is embedded so we can link back).
     * @return The java type mapping for this field
     */
    public static JavaTypeMapping createColumnsForJoinTables(Class javaType, AbstractMemberMetaData mmd, ColumnMetaData[] columnMetaData, RDBMSStoreManager storeMgr, Table table,
            boolean primaryKey, boolean nullable, FieldRole fieldRole, ClassLoaderResolver clr, Table ownerTable)
    {
        Class ownerType = javaType;
        if (ownerTable != null && fieldRole == FieldRole.ROLE_OWNER)
        {
            // If the ownerTable is set, then use its id mapping in preference
            ownerType = clr.classForName(ownerTable.getIdMapping().getType());
        }
        JavaTypeMapping mapping = storeMgr.getMappingManager().getMapping(ownerType, false, false, mmd.getFullFieldName());
        mapping.setTable(table);

        // TODO Remove this when PCMapping creates its own columns
        createColumnsForField(javaType, mapping, table, storeMgr, mmd, primaryKey, nullable, false, false, fieldRole, columnMetaData, clr, false, ownerTable);
        return mapping;
    }

    /**
     * Method to create the column(s) for a field in either a join table or for a reference field.
     * @param javaType The java type of the field being stored
     * @param mapping The JavaTypeMapping (if existing, otherwise created and returned by this method)
     * @param table The table to insert the columns into (join table, or primary table (if ref field))
     * @param storeMgr Manager for the store
     * @param mmd MetaData for the field (or null if a collection field)
     * @param isPrimaryKey Whether to create the columns as part of the PK
     * @param isNullable Whether the columns should be nullable
     * @param serialised Whether the field is serialised
     * @param embedded Whether the field is embedded
     * @param fieldRole The role of the field (when part of a join table)
     * @param columnMetaData MetaData for the column(s)
     * @param clr ClassLoader resolver
     * @param isReferenceField Whether this field is part of a reference field
     * @param ownerTable Table of the owner of this member (optional, for when the member is embedded)
     * @return The JavaTypeMapping for the table
     */
    public static JavaTypeMapping createColumnsForField(Class javaType, JavaTypeMapping mapping, Table table, RDBMSStoreManager storeMgr, AbstractMemberMetaData mmd,
            boolean isPrimaryKey, boolean isNullable, boolean serialised, boolean embedded,
            FieldRole fieldRole, ColumnMetaData[] columnMetaData, ClassLoaderResolver clr, boolean isReferenceField, Table ownerTable)
    {
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        if (mapping instanceof ReferenceMapping || mapping instanceof PersistableMapping)
        {
            // PC/interface/Object mapping
            JavaTypeMapping container = mapping;
            if (mapping instanceof ReferenceMapping)
            {
                // Interface/Object has child mappings for each implementation
                container = storeMgr.getMappingManager().getMapping(javaType, serialised, embedded, mmd != null ? mmd.getFullFieldName() : null);
                ((ReferenceMapping) mapping).addJavaTypeMapping(container);
            }

            // Get the table that we want our column to be a FK to. This could be the owner table, element table, key table, value table etc
            DatastoreClass destinationTable = null;
            try
            {
                destinationTable = storeMgr.getDatastoreClass(javaType.getName(), clr);
            }
            catch (NoTableManagedException ntme)
            {
                if (ownerTable != null && ownerTable instanceof DatastoreClass)
                {
                    destinationTable = (DatastoreClass)ownerTable;
                }
                else
                {
                    throw ntme;
                }
            }

            if (destinationTable == null)
            {
                // Maybe the owner hasn't got its own table (e.g "subclass-table" or "complete-table"+abstract)
                // Alternate is when we have an embedded type which itself has an embedded collection - not catered for at all currently
                AbstractClassMetaData ownerCmd = storeMgr.getMetaDataManager().getMetaDataForClass(javaType, clr);
                if (ownerCmd.getBaseAbstractClassMetaData().getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
                {
                    // COMPLETE-TABLE but abstract root, so find one of the subclasses with a table and use that for now
                    Collection<String> ownerSubclassNames = storeMgr.getSubClassesForClass(javaType.getName(), true, clr);
                    if (ownerSubclassNames != null && ownerSubclassNames.size() > 0)
                    {
                        for (String ownerSubclassName : ownerSubclassNames)
                        {
                            ownerCmd = storeMgr.getMetaDataManager().getMetaDataForClass(ownerSubclassName, clr);
                            try
                            {
                                destinationTable = storeMgr.getDatastoreClass(ownerSubclassName, clr);
                            }
                            catch (NoTableManagedException ntme)
                            {
                            }
                            if (destinationTable != null)
                            {
                                break;
                            }
                        }
                    }
                }
                else
                {
                    AbstractClassMetaData[] ownerCmds = storeMgr.getClassesManagingTableForClass(ownerCmd, clr);
                    if (ownerCmds == null || ownerCmds.length == 0)
                    {
                        throw new NucleusUserException(Localiser.msg("057023", javaType.getName())).setFatal();
                    }
                    // Use the first one since they should all have the same id column(s)
                    destinationTable = storeMgr.getDatastoreClass(ownerCmds[0].getFullClassName(), clr);
                }
            }

            if (destinationTable != null)
            {
                // Foreign-Key to the destination table ID mapping
                JavaTypeMapping m = destinationTable.getIdMapping();

                // For each column in the destination mapping, add a column here
                ColumnMetaDataContainer columnContainer = null;
                if (columnMetaData != null && columnMetaData.length > 0)
                {
                    columnContainer = (ColumnMetaDataContainer)columnMetaData[0].getParent();
                }
                CorrespondentColumnsMapper correspondentColumnsMapping = new CorrespondentColumnsMapper(columnContainer, table, columnMetaData, m, true);
                for (int i=0; i<m.getNumberOfDatastoreMappings(); i++)
                {
                    JavaTypeMapping refDatastoreMapping = storeMgr.getMappingManager().getMapping(m.getDatastoreMapping(i).getJavaTypeMapping().getJavaType());
                    ColumnMetaData colmd = correspondentColumnsMapping.getColumnMetaDataByIdentifier(m.getDatastoreMapping(i).getColumn().getIdentifier());
                    try
                    {
                        DatastoreIdentifier identifier = null;
                        if (fieldRole == FieldRole.ROLE_MAP_KEY && columnContainer == null)
                        {
                            // TODO If no column Metadata was specified then we should always go to the idFactory for Collection/Map join table columns not just for the map key
                            // Map KEY field and no metadata defined
                            if (isReferenceField)
                            {
                                // Create reference identifier
                                identifier = idFactory.newReferenceFieldIdentifier(mmd, storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(javaType, clr),
                                    m.getDatastoreMapping(i).getColumn().getIdentifier(),
                                    storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(javaType), fieldRole);
                            }
                            else
                            {
                                // Create join table identifier
                                AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                                // TODO If the mmd is an "embedded" type this can create invalid identifiers
                                // TODO Cater for more than 1 related field
                                identifier = idFactory.newJoinTableFieldIdentifier(mmd, relatedMmds != null ? relatedMmds[0] : null,
                                        m.getDatastoreMapping(i).getColumn().getIdentifier(), 
                                        storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(javaType), fieldRole);
                            }
                        }
                        else
                        {
                            if (colmd.getName() == null)
                            {
                                // User hasn't provided a name, so we use default naming
                                if (isReferenceField)
                                {
                                    // Create reference identifier
                                    identifier = idFactory.newReferenceFieldIdentifier(mmd, storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(javaType, clr),
                                        m.getDatastoreMapping(i).getColumn().getIdentifier(),
                                        storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(javaType), fieldRole);
                                }
                                else
                                {
                                    // Create join table identifier
                                    AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                                    // TODO If the mmd is an "embedded" type this can create invalid identifiers
                                    // TODO Cater for more than 1 related field
                                    identifier = idFactory.newJoinTableFieldIdentifier(mmd, relatedMmds != null ? relatedMmds[0] : null,
                                            m.getDatastoreMapping(i).getColumn().getIdentifier(), 
                                            storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(javaType), fieldRole);
                                }
                            }
                            else
                            {
                                // User defined name, so we use that.
                                identifier = idFactory.newColumnIdentifier(colmd.getName());
                            }
                        }

                        // Only add the column if not currently present
                        Column column = table.addColumn(javaType.getName(), identifier, refDatastoreMapping, colmd);
                        m.getDatastoreMapping(i).getColumn().copyConfigurationTo(column);
                        if (isPrimaryKey)
                        {
                            column.setPrimaryKey();
                        }
                        if (isNullable)
                        {
                            column.setNullable(true);
                        }

                        storeMgr.getMappingManager().createDatastoreMapping(refDatastoreMapping, column, 
                            m.getDatastoreMapping(i).getJavaTypeMapping().getJavaTypeForDatastoreMapping(i));
                    }
                    catch (DuplicateColumnException ex)
                    {
                    	throw new NucleusUserException("Cannot create column for field "+mmd.getFullFieldName()+" column metadata "+colmd,ex);
                    }

                    try
                    {
                        ((PersistableMapping) container).addJavaTypeMapping(refDatastoreMapping);
                    }
                    catch (ClassCastException e)
                    {
                        throw new NucleusUserException("Failed to create column for field "+mmd.getFullFieldName()+". Cannot cast mapping to PersistableMapping.",e);
                    }
                }
            }
        }
        else
        {
            // Non-PC mapping
            // Add column for the field
            Column column = null;
            ColumnMetaData colmd = null;
            if (columnMetaData != null && columnMetaData.length > 0)
            {
                colmd = columnMetaData[0];
            }

            DatastoreIdentifier identifier = null;
            if (colmd != null && colmd.getName() != null)
            {
                // User specified name
                identifier = idFactory.newColumnIdentifier(colmd.getName());
            }
            else
            {
                // No user-supplied name so generate one
                identifier = idFactory.newJoinTableFieldIdentifier(mmd, null, null, storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(javaType), fieldRole);
            }
            column = table.addColumn(javaType.getName(), identifier, mapping, colmd);
            storeMgr.getMappingManager().createDatastoreMapping(mapping, column, mapping.getJavaTypeForDatastoreMapping(0));

            if (isNullable)
            {
                column.setNullable(true);
            }
        }

        return mapping;
    }
}