/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved.
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
2004 Erik Bengtson - original version in ClassTable
2005 Andy Jefferson - changed to work from ColumnMetaData and create missing entries
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping;

import java.util.HashMap;
import java.util.Map;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.MultiMapping;
import org.datanucleus.util.Localiser;

/**
 * Class to make correspondence between columns in one side of an
 * association to the mapping at the other side. The 2 sides of the association are
 * referred to as "sideA" and "sideB". The JDO 2 metadata allows definition
 * of the correspondence using the
 * &lt;column name="{column-name}" target="{target-name}"/&gt; syntax.
 * <P>
 * This means that the column specified on sideA will be mapped to the specified "target"
 * column on sideB. If no target is provided then the first available sideB column
 * is used for the mapping. Where no columns are defined on sideA, then they will be
 * created to match those on sideB. Checks are made for consistency of the sideA data.
 * When there is insufficient ColumnMetaData on sideA then a new ColumnMetaData is added
 * to the column container.
 */
public class CorrespondentColumnsMapper
{
    /** Map of the ColumnMetaData for each column keyed by the sideB identifier name. */
    private final Map<DatastoreIdentifier, ColumnMetaData> columnMetaDataBySideBIdentifier = new HashMap();

    private final String columnsName;

    /**
     * Constructor.
     * Takes the sideB mapping and the side A definition of column metadata and matches them up as
     * defined by the user, and if not defined by the user matches them as best it can. This constructor
     * allows specification of the column metadata array directly, rather than taking what the container has
     * - is used by ColumnCreator where the user has specified multiple columns but only some of them are for
     * this field being mapped.
     * @param columnContainer Container of the columns for side A
     * @param colmds MetaData for the columns to be used
     * @param mappingSideB the mapping in the side B
     * @param updateContainer Whether to add any missing ColumnMetaData objects to the container
     */
    public CorrespondentColumnsMapper(ColumnMetaDataContainer columnContainer, ColumnMetaData[] colmds,
        JavaTypeMapping mappingSideB, boolean updateContainer)
    {
        // Go through the user-defined columns and allocate them as required
        if (columnContainer != null && colmds != null)
        {
            int noOfUserColumns = colmds.length;

            // Generate string of user-specified columns for use in diagnostics
            StringBuilder str=new StringBuilder("Columns [");
            for (int i=0;i<noOfUserColumns;i++)
            {
                str.append(colmds[i].getName());
                if (i < noOfUserColumns-1)
                {
                    str.append(", ");
                }
            }
            str.append("]");
            columnsName = str.toString();

            // Check if too many columns have been defined
            if (noOfUserColumns > mappingSideB.getNumberOfDatastoreMappings())
            {
                throw new NucleusUserException(Localiser.msg("020003", 
                    columnsName, "" + noOfUserColumns, "" + mappingSideB.getNumberOfDatastoreMappings())).setFatal();
            }

            // Retrieve sideB column names
            DatastoreIdentifier[] sideBidentifiers = new DatastoreIdentifier[mappingSideB.getNumberOfDatastoreMappings()];
            boolean[] sideButilised = new boolean[mappingSideB.getNumberOfDatastoreMappings()];
            for (int i = 0; i < mappingSideB.getNumberOfDatastoreMappings(); i++)
            {
                sideBidentifiers[i] = mappingSideB.getDatastoreMapping(i).getColumn().getIdentifier();
                sideButilised[i] = false;
            }

            JavaTypeMapping[] sideBidMappings = ((MultiMapping)mappingSideB).getJavaTypeMapping();

            // Allocate the user-defined columns using the sideB list where target column has been defined
            for (int i = 0; i < noOfUserColumns; i++)
            {
                String targetColumnName = colmds[i].getTarget();
                if (targetColumnName == null)
                {
                    // No target column, so try the field
                    String targetFieldName = colmds[i].getTargetMember();
                    if (targetFieldName != null)
                    {
                        for (int j=0;j<sideBidMappings.length;j++)
                        {
                            if (sideBidMappings[j].getMemberMetaData().getName().equals(targetFieldName))
                            {
                                targetColumnName = sideBidMappings[j].getDatastoreMapping(0).getColumn().getIdentifier().getName();
                                break;
                            }
                        }
                    }
                }

                // Find the target on sideB
                if (targetColumnName != null)
                {
                    boolean targetExists = false;
                    for (int j = 0; j < sideBidentifiers.length; j++)
                    {
                        // This allows for case incorrectness in the specified name
                        if (sideBidentifiers[j].getName().equalsIgnoreCase(targetColumnName) &&
                            !sideButilised[j])
                        {
                            putColumn(sideBidentifiers[j], colmds[i]);
                            sideButilised[j] = true;
                            targetExists = true;
                            
                            break;
                        }
                    }
                    
                    // Check for invalid sideB column
                    if (!targetExists)
                    {
                        throw new NucleusUserException(Localiser.msg("020004", 
                            columnsName, colmds[i].getName(), targetColumnName)).setFatal();
                    }
                }
            }

            // Allocate the user defined columns using the sideB list where target column has not been defined
            for (int i = 0; i < colmds.length; i++)
            {
                if (colmds[i].getTarget() == null)
                {
                    // Find the next unutilised column on sideB
                    for (int j = 0; j < sideBidentifiers.length; j++)
                    {
                        if (!sideButilised[j])
                        {
                            putColumn(sideBidentifiers[j], colmds[i]);
                            sideButilised[j] = true;
                            break;
                        }
                    }
                }
            }

            // Allocate any missing columns
            for (int i = colmds.length; i < mappingSideB.getNumberOfDatastoreMappings(); i++)
            {
                // Find next unallocated sideB column
                DatastoreIdentifier sideBidentifier = null;
                for (int j=0; j < sideBidentifiers.length; j++)
                {
                    if (!sideButilised[j])
                    {
                        sideBidentifier = sideBidentifiers[j];
                        sideButilised[j] = true;
                        break;
                    }
                }
                if (sideBidentifier == null)
                {
                    throw new NucleusUserException(Localiser.msg("020005", 
                        columnsName, "" + i)).setFatal();
                }

                // Create a new ColumnMetaData since user hasn't provided enough
                ColumnMetaData colmd = new ColumnMetaData();
                if (updateContainer)
                {
                    columnContainer.addColumn(colmd);
                }
                putColumn(sideBidentifier, colmd);
            }
        }
        else
        {
            columnsName = null;
            for (int i = 0; i < mappingSideB.getNumberOfDatastoreMappings(); i++)
            {
                final DatastoreIdentifier sideBidentifier;
                sideBidentifier = mappingSideB.getDatastoreMapping(i).getColumn().getIdentifier();

                // Create a new ColumnMetaData since user hasn't provided enough
                ColumnMetaData colmd = new ColumnMetaData();
                putColumn(sideBidentifier, colmd);
            }
        }
    }

    /**
     * Constructor. 
     * Takes the sideB mapping and the side A definition of column metadata and matches them up as
     * defined by the user, and if not defined by the user matches them as best it can.
     * @param columnContainer Container of the columns for side A
     * @param mappingSideB the mapping in the side B
     * @param updateContainer Whether to add any missing ColumnMetaData objects to the container
     */
    public CorrespondentColumnsMapper(ColumnMetaDataContainer columnContainer, JavaTypeMapping mappingSideB,
        boolean updateContainer)
    {
        // Go through the user-defined columns and allocate them as required
        if (columnContainer != null)
        {
            int noOfUserColumns = columnContainer.getColumnMetaData().length;
            ColumnMetaData[] colmds = columnContainer.getColumnMetaData();

            // Generate string of user-specified columns for use in diagnostics
            StringBuilder str=new StringBuilder("Columns [");
            for (int i=0;i<noOfUserColumns;i++)
            {
                str.append(colmds[i].getName());
                if (i < noOfUserColumns-1)
                {
                    str.append(", ");
                }
            }
            str.append("]");
            columnsName = str.toString();

            // Check if too many columns have been defined
            if (noOfUserColumns > mappingSideB.getNumberOfDatastoreMappings())
            {
                throw new NucleusUserException(Localiser.msg("020003", 
                    columnsName, "" + noOfUserColumns, "" + mappingSideB.getNumberOfDatastoreMappings())).setFatal();
            }

            // Retrieve sideB column names
            DatastoreIdentifier[] sideBidentifiers = new DatastoreIdentifier[mappingSideB.getNumberOfDatastoreMappings()];
            boolean[] sideButilised = new boolean[mappingSideB.getNumberOfDatastoreMappings()];
            for (int i = 0; i < mappingSideB.getNumberOfDatastoreMappings(); i++)
            {
                sideBidentifiers[i] = mappingSideB.getDatastoreMapping(i).getColumn().getIdentifier();
                sideButilised[i] = false;
            }
            JavaTypeMapping[] sideBidMappings = ((MultiMapping)mappingSideB).getJavaTypeMapping();

            // Allocate the user-defined columns using the sideB list where target column has been defined
            for (int i = 0; i < noOfUserColumns; i++)
            {
                String targetColumnName = colmds[i].getTarget();
                if (targetColumnName == null)
                {
                    // No target column, so try the field
                    String targetFieldName = colmds[i].getTargetMember();
                    if (targetFieldName != null)
                    {
                        for (int j=0;j<sideBidMappings.length;j++)
                        {
                            if (sideBidMappings[j].getMemberMetaData().getName().equals(targetFieldName))
                            {
                                targetColumnName = sideBidMappings[j].getDatastoreMapping(0).getColumn().getIdentifier().getName();
                                break;
                            }
                        }
                    }
                }

                // Find the target on sideB
                if (targetColumnName != null)
                {
                    boolean targetExists = false;
                    for (int j = 0; j < sideBidentifiers.length; j++)
                    {
                        // This allows for case incorrectness in the specified name
                        if (sideBidentifiers[j].getName().equalsIgnoreCase(targetColumnName) &&
                            !sideButilised[j])
                        {
                            putColumn(sideBidentifiers[j], colmds[i]);
                            sideButilised[j] = true;
                            targetExists = true;
                            
                            break;
                        }
                    }
                    
                    // Check for invalid sideB column
                    if (!targetExists)
                    {
                        throw new NucleusUserException(Localiser.msg("020004", 
                            columnsName, colmds[i].getName(), targetColumnName)).setFatal();
                    }
                }
            }

            // Allocate the user defined columns using the sideB list where target column has not been defined
            for (int i = 0; i < colmds.length; i++)
            {
                if (colmds[i].getTarget() == null)
                {
                    // Find the next unutilised column on sideB
                    for (int j = 0; j < sideBidentifiers.length; j++)
                    {
                        if (!sideButilised[j])
                        {
                            putColumn(sideBidentifiers[j], colmds[i]);
                            sideButilised[j] = true;
                            break;
                        }
                    }
                }
            }

            // Allocate any missing columns
            for (int i = colmds.length; i < mappingSideB.getNumberOfDatastoreMappings(); i++)
            {
                // Find next unallocated sideB column
                DatastoreIdentifier sideBidentifier = null;
                for (int j=0; j < sideBidentifiers.length; j++)
                {
                    if (!sideButilised[j])
                    {
                        sideBidentifier = sideBidentifiers[j];
                        sideButilised[j] = true;
                        break;
                    }
                }
                if (sideBidentifier == null)
                {
                    throw new NucleusUserException(Localiser.msg("020005", 
                        columnsName, "" + i)).setFatal();
                }

                // Create a new ColumnMetaData since user hasn't provided enough
                ColumnMetaData colmd = new ColumnMetaData();
                if (updateContainer)
                {
                    columnContainer.addColumn(colmd);
                }
                putColumn(sideBidentifier, colmd);
            }
        }
        else
        {
            columnsName = null;
            for (int i = 0; i < mappingSideB.getNumberOfDatastoreMappings(); i++)
            {
                final DatastoreIdentifier sideBidentifier;
                sideBidentifier = mappingSideB.getDatastoreMapping(i).getColumn().getIdentifier();

                // Create a new ColumnMetaData since user hasn't provided enough
                ColumnMetaData colmd = new ColumnMetaData();
                putColumn(sideBidentifier, colmd);
            }
        }
    }

    /**
     * Accessor for the column MetaData in side A that maps to the side B identifier.
     * @param name The side B identifier
     * @return ColumnMetaData in side A that equates to the side B column
     */
    public ColumnMetaData getColumnMetaDataByIdentifier(DatastoreIdentifier name)
    {
        return columnMetaDataBySideBIdentifier.get(name);
    }

    /**
     * Method to associate a sideB identifier with a sideA ColumnMetaData
     * @param identifier side B identifier
     * @param colmd side A ColumnMetaData
     */
    private void putColumn(DatastoreIdentifier identifier, ColumnMetaData colmd)
    {
        if (columnMetaDataBySideBIdentifier.put(identifier, colmd) != null)
        {
            throw new NucleusUserException(Localiser.msg("020006", identifier, columnsName)).setFatal();
        }
    }
}