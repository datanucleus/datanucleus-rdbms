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
2008 Andy Jefferson - Cleaned up and documented.
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.util.StringUtils;

/**
 * Representation of a mapping in a statement (such as JDBC), and its mapping to the associated column(s)
 * or parameters that the mapping relates to. The mapping may be for a field of a class (whether it is a real
 * field or a surrogate), or for a parameter.
 * <ul>
 * <li>Provides index positions of the column(s) of this field in the result clause of the statement</li>
 * <li>Provides parameter position(s) if any of the column(s) are statement parameters, for input</li>
 * </ul>
 * For example, fields of a class in the result clause of a statement.
 * <PRE>
 * CLASS                 FIELDNUM     MAPPING            TABLE                COLUMN POSITIONS
 * -----------------     --------     --------------     ----------------     ----------------
 * class A                                               TABLE_A              
 * {
 *    short fieldA;      1            ShortMapping       COL_FIELDA           1
 *    String fieldB;     2            StringMapping      COL_FIELDB_PART1     2
 *                                                       COL_FIELDB_PART2     3
 *    ...
 * }
 * </PRE>
 */
public class StatementMappingIndex
{
    /** Mapping being represented. */
    JavaTypeMapping mapping;

    /** Positions for the column(s) for this field in the result clause of a statement. **/
    int[] columnPositions;

    /** Positions where this mapping is a parameter. Each usage of the mapping has n parameter positions. */
    List<int[]> paramPositions = null;

    /** Any name applied in the field selection (SELECT xxx AS yyy). Only applies for cases with 1 column. */
    String columnName;

    public StatementMappingIndex(JavaTypeMapping mapping)
    {
        this.mapping = mapping;
    }

    public JavaTypeMapping getMapping()
    {
        return mapping;
    }

    public void setMapping(JavaTypeMapping mapping)
    {
        this.mapping = mapping;
    }

    /**
     * Accessor for the column name alias (if any).
     * Only applies to the first column for this field.
     * @return The column alias.
     */
    public String getColumnAlias()
    {
        if (columnName != null)
        {
            return columnName;
        }
        else if (mapping != null && mapping.getMemberMetaData() != null)
        {
            return mapping.getMemberMetaData().getName();
        }
        return null;
    }

    /**
     * Mutator for the column name (alias).
     * Overrides the name of the field that the mapping refers to.
     * @param alias The name of the column (alias).
     */
    public void setColumnAlias(String alias)
    {
        this.columnName = alias;
    }

    /**
     * Accessor for the column position(s).
     * @return The column position(s) in the result clause of the statement.
     */
    public int[] getColumnPositions()
    {
        return columnPositions;
    }

    /**
     * Mutator for the column positions in the result clause of a statement.
     * @param pos The column position(s)
     */
    public void setColumnPositions(int[] pos)
    {
        columnPositions = pos;
    }

    /**
     * Method to register statement position(s) that this mapping is used as a parameter.
     * The number of positions must be the same as the mapping number of columns.
     * @param positions The parameter positions in the statement.
     */
    public void addParameterOccurrence(int[] positions)
    {
        if (paramPositions == null)
        {
            paramPositions = new ArrayList<>();
        }
        if (mapping != null && positions.length != mapping.getNumberOfDatastoreMappings())
        {
            throw new NucleusException("Mapping " + mapping + " cannot be " + positions.length + " parameters since it has " + mapping.getNumberOfDatastoreMappings() + " columns");
        }
        paramPositions.add(positions);
    }

    /**
     * Method to deregister statement positions that this mapping is used as a parameter.
     * @param positions The param positions to deregister
     */
    public void removeParameterOccurrence(int[] positions)
    {
        if (paramPositions != null)
        {
            paramPositions.remove(positions);
        }
    }

    /**
     * Accessor for the number of times this mapping is used as a parameter.
     * @return Number of times used as a parameter
     */
    public int getNumberOfParameterOccurrences()
    {
        return (paramPositions != null) ? paramPositions.size() : 0;
    }

    /**
     * Accessor for the parameter positions for this occurrence of use of the mapping as a parameter.
     * @param num The occurrence of using this mapping as a parameter.
     * @return The parameter positions
     */
    public int[] getParameterPositionsForOccurrence(int num)
    {
        return (paramPositions == null) ? null : paramPositions.get(num);
    }

    /**
     * Method to return a string version of this object.
     * @return String version
     */
    public String toString()
    {
        StringBuilder str = new StringBuilder();
        str.append("mapping: " + mapping);
        if (paramPositions != null)
        {
            str.append(" parameter(s): ");
            Iterator<int[]> iter = paramPositions.iterator();
            while (iter.hasNext())
            {
                int[] positions = iter.next();
                str.append(StringUtils.intArrayToString(positions));
                if (iter.hasNext())
                {
                    str.append(',');
                }
            }
        }
        if (columnPositions != null)
        {
            str.append(" column(s): " + StringUtils.intArrayToString(columnPositions));
        }
        return str.toString();
    }
}