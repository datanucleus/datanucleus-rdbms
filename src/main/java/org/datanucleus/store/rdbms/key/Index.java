/**********************************************************************
Copyright (c) 2003 Andy Jefferson and others. All rights reserved.
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
	TJDO - original version
	Andy Jefferson - equality operator
	Andy Jefferson - setName()
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.key;

import java.util.Map;

import org.datanucleus.store.rdbms.table.Table;

/**
 * Representation of an index.
 */
public class Index extends ColumnOrderedKey
{
    public static final String EXTENSION_INDEX_EXTENDED_SETTING = "extended-setting";
    public static final String EXTENSION_INDEX_TYPE = "index-type";

    private final boolean isUnique;

    /**
     * Constructor.
     * @param table The table
     * @param isUnique Whether the index is unique
     * @param extensions Any extensions for the index
     */
    public Index(Table table, boolean isUnique, Map<String, String> extensions)
    {
        super(table, extensions);

        this.isUnique = isUnique;
    }

    /**
     * Constructor for an index for the specified candidate key.
     * @param ck Candidate key to use as a basis
     */
    public Index(CandidateKey ck)
    {
        super(ck.getTable(), null);

        isUnique = true;

        columns.addAll(ck.getColumns());
        int numCols = columns.size();
        for (int i = 0; i < numCols; i++)
        {
            columnOrdering.add(Boolean.TRUE);
        }
    }

    /**
     * Constructor for an index for the specified foreign key.
     * @param fk Foreign key to use as a basis
     */
    public Index(ForeignKey fk)
    {
        super(fk.getTable(), null);

        isUnique = false;

        columns.addAll(fk.getColumns());
        int numCols = columns.size();
        for (int i = 0; i < numCols; i++)
        {
            columnOrdering.add(Boolean.TRUE);
        }
    }

    /**
     * Accessor for whether the index is unique
     * @return Whether it is unique.
     */
    public boolean getUnique()
    {
        return isUnique;
    }

    /**
     * Hashcode operator.
     * @return The hashcode
     **/
    public int hashCode()
    {
        return (isUnique ? 0 : 1) ^ super.hashCode();
    }

    /**
     * Equality operator.
     * @param obj Object to compare against
     * @return Whether they are equal.
     */
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof Index))
        {
            return false;
        }

        Index idx = (Index)obj;
        if (idx.isUnique != isUnique)
        {
            return false;
        }

        return super.equals(obj);
    }

    /**
     * Stringify method.
     * @return String version of this object.
     */
    public String toString()
    {
        // TODO Change this to return "INDEX (...)"
        return getColumnList();
    }
}