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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;

/**
 * Representation of an index.
 * TODO Add concept of column order (ASC|DESC)
 */
public class Index extends Key
{
    private final boolean isUnique;

    /** extended index settings, mostly datastore proprietary settings. */
    private final String extendedIndexSettings;

    /**
     * Constructor.
     * @param table The table
     * @param isUnique Whether the index is unique
     * @param extendedIndexSettings extended index settings
     */
    public Index(Table table, boolean isUnique, String extendedIndexSettings)
    {
        super(table);

        this.isUnique = isUnique;
        this.extendedIndexSettings = extendedIndexSettings;
    }

    /**
     * Constructor.
     * @param ck Candidate key to use as a basis
     */
    public Index(CandidateKey ck)
    {
        super(ck.getTable());

        isUnique = true;
        extendedIndexSettings = null;
        columns.addAll(ck.getColumns());
    }

    /**
     * Constructor.
     * @param fk Foreign key to use as a basis
     */
    public Index(ForeignKey fk)
    {
        super(fk.getTable());

        isUnique = false;
        extendedIndexSettings = null;
        columns.addAll(fk.getColumns());
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
     * Sets a column for in a specified position <code>seq</code>
     * @param seq the specified position for the <code>col</code>
     * @param col the Column
     */
    public void setColumn(int seq, Column col)
    {
        assertSameDatastoreObject(col);

        setMinSize(columns, seq + 1);

        if (columns.get(seq) != null)
        {
            throw new NucleusException("Index part #" + seq + " for " + table + " already set").setFatal();
        }

        columns.set(seq, col);
    }

    /**
     * Accessor for the size.
     * @return The size.
     */
    public int size()
    {
        return columns.size();
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
     * Extended index settings, mostly datastore proprietary settings
     * @return the extended settings
     */
    public String getExtendedIndexSettings()
    {
        return extendedIndexSettings;
    }
    
    /**
     * Stringify method.
     * @return String version of this object.
     */
    public String toString()
    {
        return getColumnList();
    }
}