/**********************************************************************
Copyright (c) 2018 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.key;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Representation of a key that has columns with specified ordering (ascending/descending) for each column (if required).
 */
public abstract class ColumnOrderedKey extends Key
{
    /** Column ordering. True implies ascending order. */
    protected List<Boolean> columnOrdering = new ArrayList<>();

    protected Map<String,String> extensions = null;

    public ColumnOrderedKey(Table table, Map<String, String> extensions)
    {
        super(table);
        this.extensions = extensions;
    }

    public String getValueForExtension(String key)
    {
        return extensions != null ? extensions.get(key) : null;
    }

    /**
     * Class to add a column to the key
     * @param col The column to add
     */
    public void addColumn(Column col)
    {
        assertSameDatastoreObject(col);

        columns.add(col);
        columnOrdering.add(null);
    }

    public void setColumnOrdering(String ordering)
    {
        if (StringUtils.isWhitespace(ordering))
        {
            return;
        }

        StringTokenizer tokeniser = new StringTokenizer(ordering, ",");
        if (tokeniser.countTokens() != columns.size())
        {
            NucleusLogger.DATASTORE_SCHEMA.warn("Attempt to specify orderings of index with name=" + name + 
                " but incorrect number of orderings (" + tokeniser.countTokens() + ") for columns (" +columns.size() + "). IGNORED");
            return;
        }

        Iterator<Column> colIter = columns.iterator();
        int i = 0;
        while (tokeniser.hasMoreTokens())
        {
            String orderingToken = tokeniser.nextToken().toUpperCase();
            colIter.next();
            columnOrdering.set(i, orderingToken.equals("ASC") ? Boolean.TRUE : orderingToken.equals("DESC") ? Boolean.FALSE : null);

            i++;
        }
    }

    /**
     * Sets a column in a specified position <code>seq</code> for this index.
     * @param seq the specified position for the <code>col</code>
     * @param col the Column
     */
    public void setColumn(int seq, Column col)
    {
        assertSameDatastoreObject(col);

        setListMinimumSize(columns, seq + 1);
        setListMinimumSize(columnOrdering, seq + 1);

        if (columns.get(seq) != null)
        {
            throw new NucleusException("Index/candidate part #" + seq + " for " + table + " already set").setFatal();
        }

        columns.set(seq, col);
        columnOrdering.set(seq,  null);
    }

    /**
     * Method to return the list of columns which the key applies to.
     * @param includeOrdering Whether to include ordering in the column list when it is specified
     * @return The column list.
     */
    public String getColumnList(boolean includeOrdering)
    {
        StringBuilder s = new StringBuilder("(");
        Iterator<Column> colIter = columns.iterator();
        Iterator<Boolean> colOrderIter = columnOrdering.iterator();
        while (colIter.hasNext())
        {
            Column col = colIter.next();

            s.append(col != null ? col.getIdentifier() : "?");

            if (includeOrdering)
            {
                Boolean colOrder = colOrderIter.next();
                if (colOrder != null)
                {
                    s.append(colOrder ? " ASC" : " DESC");
                }
            }

            if (colIter.hasNext())
            {
                s.append(',');
            }
        }
        s.append(')');

        return s.toString();
    }
}
