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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.key;

import org.datanucleus.store.rdbms.table.Table;

/**
 * Representation of the primary key of a table.
 */
public class PrimaryKey extends CandidateKey // TODO Do we need to extend CandidateKey?
{
    /**
     * Creates a primary key. A default name of the primary key is created by the constructor. This name can be overwritten.
     * @param table Table that this is the PK for
     */
    public PrimaryKey(Table table)
    {
        super(table, null);
        name = table.getStoreManager().getIdentifierFactory().newPrimaryKeyIdentifier(table).getName();
    }

    /**
     * Accessor for the size.
     * @return The size.
     */
    public int size()
    {
        return columns.size();
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof PrimaryKey))
        {
            return false;
        }

        return super.equals(obj);
    }

    public int hashCode()
    {
        return super.hashCode();
    }

    /**
     * Stringifier method.
     * Generates a form of the PK ready to be used in a DDL statement.
     * e.g <pre>PRIMARY KEY (col1,col2)</pre>
     * @return The string form of this object. Ready to be used in a DDL statement.
     */
    public String toString()
    {
        return new StringBuilder("PRIMARY KEY ").append(getColumnList(columns)).toString();
    }
}