/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved.
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
2009 Andy Jefferson - removed hardcoded type, changed to use RequestType enum
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.request;

import java.util.Arrays;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.table.DatastoreClass;

/**
 * Representation of a request id.
 */
public class RequestIdentifier
{
    private final DatastoreClass table;
    private final int[] memberNumbers;
    private final RequestType type;
    private final int hashCode;
    private final String className;

    /**
     * Constructor.
     * @param table Datastore class for which this is a request
     * @param mmds MetaData of fields/properties to use in the request (if required)
     * @param type The type being represented
     * @param className The name of the class
     */
    public RequestIdentifier(DatastoreClass table, AbstractMemberMetaData[] mmds, RequestType type, 
            String className)
    {
        this.table = table;
        this.type = type;

        if (mmds == null)
        {
            this.memberNumbers = null;
        }
        else
        {
            this.memberNumbers = new int[mmds.length];
            for (int i=0;i<this.memberNumbers.length;i++)
            {
                this.memberNumbers[i] = mmds[i].getAbsoluteFieldNumber();
            }

            // The key uniqueness is dependent on memberNumbers being sorted
            Arrays.sort(this.memberNumbers);
        }
        this.className = className;

        // Since we are an immutable object, pre-compute the hash code for improved performance in equals()
        int h = table.hashCode() ^ type.hashCode() ^ className.hashCode();
        if (this.memberNumbers != null)
        {
            for (int i = 0; i < this.memberNumbers.length; ++i)
            {
                h ^= this.memberNumbers[i];
            }
        }
        hashCode = h;
    }

    /**
     * Accessor for the table of this request.
     * @return Table used in the request
     */
    public DatastoreClass getTable()
    {
        return table;
    }

    /**
     * Accessor for the hashcode
     * @return The hashcode
     */
    public int hashCode()
    {
        return hashCode;
    }

    /**
     * Equality operator
     * @param o Object to compare with
     * @return Whether the objects are equal
     */
    public boolean equals(Object o)
    {
        if (o == this)
        {
            return true;
        }
        if (!(o instanceof RequestIdentifier))
        {
            return false;
        }

        RequestIdentifier ri = (RequestIdentifier)o;
        if (hashCode != ri.hashCode)
        {
            return false;
        }

        return table.equals(ri.table) && type.equals(ri.type) &&
            Arrays.equals(memberNumbers, ri.memberNumbers) && className.equals(ri.className);
    }
}