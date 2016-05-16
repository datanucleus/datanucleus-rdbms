/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.mapping;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Definition of statement mapping for a particular class.
 * Provides a definition of how the results of a datastore query are mapped on to the fields of a class.
 * If we take the example of this being the candidate class retrieved, then the memberName will be null
 * and any children represent persistent objects also retrieved in this statement. So we process all fields
 * in the result-set for this class, then instantiate any child object (with its id field(s)), and process
 * all of its fields, etc.
 * <p>
 * We can have a result set with fields for multiple objects and instantiate more than 1 related object. 
 * To give an example of what an object of this type can look like
 * <pre>
 * StatementClassMapping:class=null,member=null,mappings=
 * [
 *    {field=0,mapping=mapping: org.datanucleus.store.rdbms.mapping.IntegerMapping@5995c31c column(s): [2]},
 *    {field=1,mapping=mapping: org.datanucleus.store.rdbms.mapping.PersistableMapping@5947d51d column(s): [3]},
 *    {field=2,mapping=mapping: org.datanucleus.store.rdbms.mapping.PersistableMapping@585e7d87 column(s): [7]},
 *    {field=-2,mapping=mapping: org.datanucleus.store.rdbms.mapping.VersionLongMapping@ce796 column(s): [11]},
 * ],
 * children=
 * [
 *    {field=1,mapping=StatementClassMapping:class=org.datanucleus.test.CircRef,member=next,mappings=
 *       [
 *            {field=0,mapping=mapping: org.datanucleus.store.rdbms.mapping.IntegerMapping@5995c31c column(s): [3]},
 *            {field=1,mapping=mapping: org.datanucleus.store.rdbms.mapping.PersistableMapping@5947d51d column(s): [4]},
 *            {field=2,mapping=mapping: org.datanucleus.store.rdbms.mapping.PersistableMapping@585e7d87 column(s): [5]},
 *            {field=-2,mapping=mapping: org.datanucleus.store.rdbms.mapping.VersionLongMapping@ce796 column(s): [6]}
 *        ]},
 *    {field=2,mapping=StatementClassMapping:class=org.datanucleus.test.CircRef,member=prev,mappings=
 *        [
 *            {field=0,mapping=mapping: org.datanucleus.store.rdbms.mapping.IntegerMapping@5995c31c column(s): [7]},
 *            {field=1,mapping=mapping: org.datanucleus.store.rdbms.mapping.PersistableMapping@5947d51d column(s): [8]},
 *            {field=2,mapping=mapping: org.datanucleus.store.rdbms.mapping.PersistableMapping@585e7d87 column(s): [9]},
 *            {field=-2,mapping=mapping: org.datanucleus.store.rdbms.mapping.VersionLongMapping@ce796 column(s): [10]}
 *        ]}
 * ]</pre>
 * In this example we have a persistable object as candidate, and it has 2 1-1 relations (field=1, field=2).
 * These are both being retrieved in the same query, hence the sub-mappings. Note that the column(s) of the FK
 * in the owner object is the same as the PK field(s) of the related object.
 */
public class StatementClassMapping
{
    public static final int MEMBER_DATASTORE_ID = -1;
    public static final int MEMBER_VERSION = -2;
    public static final int MEMBER_DISCRIMINATOR = -3;
    public static final int MEMBER_MULTITENANCY = -4;

    /** Name of the class. */
    String className;

    /** Name of the field/property in the parent class (null implies parent class). */
    String memberName;

    /** Name of a NUCLEUS_TYPE column if used in this query for determining this class. Null otherwise. */
    String nucleusTypeColumn;

    /** Numbers of fields/properties defined in the statement. */
    int[] memberNumbers;

    /** Mappings for the members of this object defined in the statement, keyed by the member number. */
    Map<Integer, StatementMappingIndex> mappings = new HashMap<Integer, StatementMappingIndex>();

    /** Mapping definition for a member that is a relation in this statement, keyed by the member number. */
    Map<Integer, StatementClassMapping> children;

    public StatementClassMapping()
    {
        this(null, null);
    }

    public StatementClassMapping(String className, String memberName)
    {
        this.className = className;
        this.memberName = memberName;
    }

    public String getClassName()
    {
        return className;
    }

    public String getMemberName()
    {
        return memberName;
    }

    public void setNucleusTypeColumnName(String colName)
    {
        this.nucleusTypeColumn = colName;
    }

    public String getNucleusTypeColumnName()
    {
        return nucleusTypeColumn;
    }

    /**
     * Accessor for the mapping information for the member at the specified position.
     * Member positions start at 0 (first member in the root persistent class).
     * Member position of -1 means datastore-identity.
     * Member position of -2 means version.
     * Member position of -3 means discriminator.
     * @param position The member position
     * @return The mapping information
     */
    public StatementMappingIndex getMappingForMemberPosition(int position)
    {
        return mappings.get(position);
    }

    /**
     * Accessor for the mapping definition for the object at the specified member position.
     * Returns null if the member is not a relation field.
     * Member positions start at 0 (first member in the root persistent class).
     * Member position of -1 means datastore-identity.
     * Member position of -2 means version.
     * Member position of -3 means discriminator.
     * Member position of -4 means multitenancy.
     * @param position The position of the member in this class
     * @return The mapping definition for the object at this member position
     */
    public StatementClassMapping getMappingDefinitionForMemberPosition(int position)
    {
        if (children != null)
        {
            return children.get(position);
        }
        return null;
    }

    /**
     * Accessor for whether this definition has child object definitions.
     * @return Whether there are sub-objects of this object mapped.
     */
    public boolean hasChildMappingDefinitions()
    {
        return (children != null && children.size() > 0);
    }

    /**
     * Accessor for the numbers of the members present.
     * Doesn't include any surrogate numbers since not real members.
     * @return The member positions
     */
    public int[] getMemberNumbers()
    {
        if (memberNumbers != null)
        {
            // Cache the member numbers
            return memberNumbers;
        }

        int length = mappings.size();
        if (mappings.containsKey(MEMBER_DATASTORE_ID))
        {
            length--; // Ignore datastore id
        }
        if (mappings.containsKey(MEMBER_VERSION))
        {
            length--; // Ignore version
        }
        if (mappings.containsKey(MEMBER_DISCRIMINATOR))
        {
            length--; // Ignore discriminator
        }
        if (mappings.containsKey(MEMBER_MULTITENANCY))
        {
            length--; // Ignore multitenancy
        }

        int[] positions = new int[length];
        Iterator<Integer> iter = mappings.keySet().iterator();
        int i = 0;
        while (iter.hasNext())
        {
            Integer val = iter.next();
            if (val >= 0)
            {
                positions[i++] = val;
            }
        }
        memberNumbers = positions;
        return positions;
    }

    public void addMappingForMember(int position, StatementMappingIndex mapping)
    {
        memberNumbers = null;
        mappings.put(position, mapping);
    }

    public void addMappingDefinitionForMember(int position, StatementClassMapping defn)
    {
        memberNumbers = null;
        if (children == null)
        {
            children = new HashMap();
        }
        children.put(position, defn);
    }

    public StatementClassMapping cloneStatementMappingWithoutChildren()
    {
        StatementClassMapping mapping = new StatementClassMapping(className, memberName);
        mapping.nucleusTypeColumn = nucleusTypeColumn;
        Iterator iter = mappings.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry entry = (Map.Entry)iter.next();
            Integer key = (Integer)entry.getKey();
            StatementMappingIndex value = (StatementMappingIndex)entry.getValue();
            mapping.addMappingForMember(key, value);
        }
        return mapping;
    }

    public String toString()
    {
        StringBuilder str = new StringBuilder("StatementClassMapping:");
        str.append("class=" + className + ",member=" + memberName);

        str.append(",mappings=[");
        Iterator<Map.Entry<Integer, StatementMappingIndex>> mapIter = mappings.entrySet().iterator();
        while (mapIter.hasNext())
        {
            Map.Entry<Integer, StatementMappingIndex> entry = mapIter.next();
            str.append("{field=").append(entry.getKey());
            str.append(",mapping=").append(entry.getValue());
            str.append("}");
            if (mapIter.hasNext() || children != null)
            {
                str.append(",");
            }
        }
        str.append("]");

        if (children != null)
        {
            str.append(",children=[");
            Iterator<Map.Entry<Integer, StatementClassMapping>> childIter = children.entrySet().iterator();
            while (childIter.hasNext())
            {
                Map.Entry<Integer, StatementClassMapping> entry = childIter.next();
                str.append("{field=").append(entry.getKey());
                str.append(",mapping=").append(entry.getValue());
                str.append("}");
                if (childIter.hasNext())
                {
                    str.append(",");
                }
            }
            str.append("]");
        }

        if (nucleusTypeColumn != null)
        {
            str.append(",nucleusTypeColumn=" + nucleusTypeColumn);
        }
        return str.toString();
    }
}