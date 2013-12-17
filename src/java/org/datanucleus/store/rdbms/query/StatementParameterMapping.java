/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;

/**
 * Definition of the mapping of parameters in a datastore statement.
 * A typical use is for RDBMS where we have a JDBC statement and each parameter is looked up
 * via its name. The mapping information for the parameter provides the mapping and the parameter
 * positions to use.
 */
public class StatementParameterMapping
{
    /** Mappings for the parameters keyed by the parameter name. */
    Map<String, StatementMappingIndex> mappings = null;

    public StatementParameterMapping()
    {
    }

    /**
     * Accessor for the mapping information for the parameter with the specified name.
     * @param name Parameter name
     * @return The mapping information
     */
    public StatementMappingIndex getMappingForParameter(String name)
    {
        if (mappings == null)
        {
            return null;
        }
        return mappings.get(name);
    }

    /**
     * Convenience method to return the mapping for the parameter that is at the specified position.
     * The position should use the same origin as the parameter positions here.
     * @param pos The position
     * @return The mapping (if any)
     */
    public StatementMappingIndex getMappingForParameterPosition(int pos)
    {
        if (mappings == null)
        {
            return null;
        }
        Iterator iter = mappings.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, StatementMappingIndex> entry = (Map.Entry)iter.next();
            StatementMappingIndex idx = entry.getValue();
            for (int i=0;i<idx.getNumberOfParameterOccurrences();i++)
            {
                int[] positions = idx.getParameterPositionsForOccurrence(i);
                for (int j=0;j<positions.length;j++)
                {
                    if (positions[j] == pos)
                    {
                        return idx;
                    }
                }
            }
        }
        return null;
    }

    public void addMappingForParameter(String name, StatementMappingIndex mapping)
    {
        if (mappings == null)
        {
            mappings = new HashMap<String, StatementMappingIndex>();
        }
        mappings.put(name, mapping);
    }

    public String[] getParameterNames()
    {
        if (mappings == null)
        {
            return null;
        }

        return mappings.keySet().toArray(new String[mappings.size()]);
    }

    public boolean isEmpty()
    {
        return (mappings == null || mappings.size() == 0);
    }

    public String toString()
    {
        StringBuilder str = new StringBuilder("StatementParameters:");
        if (mappings != null)
        {
            Iterator<Map.Entry<String, StatementMappingIndex>> mapIter = mappings.entrySet().iterator();
            while (mapIter.hasNext())
            {
                Map.Entry<String, StatementMappingIndex> entry = mapIter.next();
                str.append(" param=").append(entry.getKey());
                str.append(" mapping=").append(entry.getValue());
                if (mapIter.hasNext())
                {
                    str.append(",");
                }
            }
        }
        return str.toString();
    }
}