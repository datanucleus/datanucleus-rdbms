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

import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;

/**
 * Definition of the mapping of result expressions in a datastore statement.
 * In a typical RDBMS SQL statement the result clause will contain many expressions.
 * If the overall result of the statement maps to a class then we use StatementClassMapping.
 * If the overall result has various expressions then we use this.
 * Each position in the result can either be a StatementMappingIndex, or a StatementNewObjectMapping,
 * or a StatementClassMapping.
 */
public class StatementResultMapping
{
    /** Mappings for the result expressions keyed by the position. */
    Map<Integer, Object> mappings = null;

    public StatementResultMapping()
    {
    }

    /**
     * Accessor for the mapping information for the result expression at the position.
     * @param position The position in the result
     * @return The mapping information
     */
    public Object getMappingForResultExpression(int position)
    {
        if (mappings == null)
        {
            return null;
        }
        return mappings.get(position);
    }

    public void addMappingForResultExpression(int position, StatementMappingIndex mapping)
    {
        if (mappings == null)
        {
            mappings = new HashMap<Integer, Object>();
        }
        mappings.put(position, mapping);
    }

    public void addMappingForResultExpression(int position, StatementNewObjectMapping mapping)
    {
        if (mappings == null)
        {
            mappings = new HashMap<Integer, Object>();
        }
        mappings.put(position, mapping);
    }

    public void addMappingForResultExpression(int position, StatementClassMapping mapping)
    {
        if (mappings == null)
        {
            mappings = new HashMap<Integer, Object>();
        }
        mappings.put(position, mapping);
    }

    public boolean isEmpty()
    {
        return (getNumberOfResultExpressions() == 0);
    }

    public int getNumberOfResultExpressions()
    {
        return (mappings != null ? mappings.size() : 0);
    }

    public String toString()
    {
        StringBuilder str = new StringBuilder("StatementResults:");
        if (mappings != null)
        {
            Iterator<Map.Entry<Integer, Object>> mapIter = mappings.entrySet().iterator();
            while (mapIter.hasNext())
            {
                Map.Entry<Integer, Object> entry = mapIter.next();
                str.append(" position=").append(entry.getKey());
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