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

/**
 * Definition of the mapping of a new object definition in the results of a statement.
 * The mappings are for the arguments to the constructor.
 */
public class StatementNewObjectMapping
{
    /** Class that we create an object of. */
    Class cls = null;

    /** Mappings for the constructor objects keyed by the position (in the constructor). */
    Map<Integer, Object> ctrArgMappings = null;

    public StatementNewObjectMapping(Class cls)
    {
        this.cls = cls;
    }

    public Class getObjectClass()
    {
        return cls;
    }

    /**
     * Accessor for the mapping info for a constructor argument at the specified position.
     * @param position The position in the constructor
     * @return The argument mappings
     */
    public Object getConstructorArgMapping(int position)
    {
        if (ctrArgMappings == null)
        {
            return null;
        }
        return ctrArgMappings.get(position);
    }

    /**
     * Method to define the mapping for a constructor argument.
     * The "mapping" can be either a StatementMappingIndex, a literal or a StatementNewObjectMapping
     * @param ctrPos The position in the constructor
     * @param argMapping The mapping for the argument
     */
    public void addConstructorArgMapping(int ctrPos, Object argMapping)
    {
        if (ctrArgMappings == null)
        {
            ctrArgMappings = new HashMap<Integer, Object>();
        }
        ctrArgMappings.put(ctrPos, argMapping);
    }

    public boolean isEmpty()
    {
        return (getNumberOfConstructorArgMappings() == 0);
    }

    public int getNumberOfConstructorArgMappings()
    {
        return (ctrArgMappings != null ? ctrArgMappings.size() : 0);
    }

    public String toString()
    {
        StringBuffer str = new StringBuffer("StatementNewObject: " + cls.getName() + "(");
        if (ctrArgMappings != null)
        {
            Iterator<Integer> keyIter = ctrArgMappings.keySet().iterator();
            while (keyIter.hasNext())
            {
                Integer position = keyIter.next();
                str.append(ctrArgMappings.get(position));
                if (keyIter.hasNext())
                {
                    str.append(",");
                }
            }
        }
        str.append(")");
        return str.toString();
    }
}