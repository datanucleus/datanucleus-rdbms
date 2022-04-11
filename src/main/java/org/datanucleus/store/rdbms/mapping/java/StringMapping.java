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
2002 Mike Martin (TJDO)
2003 Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import org.datanucleus.util.StringUtils;

/**
 * Mapping for a String type.
 */
public class StringMapping extends SingleFieldMapping
{
    @Override
    public Class getJavaType()
    {
        return String.class;
    }

    @Override
    public Object[] getValidValues(int index)
    {
        if (mmd != null)
        {
            if (mmd.hasExtension(EXTENSION_CHECK_CONSTRAINT_VALUES))
            {
                String valuesStr = mmd.getValueForExtension(EXTENSION_CHECK_CONSTRAINT_VALUES);
                return StringUtils.split(valuesStr, ",");
            }
        }

        return super.getValidValues(index);
    }
}