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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

/**
 * SCO Mapping for an SQLTime type.
 */
public class SqlTimeMapping extends TemporalMapping
{
    public Class getJavaType()
    {
        return java.sql.Time.class;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.TemporalMapping#getDefaultLengthAsString()
     */
    protected int getDefaultLengthAsString()
    {
        // java.sql.Time requires 8 characters ("hh:mm:ss")
        return 8;
    }
}