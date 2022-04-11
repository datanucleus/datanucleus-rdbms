/**********************************************************************
Copyright (c) 2006 Erik Bengtson and others. All rights reserved. 
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.rdbms.RDBMSStoreManager;

/**
 * Simple mapping for a null literal. Only used when the type is not determined
 */
public class NullMapping extends SingleFieldMapping
{
    public NullMapping(RDBMSStoreManager storeMgr)
    {
        initialize(storeMgr, null);
    }

    @Override
    public Class getJavaType()
    {
        return null;
    }

    @Override
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        return null;
    }

    @Override
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
    }
}