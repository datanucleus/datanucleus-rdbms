/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved. 
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
2004 Brendan de Beer - primitive array support
2004 Andy Jefferson - localised messages
2005 Andy Jefferson - added primitive wrapper array capability
2006 Andy Jefferson - migrated to support serialised and non-serialised fields
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.sql.Types;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Mapping of a LONGVARBINARY RDBMS type.
 */
public class LongVarBinaryRDBMSMapping extends AbstractLargeBinaryRDBMSMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public LongVarBinaryRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(mapping, storeMgr, col);
	}

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping#getJDBCType()
     */
    @Override
    public int getJDBCType()
    {
        return Types.LONGVARBINARY;
    }
}