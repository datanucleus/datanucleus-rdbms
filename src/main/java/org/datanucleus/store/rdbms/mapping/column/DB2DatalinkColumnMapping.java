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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.column;

import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.adapter.DB2TypeInfo;
import org.datanucleus.store.rdbms.mapping.MappingManagerImpl;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Mapping of a DB2 "Datalink" column.
 */
public class DB2DatalinkColumnMapping extends CharColumnMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public DB2DatalinkColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(mapping, storeMgr, col);
	}

    protected void initialize()
    {
        if (column != null)
        {
            if (mapping.getMemberMetaData().getValueForExtension(MappingManagerImpl.METADATA_EXTENSION_SELECT_FUNCTION) == null)
            {
                column.setWrapperFunction("DLURLCOMPLETEONLY(?)", Column.WRAPPER_FUNCTION_SELECT);
            }
        }
		initTypeInfo();
    }

    public int getJDBCType()
    {
        return DB2TypeInfo.DATALINK;
    }

    public String getInsertionInputParameter()
    {
        //instead of (?) we use (? || '') as workaround
        //could be replaced with something like ? CAST ( ? AS varchar(255) ) 
        return "DLVALUE(? || '')";
    }
    
    public boolean includeInFetchStatement()
    {
        return true;
    }

    public String getUpdateInputParameter()
    {
        //instead of (?) we use (? || '') as workaround
        //could be replaced with something like ? CAST ( ? AS varchar(255) ) 
        return "DLVALUE(? || '')";
    }	
}