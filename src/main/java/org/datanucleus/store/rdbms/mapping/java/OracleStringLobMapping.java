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
2004 Erik Bengtson - changed to use EMPTY_CLOB approach
2006 Andy Jefferson - use commonised CLOB method
2007 Thomas Marti - added BLOB handling
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingPostSet;
import org.datanucleus.store.rdbms.mapping.column.OracleBlobColumnMapping;
import org.datanucleus.store.rdbms.mapping.column.OracleClobColumnMapping;

/**
 * Mapping for a String type for Oracle when stored in a BLOB or CLOB column.
 */
public class OracleStringLobMapping extends StringMapping
{
    /**
     * Retrieve the empty BLOB/CLOB locator created by the insert statement and write out the current BLOB/CLOB field value to the Oracle BLOB/CLOB object
     * @param sm StateManager owner of this field
     */
    @Override
    public void performSetPostProcessing(DNStateManager sm)
    {
        // Generate the contents for the BLOB/CLOB
        String value = (String)sm.provideField(mmd.getAbsoluteFieldNumber());
        sm.isLoaded(mmd.getAbsoluteFieldNumber());
        if (value == null)
        {
            value = "";
        }
        else if (value.length() == 0)
        {
            if (storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_PERSIST_EMPTY_STRING_AS_NULL))
            {
                value = "";
            }
            else
            {
                value = storeMgr.getDatastoreAdapter().getSurrogateForEmptyStrings();
            }
        }

        // Update BLOB/CLOB value
        if (columnMappings[0] instanceof ColumnMappingPostSet)
        {
            if (columnMappings[0] instanceof OracleBlobColumnMapping)
            {
                ((ColumnMappingPostSet)columnMappings[0]).setPostProcessing(sm, value.getBytes());
            }
            else if (columnMappings[0] instanceof OracleClobColumnMapping)
            {
                ((ColumnMappingPostSet)columnMappings[0]).setPostProcessing(sm, value);
            }
        }
    }
}