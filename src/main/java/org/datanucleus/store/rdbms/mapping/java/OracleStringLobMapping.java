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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.datastore.OracleBlobRDBMSMapping;
import org.datanucleus.store.rdbms.mapping.datastore.OracleClobRDBMSMapping;

/**
 * Mapping for a String type for Oracle when stored in a BLOB or CLOB column.
 */
public class OracleStringLobMapping extends StringMapping implements MappingCallbacks
{
    /**
     * Retrieve the empty BLOB/CLOB locator created by the insert statement
     * and write out the current BLOB/CLOB field value to the Oracle BLOB/CLOB object
     * @param op The ObjectProvider owner of this field
     */
    public void insertPostProcessing(ObjectProvider op)
    {
        // Generate the contents for the BLOB/CLOB
        String value = (String)op.provideField(mmd.getAbsoluteFieldNumber());
        op.isLoaded(mmd.getAbsoluteFieldNumber());
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
        if (mmd.getColumnMetaData()[0].getJdbcType() == JdbcType.BLOB)
        {
            OracleBlobRDBMSMapping.updateBlobColumn(op, getTable(), getDatastoreMapping(0), value.getBytes());
        }
        else if (mmd.getColumnMetaData()[0].getJdbcType() == JdbcType.CLOB)
        {
            OracleClobRDBMSMapping.updateClobColumn(op, getTable(), getDatastoreMapping(0), value);
        }
        else
        {
            throw new NucleusException("AssertionError: Only JDBC types BLOB and CLOB are allowed!");
        }
    }

    public void postInsert(ObjectProvider op)
    {
    }

    public void postFetch(ObjectProvider op)
    {
    }

    public void postUpdate(ObjectProvider op)
    {
        insertPostProcessing(op);
    }

    public void preDelete(ObjectProvider op)
    {
    }
}