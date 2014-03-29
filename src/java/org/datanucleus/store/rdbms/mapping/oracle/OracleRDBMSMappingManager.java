/******************************************************************
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
2004 Andy Jefferson - moved getMapping() from OracleDatabaseAdapter
2005 Andy Jefferson - changed to extend RDBMSMappingManager
2006 Andy Jefferson - added override mapping classes method
2006 Andy Jefferson - moved all mappings to plugin.xml
2007 Thomas Marti - added BLOB handling
    ...
*****************************************************************/
package org.datanucleus.store.rdbms.mapping.oracle;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ElementMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.KeyMetaData;
import org.datanucleus.metadata.ValueMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.RDBMSMappingManager;
import org.datanucleus.store.rdbms.mapping.java.ArrayMapping;
import org.datanucleus.store.rdbms.mapping.java.BitSetMapping;
import org.datanucleus.store.rdbms.mapping.java.CollectionMapping;
import org.datanucleus.store.rdbms.mapping.java.MapMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.StringMapping;

/**
 * Mapping manager for Oracle RDBMS adapters.
 * Overrides some of the mappings in the RDBMSMappingManager to allow for Oracle strangeness 
 * on BLOB/CLOB Handling.
 */
public class OracleRDBMSMappingManager extends RDBMSMappingManager
{
    /**
     * Constructor for a mapping manager for an ORM datastore.
     * @param storeMgr The StoreManager
     */
    public OracleRDBMSMappingManager(RDBMSStoreManager storeMgr)
    {
        super(storeMgr);
    }

    /**
     * Method to allow overriding of mapping classes
     * @param mappingClass The mapping class
     * @param mmd Field meta data for the field (if appropriate)
     * @param fieldRole Role of this column for the field (e.g collection element)
     * @return The mapping class to use
     */
    protected Class getOverrideMappingClass(Class mappingClass, AbstractMemberMetaData mmd, int fieldRole)
    {
        // Override some mappings with Oracle-specific mappings
        if (mappingClass.equals(BitSetMapping.class))
        {
            return OracleBitSetMapping.class;
        }
        else if (mappingClass.equals(StringMapping.class))
        {
            // Convert into OracleStringMapping if storing the string as a BLOB/CLOB somewhere
            String jdbcType = null;
            if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT ||
                fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
            {
                ElementMetaData elemmd = (mmd != null ? mmd.getElementMetaData() : null);
                if (elemmd != null && elemmd.getColumnMetaData() != null && elemmd.getColumnMetaData().length > 0)
                {
                    jdbcType = elemmd.getColumnMetaData()[0].getJdbcTypeName();
                }
            }
            else if (fieldRole == FieldRole.ROLE_MAP_KEY)
            {
                KeyMetaData keymd = (mmd != null ? mmd.getKeyMetaData() : null);
                if (keymd != null && keymd.getColumnMetaData() != null && keymd.getColumnMetaData().length > 0)
                {
                    jdbcType = keymd.getColumnMetaData()[0].getJdbcTypeName();
                }
            }
            else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                ValueMetaData valmd = (mmd != null ? mmd.getValueMetaData() : null);
                if (valmd != null && valmd.getColumnMetaData() != null && valmd.getColumnMetaData().length > 0)
                {
                    jdbcType = valmd.getColumnMetaData()[0].getJdbcTypeName();
                }
            }
            else
            {
                if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                {
                    jdbcType = mmd.getColumnMetaData()[0].getJdbcTypeName();
                }
            }

            if (jdbcType != null)
            {
                String jdbcTypeLower = jdbcType.toLowerCase();
                if (jdbcTypeLower.indexOf("blob") >= 0 || jdbcTypeLower.indexOf("clob") >= 0)
                {
                    // BLOB/CLOB, so use OracleString
                    return OracleStringMapping.class;
                }
            }
            return mappingClass;
        }
        else if (mappingClass.equals(SerialisedMapping.class))
        {
            return OracleSerialisedObjectMapping.class;
        }
        else if (mappingClass.equals(SerialisedPCMapping.class))
        {
            return OracleSerialisedPCMapping.class;
        }
        else if (mappingClass.equals(ArrayMapping.class))
        {
            return OracleArrayMapping.class;
        }
        else if (mappingClass.equals(MapMapping.class))
        {
            return OracleMapMapping.class;
        }
        else if (mappingClass.equals(CollectionMapping.class))
        {
            return OracleCollectionMapping.class;
        }
        else
        {
            return mappingClass;
        }
    }
}