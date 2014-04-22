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
2009 Andy Jefferson - rewritten to cater for all possible types of identity, and set method
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.util.ClassUtils;

/**
 * Maps to identity objects of persistable values.
 * Used only from within JDOQL queries on JDOHelper.getObjectId expressions
 */
public class PersistableIdMapping extends PersistableMapping
{
    /**
     * Constructor used to generate a PCMapping representing only the identity of the object.
     * This is typically used where the user has selected the id in a JDOQL query as a result field.
     * @param pcMapping The mapping to base it on
     */
    public PersistableIdMapping(PersistableMapping pcMapping)
    {
        super();
        initialize(pcMapping.storeMgr, pcMapping.type);
        table = pcMapping.table;

        // Add the same field mappings to the identity
        javaTypeMappings = new JavaTypeMapping[pcMapping.javaTypeMappings.length]; 
        System.arraycopy(pcMapping.javaTypeMappings, 0, javaTypeMappings, 0, javaTypeMappings.length);
    }

    /**
     * Returns an identity for a persistable class.
     * Processes a FK field and finds the object that it relates to, then returns the identity.
     * @param ec The ExecutionContext
     * @param rs The ResultSet
     * @param param Array of parameter ids in the ResultSet to retrieve
     * @return The identity of the Persistence Capable object
     */
    public Object getObject(ExecutionContext ec, final ResultSet rs, int[] param)
    {
        Object value = super.getObject(ec, rs, param);
        if (value != null)
        {
            ApiAdapter api = ec.getApiAdapter();
            return api.getIdForObject(value);
        }
        return null;
    }

    /**
     * Method to set the object based on an input identity.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param param Parameter positions to populate when setting the value
     * @param value The identity
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] param, Object value)
    {
        if (value == null)
        {
            super.setObject(ec, ps, param, null);
            return;
        }

        if (cmd == null)
        {
            cmd = ec.getMetaDataManager().getMetaDataForClass(getType(),ec.getClassLoaderResolver());
        }

        if (value instanceof OID)
        {
            if (getJavaTypeMapping()[0] instanceof OIDMapping)
            {
                getJavaTypeMapping()[0].setObject(ec, ps, param, value);
            }
            else
            {
                Object key = ((OID)value).getKeyValue();
                if (key instanceof String)
                {
                    getJavaTypeMapping()[0].setString(ec, ps, param, (String)key);
                }
                else
                {
                    getJavaTypeMapping()[0].setObject(ec, ps, param, key);
                }
            }
        }
        else if (IdentityUtils.isSingleFieldIdentity(value))
        {
            Object key = IdentityUtils.getTargetKeyForSingleFieldIdentity(value);
            if (key instanceof String)
            {
                getJavaTypeMapping()[0].setString(ec, ps, param, (String)key);
            }
            else
            {
                getJavaTypeMapping()[0].setObject(ec, ps, param, key);
            }
        }
        else
        {
            // TODO Cater for compound identity
            String[] pkMemberNames = cmd.getPrimaryKeyMemberNames();
            for (int i=0;i<pkMemberNames.length;i++)
            {
                Object pkMemberValue = ClassUtils.getValueForIdentityField(value, pkMemberNames[i]);
                if (pkMemberValue instanceof Byte)
                {
                    getDatastoreMapping(i).setByte(ps, param[i], (Byte)pkMemberValue);
                }
                else if (pkMemberValue instanceof Character)
                {
                    getDatastoreMapping(i).setChar(ps, param[i], (Character)pkMemberValue);
                }
                else if (pkMemberValue instanceof Integer)
                {
                    getDatastoreMapping(i).setInt(ps, param[i], (Integer)pkMemberValue);
                }
                else if (pkMemberValue instanceof Long)
                {
                    getDatastoreMapping(i).setLong(ps, param[i], (Long)pkMemberValue);
                }
                else if (pkMemberValue instanceof Short)
                {
                    getDatastoreMapping(i).setShort(ps, param[i], (Short)pkMemberValue);
                }
                else if (pkMemberValue instanceof String)
                {
                    getDatastoreMapping(i).setString(ps, param[i], (String)pkMemberValue);
                }
                else
                {
                    getDatastoreMapping(i).setObject(ps, param[i], pkMemberValue);
                }
            }
        }
    }
}