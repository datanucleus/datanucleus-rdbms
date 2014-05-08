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
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.util.ClassUtils;

/**
 * Maps to identity objects of reference values.
 * Used only from within JDOQL queries on JDOHelper.getObjectId expressions
 */
public class ReferenceIdMapping extends ReferenceMapping
{
    /**
     * Constructor used to generate a ReferenceMapping representing only the identity of the object.
     * This is typically used where the user has selected the id in a JDOQL query as a result field.
     * @param refMapping The mapping to base it on
     */
    public ReferenceIdMapping(ReferenceMapping refMapping)
    {
        super();
        initialize(refMapping.storeMgr, refMapping.type);
        table = refMapping.table;

        // Add the same field mappings to the identity
        javaTypeMappings = new JavaTypeMapping[refMapping.javaTypeMappings.length]; 
        System.arraycopy(refMapping.javaTypeMappings, 0, javaTypeMappings, 0, javaTypeMappings.length);
    }

    /**
     * Returns an identity for a persistable class.
     * Processes a FK field and finds the object that it relates to, then returns the identity.
     * @param ec execution context
     * @param rs The ResultSet
     * @param param Array of parameter ids in the ResultSet to retrieve
     * @return The id of the PC object.
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
     * @param ec execution context
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

        if (mappingStrategy == ID_MAPPING || mappingStrategy == XCALIA_MAPPING)
        {
            String refString = getReferenceStringForObject(ec, value);
            getJavaTypeMapping()[0].setString(ec, ps, param, refString);
            return;
        }
        else
        {
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            int colPos = 0;
            for (int i=0; i<javaTypeMappings.length; i++)
            {
                int[] cols = new int[javaTypeMappings[i].getNumberOfDatastoreMappings()];
                for (int j=0;j<cols.length;j++)
                {
                    cols[j] = param[colPos++];
                }

                Class cls = clr.classForName(javaTypeMappings[i].getType());
                AbstractClassMetaData implCmd = ec.getMetaDataManager().getMetaDataForClass(cls, clr);
                if (implCmd.getObjectidClass().equals(value.getClass().getName()))
                {
                    if (IdentityUtils.isDatastoreIdentity(value))
                    {
                        Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(value);
                        if (key instanceof String)
                        {
                            javaTypeMappings[i].setString(ec, ps, cols, (String)key);
                        }
                        else
                        {
                            javaTypeMappings[i].setObject(ec, ps, cols, key);
                        }
                    }
                    else if (IdentityUtils.isSingleFieldIdentity(value))
                    {
                        Object key = IdentityUtils.getTargetKeyForSingleFieldIdentity(value);
                        if (key instanceof String)
                        {
                            javaTypeMappings[i].setString(ec, ps, cols, (String)key);
                        }
                        else
                        {
                            javaTypeMappings[i].setObject(ec, ps, cols, key);
                        }
                    }
                    else
                    {
                        // TODO Cater for compound identity
                        String[] pkMemberNames = implCmd.getPrimaryKeyMemberNames();
                        for (int j=0;j<pkMemberNames.length;j++)
                        {
                            Object pkMemberValue = ClassUtils.getValueForIdentityField(value, pkMemberNames[j]);
                            if (pkMemberValue instanceof Byte)
                            {
                                getDatastoreMapping(j).setByte(ps, param[j], (Byte)pkMemberValue);
                            }
                            else if (pkMemberValue instanceof Character)
                            {
                                getDatastoreMapping(j).setChar(ps, param[j], (Character)pkMemberValue);
                            }
                            else if (pkMemberValue instanceof Integer)
                            {
                                getDatastoreMapping(j).setInt(ps, param[j], (Integer)pkMemberValue);
                            }
                            else if (pkMemberValue instanceof Long)
                            {
                                getDatastoreMapping(j).setLong(ps, param[j], (Long)pkMemberValue);
                            }
                            else if (pkMemberValue instanceof Short)
                            {
                                getDatastoreMapping(j).setShort(ps, param[j], (Short)pkMemberValue);
                            }
                            else if (pkMemberValue instanceof String)
                            {
                                getDatastoreMapping(j).setString(ps, param[j], (String)pkMemberValue);
                            }
                            else
                            {
                                getDatastoreMapping(j).setObject(ps, param[j], pkMemberValue);
                            }
                        }
                    }
                }
                else
                {
                    // Set columns to null for this implementation
                    javaTypeMappings[i].setObject(ec, ps, cols, null);
                }
            }
        }
    }
}