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
2003 Erik Bengtson - removed unused import
2003 Andy Jefferson - coding standards
2003 Andy Jefferson - updated setObject to use all input "params"
2004 Andy Jefferson - fixes to allow full use of Long/String OIDs
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NotYetFlushedException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping for datastore identity "field".
 */
public class DatastoreIdMapping extends SingleFieldMapping
{
    /**
     * Mutator for the OID in the datastore
     * @param ec ExecutionContext
     * @param ps The Prepared Statement
     * @param param Param numbers in the PreparedStatement for this object
     * @param value The OID value to use
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] param, Object value)
    {
        if (value == null)
        {
            getDatastoreMapping(0).setObject(ps, param[0], null);
        }
        else
        {
            ApiAdapter api = ec.getApiAdapter();
            Object id;
            if (api.isPersistable(value))
            {
                id = api.getIdForObject(value);
                if (id == null)
                {
                    if (ec.isInserting(value))
                    {
                        // Object is in the process of being inserted, but has no id yet so provide a null for now
                        // The "NotYetFlushedException" is caught by ParameterSetter and processed as an update being required.
                        getDatastoreMapping(0).setObject(ps, param[0], null);
                        throw new NotYetFlushedException(value);
                    }

                    // Object is not persist, nor in the process of being made persistent
                    ec.persistObjectInternal(value, null, -1, ObjectProvider.PC);
                    ec.flushInternal(false);
                }
                id = api.getIdForObject(value);
            }
            else
            {
                id = value;
            }

            Object idKey = IdentityUtils.getTargetKeyForDatastoreIdentity(id);
            try
            {
                // Try as a Long
                getDatastoreMapping(0).setObject(ps,param[0], idKey);
            }
            catch (Exception e)
            {
                // Must be a String
                getDatastoreMapping(0).setObject(ps,param[0], idKey.toString());
            }
        }
    }

    /**
     * Accessor for the OID object from the result set
     * @param ec ExecutionContext managing this object
     * @param rs The ResultSet
     * @param param Array of param numbers for this object
     * @return The OID object
     */
    public Object getObject(ExecutionContext ec, ResultSet rs, int[] param)
    {
        Object value;
        if (getNumberOfDatastoreMappings() > 0)
        {
            value = getDatastoreMapping(0).getObject(rs,param[0]);
        }
        else
        {
            // 1-1 bidirectional "mapped-by" relation, so use ID mappings of related class to retrieve the value
        	if (referenceMapping != null) //TODO why is it null for PC concrete classes?
        	{
                return referenceMapping.getObject(ec, rs, param);
        	}

            Class fieldType = mmd.getType();
            JavaTypeMapping referenceMapping = storeMgr.getDatastoreClass(fieldType.getName(), ec.getClassLoaderResolver()).getIdMapping();
            value = referenceMapping.getDatastoreMapping(0).getObject(rs, param[0]);
        }

        if (value != null)
        {
            value = ec.getNucleusContext().getIdentityManager().getDatastoreId(getType(), value);
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("041034",value));
            }
        }

        return value;
    }

    public Class getJavaType()
    {
        return DatastoreId.class;
    }
}