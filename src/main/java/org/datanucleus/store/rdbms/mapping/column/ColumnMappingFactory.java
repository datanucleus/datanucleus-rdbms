/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved. 
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
2003 Andy Jefferson - converted to use Reflection
2004 Andy Jefferson - changed to give targetException on Invocation error
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.column;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Factory class for creating ColumnMapping instances.
 */
public final class ColumnMappingFactory
{
    /** Private constructor to prevent instantiation. */
    private ColumnMappingFactory()
    {
    }

    /** cache of constructors keyed by mapping class **/
    private static Map<Class<? extends ColumnMapping>, Constructor> DATASTORE_MAPPING_CONSTRUCTOR_BY_CLASS = new ConcurrentHashMap<>();

    /** constructor arguments **/
    private static final Class[] DATASTORE_MAPPING_CTR_ARG_CLASSES = new Class[] {JavaTypeMapping.class, RDBMSStoreManager.class, Column.class};

    /**
     * Get a new instance of the ColumnMapping using the mapping, StoreManager and column.
     * @param mappingClass the Mapping class to be created
     * @param mapping The java mapping type
     * @param storeMgr The Store Manager
     * @param column The column to map
     * @return The ColumnMapping
     */
    public static ColumnMapping createMapping(Class<? extends ColumnMapping> mappingClass, JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column column)
    {
        Object obj = null;
        try
        {
            Object[] args = new Object[]{mapping, storeMgr, column};
            Constructor ctr = DATASTORE_MAPPING_CONSTRUCTOR_BY_CLASS.get(mappingClass);
            if (ctr == null)
            {
                ctr = mappingClass.getConstructor(DATASTORE_MAPPING_CTR_ARG_CLASSES);
                DATASTORE_MAPPING_CONSTRUCTOR_BY_CLASS.put(mappingClass, ctr);
            }
            try
            {
                obj = ctr.newInstance(args);
            }
            catch (InvocationTargetException e)
            {
                throw new NucleusException(Localiser.msg("041009", mappingClass.getName(), e.getTargetException()), e.getTargetException()).setFatal();
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mappingClass.getName(), e), e).setFatal();
            }
        }
        catch (NoSuchMethodException nsme)
        {
            throw new NucleusException(Localiser.msg("041007", JavaTypeMapping.class, RDBMSStoreManager.class, Column.class, mappingClass.getName())).setFatal();
        }
        return (ColumnMapping) obj;
    }
}