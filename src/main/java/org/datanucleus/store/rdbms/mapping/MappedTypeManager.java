/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Registry of java type mapping support. Provides a lookup between java type and the mapping to use
 * for that type. Uses the plugin mechanism extension-point "org.datanucleus.store.rdbms.java_mapping".
 */
public class MappedTypeManager
{
    protected final NucleusContext nucleusCtx;

    protected final ClassLoaderResolver clr;

    /** The mapped types, keyed by the class name. */
    Map<String, MappedType> mappedTypes = new HashMap();

    /**
     * Constructor, loading support for type mappings using the plugin mechanism.
     * @param nucleusCtx Context in use
     */
    public MappedTypeManager(NucleusContext nucleusCtx)
    {
        this.nucleusCtx = nucleusCtx;
        this.clr = nucleusCtx.getClassLoaderResolver(null);
        loadMappings(nucleusCtx.getPluginManager(), clr);
    }

    /**
     * Accessor for whether a class is supported as being mappable.
     * @param className The class name
     * @return Whether the class is supported (to some degree)
     */
    public boolean isSupportedMappedType(String className)
    {
        if (className == null)
        {
            return false;
        }

        MappedType type = getMappedType(className);
        if (type == null)
        {
            try
            {
                Class cls = clr.classForName(className);
                type = findMappedTypeForClass(cls);
                return (type != null && type.javaMappingType != null);
            }
            catch (Exception e)
            {
            }
            return false;
        }

        return (type.javaMappingType != null);
    }

    /**
     * Accessor for the Java Mapping type class for the supplied class.
     * @param className The class name
     * @return The Java mapping type
     */
    public Class getMappingType(String className)
    {
        if (className == null)
        {
            return null;
        }

        MappedType type = getMappedType(className);
        if (type == null)
        {
            // Check if this is a SCO wrapper
            TypeManager typeMgr = nucleusCtx.getTypeManager();
            Class cls = typeMgr.getTypeForSecondClassWrapper(className);
            if (cls != null)
            {
                // Supplied class is a SCO wrapper, so return the java type mapping for the underlying java type
                type = getMappedType(cls.getName());
                if (type != null)
                {
                    return type.javaMappingType;
                }
            }

            // Not SCO wrapper so find a type
            try
            {
                cls = clr.classForName(className);
                type = findMappedTypeForClass(cls);
                return type.javaMappingType;
            }
            catch (Exception e)
            {
                return null;
            }
        }

        return type.javaMappingType;
    }

    /**
     * Method to load the user type mappings that are currently registered in the PluginManager.
     * @param mgr the PluginManager
     * @param clr the ClassLoaderResolver
     */
    private void loadMappings(PluginManager mgr, ClassLoaderResolver clr)
    {
        ConfigurationElement[] elems = mgr.getConfigurationElementsForExtension("org.datanucleus.store.rdbms.java_mapping", null, null);
        if (elems != null)
        {
            for (int i=0;i<elems.length;i++)
            {
                String javaName = elems[i].getAttribute("java-type").trim();
                String mappingClassName = elems[i].getAttribute("mapping-class");

                if (!mappedTypes.containsKey(javaName)) // Use "priority" attribute to be placed higher in the list
                {
                    addMappedType(mgr, elems[i].getExtension().getPlugin().getSymbolicName(), javaName, 
                        mappingClassName, clr);
                }
            }
        }
    }

    /**
     * Definition of a java type that can be "mapped".
     * The type is supported to some degree, maybe as FCO or maybe as SCO.
     */
    static class MappedType
    {
        /** supported class. */
        final Class cls;
        /** mapping class. An extension of {@link org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping}*/
        final Class javaMappingType;

        /**
         * Constructor.
         * @param cls the java type class being supported
         * @param mappingType the mapping class. An extension of {@link org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping}
         */
        public MappedType(Class cls, Class mappingType)
        {
            this.cls = cls;
            this.javaMappingType = mappingType;
        }

        public String toString()
        {
            StringBuilder str = new StringBuilder("MappedType " + cls.getName() + " [");
            if (javaMappingType != null)
            {
                str.append(" mapping=" + javaMappingType);
            }
            str.append("]");
            return str.toString();
        }
    }

    /**
     * Method to add support for a Java class (to some degree).
     * @param mgr The PluginManager
     * @param pluginId the plug-in id
     * @param className Name of the class to add
     * @param mappingClassName The Java mapping type
     * @param clr the ClassLoaderResolver
     */
    private void addMappedType(PluginManager mgr, String pluginId, String className, String mappingClassName,
            ClassLoaderResolver clr)
    {
        if (className == null)
        {
            return;
        }

        Class mappingType = null;
        if (!StringUtils.isWhitespace(mappingClassName))
        {
            try
            {
                mappingType = mgr.loadClass(pluginId,mappingClassName);
            }
            catch (NucleusException jpe)
            {
                NucleusLogger.PERSISTENCE.error(Localiser.msg("016004", mappingClassName));
                return;
            }
        }

        Class cls = null;
        try
        {
            cls = clr.classForName(className);
        }
        catch (Exception e)
        {
            // Class not found so ignore. Should log this
        }
        if (cls != null)
        {
            MappedType type = new MappedType(cls, mappingType);
            mappedTypes.put(className, type);
        }
    }

    protected MappedType findMappedTypeForClass(Class cls)
    {
        MappedType type = getMappedType(cls.getName());
        if (type != null)
        {
            return type;
        }

        // Not supported so try to find one that is supported that this class derives from
        Class componentCls = (cls.isArray() ? cls.getComponentType() : null);
        Collection supportedTypes = new HashSet(mappedTypes.values());
        Iterator<MappedType> iter = supportedTypes.iterator();
        while (iter.hasNext())
        {
            type = iter.next();
            if (type.cls == cls)
            {
                return type;
            }
            if (!type.cls.getName().equals("java.lang.Object") && !type.cls.getName().equals("java.io.Serializable"))
            {
                if (componentCls != null)
                {
                    // Array type
                    if (type.cls.isArray() && type.cls.getComponentType().isAssignableFrom(componentCls))
                    {
                        mappedTypes.put(cls.getName(), type);
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("016001", cls.getName(), type.cls.getName()));
                        }
                        return type;
                    }
                }
                else
                {
                    // Basic type
                    if (type.cls.isAssignableFrom(cls))
                    {
                        mappedTypes.put(cls.getName(), type);
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("016001", cls.getName(), type.cls.getName()));
                        }
                        return type;
                    }
                }
            }
        }

        // Not supported
        return null;
    }

    /**
     * Utility class to retrieve a supported type
     * @param className The class name
     * @return The internal type information for the class
     */
    protected MappedType getMappedType(String className)
    {
        if (className == null)
        {
            return null;
        }

        return mappedTypes.get(className);
    }
}