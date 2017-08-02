/******************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved. 
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
2004 Erik Bengtson - addition of JDBC, SQL type maps
2004 Andy Jefferson - fixed use of SQL/JDBC to find the one applicable to
                      the specified field and type.
    ...
*****************************************************************/
package org.datanucleus.store.rdbms.mapping;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.ValueGenerationStrategy;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.mapping.datastore.DatastoreMapping;
import org.datanucleus.store.rdbms.mapping.datastore.DatastoreMappingFactory;
import org.datanucleus.store.rdbms.mapping.java.ArrayMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedKeyPCMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedValuePCMapping;
import org.datanucleus.store.rdbms.mapping.java.InterfaceMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ObjectMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedElementPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedKeyPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedLocalFileMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedValuePCMapping;
import org.datanucleus.store.rdbms.mapping.java.TypeConverterMapping;
import org.datanucleus.store.rdbms.mapping.java.TypeConverterMultiMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Default Mapping manager implementation.
 * Provides mappings from standard Java types (defined in org.datanucleus.store.rdbms.mapping.java) to
 * datastore mappings for JDBC types (defined in org.datanucleus.store.rdbms.mapping.datastore).
 */
public class MappingManagerImpl implements MappingManager
{
    protected final RDBMSStoreManager storeMgr;

    protected final ClassLoaderResolver clr;

    /** The mapped types, keyed by the class name. */
    protected Map<String, MappedType> mappedTypes = new ConcurrentHashMap();

    /**
     * Constructor for a mapping manager for an RDBMS datastore.
     * @param storeMgr The StoreManager
     */
    public MappingManagerImpl(RDBMSStoreManager storeMgr)
    {
        this.storeMgr = storeMgr;
        this.clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);

        // Load up the mappings of javaType -> JavaTypeMapping

        // a). load built-in mappings
        addMappedType(boolean.class, org.datanucleus.store.rdbms.mapping.java.BooleanMapping.class);
        addMappedType(byte.class, org.datanucleus.store.rdbms.mapping.java.ByteMapping.class);
        addMappedType(char.class, org.datanucleus.store.rdbms.mapping.java.CharacterMapping.class);
        addMappedType(double.class, org.datanucleus.store.rdbms.mapping.java.DoubleMapping.class);
        addMappedType(float.class,org.datanucleus.store.rdbms.mapping.java.FloatMapping.class);
        addMappedType(int.class, org.datanucleus.store.rdbms.mapping.java.IntegerMapping.class);
        addMappedType(long.class, org.datanucleus.store.rdbms.mapping.java.LongMapping.class);
        addMappedType(short.class, org.datanucleus.store.rdbms.mapping.java.ShortMapping.class);
        addMappedType(Boolean.class, org.datanucleus.store.rdbms.mapping.java.BooleanMapping.class);
        addMappedType(Byte.class, org.datanucleus.store.rdbms.mapping.java.ByteMapping.class);
        addMappedType(Character.class, org.datanucleus.store.rdbms.mapping.java.CharacterMapping.class);
        addMappedType(Double.class, org.datanucleus.store.rdbms.mapping.java.DoubleMapping.class);
        addMappedType(Float.class,org.datanucleus.store.rdbms.mapping.java.FloatMapping.class);
        addMappedType(Integer.class, org.datanucleus.store.rdbms.mapping.java.IntegerMapping.class);
        addMappedType(Long.class, org.datanucleus.store.rdbms.mapping.java.LongMapping.class);
        addMappedType(Short.class, org.datanucleus.store.rdbms.mapping.java.ShortMapping.class);
        addMappedType(Number.class, org.datanucleus.store.rdbms.mapping.java.NumberMapping.class);
        addMappedType(String.class, org.datanucleus.store.rdbms.mapping.java.StringMapping.class);
        addMappedType(Enum.class, org.datanucleus.store.rdbms.mapping.java.EnumMapping.class);
        addMappedType(Object.class, org.datanucleus.store.rdbms.mapping.java.SerialisedMapping.class);

        addMappedType(java.io.File.class, org.datanucleus.store.rdbms.mapping.java.FileMapping.class);
        addMappedType(java.io.Serializable.class, org.datanucleus.store.rdbms.mapping.java.SerialisedMapping.class);

        addMappedType(java.math.BigDecimal.class, org.datanucleus.store.rdbms.mapping.java.BigDecimalMapping.class);
        addMappedType(java.math.BigInteger.class, org.datanucleus.store.rdbms.mapping.java.BigIntegerMapping.class);

        addMappedType(java.util.Calendar.class, org.datanucleus.store.rdbms.mapping.java.GregorianCalendarMapping.class);
        addMappedType(java.util.GregorianCalendar.class, org.datanucleus.store.rdbms.mapping.java.GregorianCalendarMapping.class);
        addMappedType(java.util.Date.class, org.datanucleus.store.rdbms.mapping.java.DateMapping.class);
        addMappedType(java.util.UUID.class, org.datanucleus.store.rdbms.mapping.java.UUIDMapping.class);

        addMappedType(java.sql.Date.class, org.datanucleus.store.rdbms.mapping.java.SqlDateMapping.class);
        addMappedType(java.sql.Time.class, org.datanucleus.store.rdbms.mapping.java.SqlTimeMapping.class);
        addMappedType(java.sql.Timestamp.class, org.datanucleus.store.rdbms.mapping.java.SqlTimestampMapping.class);

        addMappedType(boolean[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(byte[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(char[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(double[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(float[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(int[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(long[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(short[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Boolean[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Byte[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Character[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Double[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Float[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Integer[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Long[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Short[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Number[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(String[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(Enum[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(java.math.BigDecimal[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(java.math.BigInteger[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(java.util.Date[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);
        addMappedType(java.util.Locale[].class, org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class);

        addMappedType(java.util.ArrayList.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.BitSet.class, org.datanucleus.store.rdbms.mapping.java.BitSetMapping.class);
        addMappedType(java.util.Collection.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.HashMap.class, org.datanucleus.store.rdbms.mapping.java.MapMapping.class);
        addMappedType(java.util.HashSet.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.Hashtable.class, org.datanucleus.store.rdbms.mapping.java.MapMapping.class);
        addMappedType(java.util.LinkedList.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.List.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.LinkedHashMap.class, org.datanucleus.store.rdbms.mapping.java.MapMapping.class);
        addMappedType(java.util.LinkedHashSet.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.Map.class, org.datanucleus.store.rdbms.mapping.java.MapMapping.class);
        addMappedType(java.util.PriorityQueue.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.Properties.class, org.datanucleus.store.rdbms.mapping.java.MapMapping.class);
        addMappedType(java.util.Queue.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.Set.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.SortedMap.class, org.datanucleus.store.rdbms.mapping.java.MapMapping.class);
        addMappedType(java.util.SortedSet.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.Stack.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.TreeMap.class, org.datanucleus.store.rdbms.mapping.java.MapMapping.class);
        addMappedType(java.util.TreeSet.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.Vector.class, org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class);
        addMappedType(java.util.Optional.class, org.datanucleus.store.rdbms.mapping.java.OptionalMapping.class);

        addMappedType(org.datanucleus.identity.DatastoreId.class, org.datanucleus.store.rdbms.mapping.java.DatastoreIdMapping.class);

        // b). Load any mappings from the plugin mechanism
        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();
        ConfigurationElement[] elems = pluginMgr.getConfigurationElementsForExtension("org.datanucleus.store.rdbms.java_mapping", null, null);
        if (elems != null)
        {
            for (int i=0;i<elems.length;i++)
            {
                String javaName = elems[i].getAttribute("java-type").trim();
                String mappingClassName = elems[i].getAttribute("mapping-class");

                if (javaName != null && !mappedTypes.containsKey(javaName) && !StringUtils.isWhitespace(mappingClassName)) // Use "priority" attribute to be placed higher in the list
                {
                    try
                    {
                        Class mappingType = pluginMgr.loadClass(elems[i].getExtension().getPlugin().getSymbolicName(), mappingClassName);
                        try
                        {
                            Class cls = clr.classForName(javaName);
                            if (cls != null)
                            {
                                mappedTypes.put(javaName, new MappedType(cls, mappingType));
                            }
                        }
                        catch (Exception e)
                        {
                            // Class not found so ignore. Should log this
                        }
                    }
                    catch (NucleusException jpe)
                    {
                        NucleusLogger.PERSISTENCE.error(Localiser.msg("016004", mappingClassName));
                    }
                }
            }
        }
    }

    protected void addMappedType(Class javaType, Class mappingType)
    {
        mappedTypes.put(javaType.getName(), new MappedType(javaType, mappingType));
    }

    /**
     * Accessor for whether a java type is supported as being mappable.
     * @param javaTypeName The java type name
     * @return Whether the class is supported (to some degree)
     */
    public boolean isSupportedMappedType(String javaTypeName)
    {
        if (javaTypeName == null)
        {
            return false;
        }

        MappedType type = mappedTypes.get(javaTypeName);
        if (type == null)
        {
            try
            {
                Class cls = clr.classForName(javaTypeName);
                type = findMappedTypeForClass(cls);
                return type != null && type.javaMappingType != null;
            }
            catch (Exception e)
            {
            }
            return false;
        }

        return type.javaMappingType != null;
    }

    /**
     * Accessor for the JavaTypeMapping class for the supplied java type.
     * @param javaTypeName The java type name
     * @return The Java mapping type
     */
    public Class getMappingType(String javaTypeName)
    {
        if (javaTypeName == null)
        {
            return null;
        }

        MappedType type = mappedTypes.get(javaTypeName);
        if (type == null)
        {
            // No explicit <java_mapping> so check for a default TypeConverter against the basic <java-type>
            TypeManager typeMgr = storeMgr.getNucleusContext().getTypeManager();
            TypeConverter defaultTypeConv = typeMgr.getDefaultTypeConverterForType(clr.classForName(javaTypeName));
            if (defaultTypeConv != null)
            {
                // We have no explicit mapping and there is a defined default TypeConverter so return and that will be picked
                return null;
            }

            // Check if this is a SCO wrapper
            Class cls = typeMgr.getTypeForSecondClassWrapper(javaTypeName);
            if (cls != null)
            {
                // Supplied class is a SCO wrapper, so return the java type mapping for the underlying java type
                type = mappedTypes.get(cls.getName());
                if (type != null)
                {
                    return type.javaMappingType;
                }
            }

            // Not SCO wrapper so find a type
            try
            {
                cls = clr.classForName(javaTypeName);
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
     * Definition of a java type that can be "mapped".
     * The type is supported to some degree, maybe as FCO or maybe as SCO.
     */
    static class MappedType
    {
        /** Supported java type. */
        final Class cls;

        /** Mapping class to use. An extension of {@link org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping}*/
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

    protected MappedType findMappedTypeForClass(Class cls)
    {
        MappedType type = mappedTypes.get(cls.getName());
        if (type != null)
        {
            return type;
        }

        // Not supported so try to find one that is supported that this class derives from
        Class componentCls = cls.isArray() ? cls.getComponentType() : null;
        Collection<MappedType> supportedTypes = new HashSet<>(mappedTypes.values());
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
                        return type;
                    }
                }
                else
                {
                    // Basic type
                    if (type.cls.isAssignableFrom(cls))
                    {
                        mappedTypes.put(cls.getName(), type);
                        return type;
                    }
                }
            }
        }

        // Not supported
        return null;
    }

    /**
     * Accessor for the mapping for the specified class. Usually only called by JDOQL query expressions.
     * If the type has its own table returns the id mapping of the table.
     * If the type doesn't have its own table then creates the mapping and, if it has a simple
     * datastore representation, creates the datastore mapping. The JavaTypeMapping has no metadata/table associated.
     * @param javaType Java type
     * @param serialised Whether the type is serialised
     * @param embedded Whether the type is embedded
     * @param clr ClassLoader resolver
     * @return The mapping for the class.
     */
    public JavaTypeMapping getMappingWithDatastoreMapping(Class javaType, boolean serialised, boolean embedded, ClassLoaderResolver clr)
    {
        try
        {
            // TODO This doesn't take into account serialised/embedded
            // If the type has its own table just take the id mapping of its table
            DatastoreClass datastoreClass = storeMgr.getDatastoreClass(javaType.getName(), clr);
            return datastoreClass.getIdMapping();
        }
        catch (NoTableManagedException ex)
        {
            // Doesn't allow for whether a field is serialised/embedded so they get the default mapping only
            MappingConverterDetails mcd = getMappingClass(javaType, serialised, embedded, null, null); // TODO Pass in 4th arg?

            Class mc = mcd.mappingClass;
            // Allow override by Oracle
            mc = getOverrideMappingClass(mc, null, null);
            try
            {
                JavaTypeMapping m = (JavaTypeMapping)mc.newInstance();
                m.initialize(storeMgr, javaType.getName());
                if (m.hasSimpleDatastoreRepresentation())
                {
                    // Create the datastore mapping (NOT the column)
                    createDatastoreMapping(m, null, m.getJavaTypeForDatastoreMapping(0));
                    // TODO How to handle SingleFieldMultiMapping cases ?
                }
                return m;
            }
            catch (NucleusUserException nue)
            {
                throw nue;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
            }
        }
    }

    /**
     * Accessor for the mapping for the specified class.
     * This simply creates a JavaTypeMapping for the java type and returns it. The mapping
     * has no underlying datastore mapping(s) and no associated field/table.
     * @param javaType Java type
     * @return The mapping for the class.
     */
    public JavaTypeMapping getMapping(Class javaType)
    {
        return getMapping(javaType, false, false, (String)null);
    }

    /**
     * Accessor for the mapping for the specified class.
     * This simply creates a JavaTypeMapping for the java type and returns it.
     * The mapping has no underlying datastore mapping(s) and no associated field/table.
     * @param javaType Java type
     * @param serialised Whether the type is serialised
     * @param embedded Whether the type is embedded
     * @param fieldName Name of the field (for logging)
     * @return The mapping for the class.
     */
    public JavaTypeMapping getMapping(Class javaType, boolean serialised, boolean embedded, String fieldName)
    {
        MappingConverterDetails mcd = getMappingClass(javaType, serialised, embedded, null, fieldName); // TODO Pass in 4th arg?
        Class mc = mcd.mappingClass;
        // Allow override by Oracle
        mc = getOverrideMappingClass(mc, null, null);
        try
        {
            JavaTypeMapping m = (JavaTypeMapping)mc.newInstance();
            m.initialize(storeMgr, javaType.getName());
            return m;
        }
        catch (Exception e)
        {
            throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
        }
    }

    /**
     * Accessor for the mapping for the member of the specified table.
     * Can be used for members of a class, element of a collection of a class, element of an array of a class, 
     * keys of a map of a class, values of a map of a class; this is controlled by the role argument.
     * @param table Table to add the mapping to
     * @param mmd MetaData for the member to map
     * @param clr The ClassLoaderResolver
     * @param fieldRole Role that this mapping plays for the field
     * @return The mapping for the member.
     */
    public JavaTypeMapping getMapping(Table table, AbstractMemberMetaData mmd, ClassLoaderResolver clr, FieldRole fieldRole)
    {
        if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT || fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            // Mapping a collection/array element (in a join table)
            return getElementMapping(table, mmd, fieldRole, clr);
        }
        else if (fieldRole == FieldRole.ROLE_MAP_KEY)
        {
            // Mapping a map key (in a join table)
            return getKeyMapping(table, mmd, clr);
        }
        else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
        {
            // Mapping a map value (in a join table)
            return getValueMapping(table, mmd, clr);
        }

        // Check for use of TypeConverter (either specific, or auto-apply for this type)
        TypeManager typeMgr = table.getStoreManager().getNucleusContext().getTypeManager();
        TypeConverter conv = null;
        if (!mmd.isTypeConversionDisabled())
        {
            // User-specified TypeConverter defined for the whole member, or an autoApply is present for this member type
            if (mmd.getTypeConverterName() != null)
            {
                conv = typeMgr.getTypeConverterForName(mmd.getTypeConverterName());
                if (conv == null)
                {
                    throw new NucleusUserException(Localiser.msg("044062", mmd.getFullFieldName(), mmd.getTypeConverterName()));
                }
            }
            else
            {
                TypeConverter autoApplyConv = typeMgr.getAutoApplyTypeConverterForType(mmd.getType());
                if (autoApplyConv != null)
                {
                    conv = autoApplyConv;
                }
            }

            if (conv != null)
            {
                // Create the mapping of the selected type
                JavaTypeMapping m = null;
                if (conv instanceof MultiColumnConverter)
                {
                    Class mc = TypeConverterMultiMapping.class;
                    try
                    {
                        m = (JavaTypeMapping)mc.newInstance();
                        m.setRoleForMember(FieldRole.ROLE_FIELD);
                        ((TypeConverterMultiMapping)m).initialize(mmd, table, clr, conv);
                        return m;
                    }
                    catch (Exception e)
                    {
                        throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
                    }
                }

                Class mc = TypeConverterMapping.class;
                try
                {
                    m = (JavaTypeMapping)mc.newInstance();
                    m.setRoleForMember(FieldRole.ROLE_FIELD);
                    ((TypeConverterMapping)m).initialize(mmd, table, clr, conv);
                    return m;
                }
                catch (Exception e)
                {
                    throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
                }
            }
        }

        AbstractMemberMetaData overrideMmd = null;

        MappingConverterDetails mcd = null;
        Class mc = null;
        String userMappingClassName = mmd.getValueForExtension("mapping-class");
        if (userMappingClassName != null)
        {
            // User has defined their own mapping class for this field so use that
            try
            {
                mc = clr.classForName(userMappingClassName);
            }
            catch (NucleusException ne)
            {
                throw new NucleusUserException(Localiser.msg("041014", mmd.getFullFieldName(), userMappingClassName)).setFatal();
            }
        }
        else
        {
            AbstractClassMetaData typeCmd = null;
            if (mmd.getType().isInterface())
            {
                typeCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForInterface(mmd.getType(), clr);
            }
            else
            {
                typeCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            }

            if (mmd.hasExtension(SerialisedLocalFileMapping.EXTENSION_SERIALIZE_TO_FOLDER) && Serializable.class.isAssignableFrom(mmd.getType()))
            {
                // Special case : use file serialization mapping
                mc = SerialisedLocalFileMapping.class;
            }
            else if (mmd.isSerialized())
            {
                // Field is marked as serialised then we have no other option - serialise it
                mcd = getMappingClass(mmd.getType(), true, false, null, mmd.getFullFieldName());
            }
            else if (mmd.getEmbeddedMetaData() != null)
            {
                // Field has an <embedded> specification so use that
                mcd = getMappingClass(mmd.getType(), false, true, null, mmd.getFullFieldName());
            }
            else if (typeCmd != null && typeCmd.isEmbeddedOnly())
            {
                // Reference type is declared with embedded only
                mcd = getMappingClass(mmd.getType(), false, true, null, mmd.getFullFieldName());
            }
            else if (mmd.isEmbedded()) // TODO Check this since it will push all basic nonPC fields through here
            {
                // Otherwise, if the field is embedded then we request that it be serialised into the owner table
                // This is particularly for java.lang.Object which should be "embedded" by default, and hence serialised
                mcd = getMappingClass(mmd.getType(), true, false, mmd.getColumnMetaData(), mmd.getFullFieldName());
            }
            else
            {
                // Non-embedded/non-serialised - Just get the basic mapping for the type
                Class memberType = mmd.getType();
                mcd = getMappingClass(memberType, false, false, mmd.getColumnMetaData(), mmd.getFullFieldName());

                if (mmd.getParent() instanceof EmbeddedMetaData && mmd.getRelationType(clr) != RelationType.NONE)
                {
                    // See NUCCORE-697 - always need to use the real member metadata for the mapping
                    // so that it can find sub-fields when persisting/querying etc
                    AbstractClassMetaData cmdForFmd = table.getStoreManager().getMetaDataManager().getMetaDataForClass(mmd.getClassName(), clr);
                    overrideMmd = cmdForFmd.getMetaDataForMember(mmd.getName());
                }
            }
        }

        // Create the mapping of the selected type
        if (mcd != null)
        {
            mc = mcd.mappingClass;
            // Allow override by Oracle
            mc = getOverrideMappingClass(mc, mmd, fieldRole);
        }

        if (mc != null && (mcd == null || mcd.typeConverter == null))
        {
            try
            {
                JavaTypeMapping m = (JavaTypeMapping)mc.newInstance();
                m.setRoleForMember(FieldRole.ROLE_FIELD);
                m.initialize(mmd, table, clr);
                if (overrideMmd != null)
                {
                    // Note cannot just use this overrideMmd in the initialize(...) call above, a test fails.
                    m.setMemberMetaData(overrideMmd);
                }
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
            }
        }
        else if (mcd != null && mcd.typeConverter != null)
        {
            try
            {
                JavaTypeMapping m = (JavaTypeMapping)mcd.mappingClass.newInstance();
                m.setRoleForMember(FieldRole.ROLE_FIELD);
                if (m instanceof TypeConverterMapping)
                {
                    ((TypeConverterMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                else if (m instanceof TypeConverterMultiMapping)
                {
                    ((TypeConverterMultiMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                if (overrideMmd != null)
                {
                    // Note cannot just use this overrideMmd in the initialize(...) call above, a test fails.
                    m.setMemberMetaData(overrideMmd);
                }
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
            }
        }

        throw new NucleusException("Unable to create mapping for member at " + mmd.getFullFieldName() + " - no available mapping");
    }

    /**
     * Convenience method to allow overriding of particular mapping classes. This is currently only required for Oracle due to non-standard BLOB handlers.
     * @param mappingClass The mapping class selected
     * @param mmd Meta data for the member (if appropriate)
     * @param fieldRole Role for the field (e.g collection element)
     * @return The mapping class to use
     */
    protected Class getOverrideMappingClass(Class mappingClass, AbstractMemberMetaData mmd, FieldRole fieldRole)
    {
        return mappingClass;
    }

    public class MappingConverterDetails
    {
        Class mappingClass;
        TypeConverter typeConverter;
        public MappingConverterDetails(Class mappingCls)
        {
            this.mappingClass = mappingCls;
        }
        public MappingConverterDetails(Class mappingCls, TypeConverter typeConv)
        {
            this.mappingClass = mappingCls;
            this.typeConverter = typeConv;
        }
    }

    /**
     * Accessor for the mapping class for the specified type.
     * Provides special handling for interface types and for classes that are being embedded in a field.
     * Refers others to its mapping manager lookup.
     * @param javaType Class to query
     * @param serialised Whether the field is serialised
     * @param embedded Whether the field is embedded
     * @param colmds Metadata for column(s) (optional)
     * @param fieldName The full field name (for logging only)
     * @return The mapping class for the class
     **/
    protected MappingConverterDetails getMappingClass(Class javaType, boolean serialised, boolean embedded, ColumnMetaData[] colmds, String fieldName)
    {
        ApiAdapter api = storeMgr.getApiAdapter();
        if (api.isPersistable(javaType))
        {
            // Persistence Capable field
            if (serialised)
            {
                // Serialised PC field
                return new MappingConverterDetails(SerialisedPCMapping.class);
            }
            else if (embedded)
            {
                // Embedded PC field
                return new MappingConverterDetails(EmbeddedPCMapping.class);
            }
            else
            {
                // PC field
                return new MappingConverterDetails(PersistableMapping.class);
            }
        }

        if (javaType.isInterface() && !storeMgr.getMappingManager().isSupportedMappedType(javaType.getName()))
        {
            // Interface field
            if (serialised)
            {
                // Serialised Interface field
                return new MappingConverterDetails(SerialisedReferenceMapping.class);
            }
            else if (embedded)
            {
                // Embedded interface field - just default to an embedded PCMapping!
                return new MappingConverterDetails(EmbeddedPCMapping.class);
            }
            else
            {
                // Interface field
                return new MappingConverterDetails(InterfaceMapping.class);
            }
        }

        if (javaType == java.lang.Object.class)
        {
            // Object field
            if (serialised)
            {
                // Serialised Object field
                return new MappingConverterDetails(SerialisedReferenceMapping.class);
            }
            else if (embedded)
            {
                // Embedded Object field - do we ever want to support this ? I think not ;-)
                throw new NucleusUserException(Localiser.msg("041042", fieldName)).setFatal();
            }
            else
            {
                // Object field as reference to PC object
                return new MappingConverterDetails(ObjectMapping.class);
            }
        }

        if (javaType.isArray())
        {
            // Array field
            if (api.isPersistable(javaType.getComponentType()))
            {
                // Array of PC objects
                return new MappingConverterDetails(ArrayMapping.class);
            }
            else if (javaType.getComponentType().isInterface() &&
                !storeMgr.getMappingManager().isSupportedMappedType(javaType.getComponentType().getName()))
            {
                // Array of interface objects
                return new MappingConverterDetails(ArrayMapping.class);
            }
            else if (javaType.getComponentType() == java.lang.Object.class)
            {
                // Array of Object reference objects
                return new MappingConverterDetails(ArrayMapping.class);
            }
            // Other array types will be caught by the default mappings
        }

        // Find a suitable mapping for the type and column definition (doesn't allow for serialised setting)
        MappingConverterDetails mcd = getDefaultJavaTypeMapping(javaType, colmds);
        if (mcd == null || mcd.mappingClass == null)
        {
            Class superClass = javaType.getSuperclass();
            while (superClass != null && !superClass.getName().equals(ClassNameConstants.Object) && (mcd == null || mcd.mappingClass == null))
            {
                mcd = getDefaultJavaTypeMapping(superClass, colmds);
                superClass = superClass.getSuperclass();
            }
        }
        if (mcd == null)
        {
            if (storeMgr.getMappingManager().isSupportedMappedType(javaType.getName()))
            {
                // "supported" type yet no FCO mapping !
                throw new NucleusUserException(Localiser.msg("041001", fieldName, javaType.getName()));
            }

            Class superClass = javaType; // start in this class
            while (superClass!=null && !superClass.getName().equals(ClassNameConstants.Object) && (mcd == null || mcd.mappingClass == null))
            {
                Class[] interfaces = superClass.getInterfaces();
                for( int i=0; i<interfaces.length && (mcd == null || mcd.mappingClass == null); i++)
                {
                    mcd = getDefaultJavaTypeMapping(interfaces[i], colmds);
                }
                superClass = superClass.getSuperclass();
            }
            if (mcd == null)
            {
                //TODO if serialised == false, should we raise an exception?
                // Treat as serialised
                return new MappingConverterDetails(SerialisedMapping.class);
            }
        }
        return mcd;
    }

    /**
     * Convenience accessor for the element mapping for the element of a collection/array of elements.
     * @param table The table
     * @param mmd MetaData for the collection member containing the collection/array of PCs
     * @param fieldRole role of this mapping for this member
     * @param clr ClassLoader resolver
     * @return The mapping
     */
    protected JavaTypeMapping getElementMapping(Table table, AbstractMemberMetaData mmd, FieldRole fieldRole, ClassLoaderResolver clr)
    {
        if (!mmd.hasCollection() && !mmd.hasArray())
        {
            // TODO Localise this message
            throw new NucleusException("Attempt to get element mapping for field " + mmd.getFullFieldName() + " that has no collection/array!").setFatal();
        }
        if (mmd.getJoinMetaData() == null)
        {
            AbstractMemberMetaData[] refMmds = mmd.getRelatedMemberMetaData(clr);
            if (refMmds == null || refMmds.length == 0)
            {
                // TODO Localise this
                throw new NucleusException("Attempt to get element mapping for field " + mmd.getFullFieldName() + 
                    " that has no join table defined for the collection/array").setFatal();
            }

            if (refMmds[0].getJoinMetaData() == null)
            {
                // TODO Localise this
                throw new NucleusException("Attempt to get element mapping for field " + mmd.getFullFieldName() + 
                        " that has no join table defined for the collection/array").setFatal();
            }
        }

        MappingConverterDetails mcd = null;
        Class mc = null;
        String userMappingClassName = null;
        String userTypeConverterName = null;
        if (mmd.getElementMetaData() != null)
        {
            userTypeConverterName = mmd.getElementMetaData().getValueForExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME);
            userMappingClassName = mmd.getElementMetaData().getValueForExtension("mapping-class");
        }
        if (userTypeConverterName != null)
        {
            TypeConverter conv = storeMgr.getNucleusContext().getTypeManager().getTypeConverterForName(userTypeConverterName);
            if (conv == null)
            {
                throw new NucleusUserException("Field " + mmd.getFullFieldName() + " ELEMENT has been specified to use type converter " + userTypeConverterName + " but not found!");
            }
            mcd = new MappingConverterDetails(TypeConverterMapping.class, conv); // TODO Could be TypeConverterMultiMapping?
        }
        else if (userMappingClassName != null)
        {
            // User has defined their own mapping class for this element so use that
            try
            {
                mc = clr.classForName(userMappingClassName);
            }
            catch (NucleusException jpe)
            {
                throw new NucleusUserException(Localiser.msg("041014", userMappingClassName)).setFatal();
            }
        }
        else
        {
            boolean serialised = ((mmd.hasCollection() && mmd.getCollection().isSerializedElement()) ||
                    (mmd.hasArray() && mmd.getArray().isSerializedElement()));
            boolean embeddedPC = (mmd.hasCollection() && mmd.getCollection().elementIsPersistent() && mmd.getCollection().isEmbeddedElement()) ||
                    (mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null);
            boolean elementPC = ((mmd.hasCollection() && mmd.getCollection().elementIsPersistent()) ||
                    (mmd.hasArray() && mmd.getArray().elementIsPersistent()));
            boolean embedded = true;
            if (mmd.hasCollection())
            {
                embedded = mmd.getCollection().isEmbeddedElement();
            }
            else if (mmd.hasArray())
            {
                embedded = mmd.getArray().isEmbeddedElement();
            }

            Class elementCls = null;
            if (mmd.hasCollection())
            {
                elementCls = clr.classForName(mmd.getCollection().getElementType());
            }
            else if (mmd.hasArray())
            {
                // Use declared element type rather than any restricted type specified in metadata
                elementCls = mmd.getType().getComponentType();
            }
            boolean elementReference = ClassUtils.isReferenceType(elementCls);

            if (serialised)
            {
                if (elementPC)
                {
                    // Serialised PC element
                    mc = SerialisedElementPCMapping.class;
                }
                else if (elementReference)
                {
                    // Serialised Reference element
                    mc = SerialisedReferenceMapping.class;
                }
                else
                {
                    // Serialised Non-PC element
                    mc = SerialisedMapping.class;
                }
            }
            else if (embedded)
            {
                if (embeddedPC)
                {
                    // Embedded PC type
                    mc = EmbeddedElementPCMapping.class;
                }
                else if (elementPC)
                {
                    // "Embedded" PC type but no <embedded> so dont embed for now. Is this correct?
                    mc = PersistableMapping.class;
                }
                else
                {
                    // Embedded Non-PC type
                    mcd = getMappingClass(elementCls, serialised, embedded, mmd.getElementMetaData() != null ? mmd.getElementMetaData().getColumnMetaData() : null, mmd.getFullFieldName());
                }
            }
            else
            {
                // Normal element mapping
                mcd = getMappingClass(elementCls, serialised, embedded, mmd.getElementMetaData() != null ? mmd.getElementMetaData().getColumnMetaData() : null, mmd.getFullFieldName());
            }
        }

        if (mcd != null && mcd.typeConverter == null)
        {
            mc = mcd.mappingClass;
        }
        if (mc != null && (mcd == null || mcd.typeConverter == null))
        {
            // Create the mapping of the selected type
            JavaTypeMapping m = null;
            try
            {
                m = (JavaTypeMapping)mc.newInstance();
                m.setRoleForMember(fieldRole);
                m.initialize(mmd, table, clr);
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
            }
        }
        else if (mcd != null && mcd.typeConverter != null)
        {
            try
            {
                JavaTypeMapping m = (JavaTypeMapping)mcd.mappingClass.newInstance();
                m.setRoleForMember(fieldRole);
                if (m instanceof TypeConverterMapping)
                {
                    ((TypeConverterMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                else if (m instanceof TypeConverterMultiMapping)
                {
                    ((TypeConverterMultiMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc != null ? mc.getName() : null, e), e).setFatal();
            }
        }

        throw new NucleusException("Unable to create mapping for element of collection/array at " + mmd.getFullFieldName() + " - no available mapping");
    }

    /**
     * Convenience accessor for the mapping of the key of a map.
     * @param table The container
     * @param mmd MetaData for the field containing the map that this key is for
     * @param clr ClassLoader resolver
     * @return The mapping
     */
    protected JavaTypeMapping getKeyMapping(Table table, AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        if (mmd.getMap() == null)
        {
            // TODO Localise this
            throw new NucleusException("Attempt to get key mapping for field " + mmd.getFullFieldName() + " that has no map!").setFatal();
        }

        MappingConverterDetails mcd = null;
        Class mc = null;
        String userTypeConverterName = null;
        String userMappingClassName = null;
        if (mmd.getKeyMetaData() != null)
        {
            userTypeConverterName = mmd.getKeyMetaData().getValueForExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME);
            userMappingClassName = mmd.getKeyMetaData().getValueForExtension("mapping-class");
        }
        if (userTypeConverterName != null)
        {
            TypeConverter conv = storeMgr.getNucleusContext().getTypeManager().getTypeConverterForName(userTypeConverterName);
            if (conv == null)
            {
                throw new NucleusUserException("Field " + mmd.getFullFieldName() + " KEY has been specified to use type converter " + userTypeConverterName + " but not found!");
            }
            mcd = new MappingConverterDetails(TypeConverterMapping.class, conv); // TODO Could be TypeConverterMultiMapping?
        }
        else if (userMappingClassName != null)
        {
            // User has defined their own mapping class for this key so use that
            try
            {
                mc = clr.classForName(userMappingClassName);
            }
            catch (NucleusException jpe)
            {
                throw new NucleusUserException(Localiser.msg("041014", userMappingClassName)).setFatal();
            }
        }
        else
        {
            boolean serialised = (mmd.hasMap() && mmd.getMap().isSerializedKey());
            boolean embedded = (mmd.hasMap() && mmd.getMap().isEmbeddedKey());
            boolean embeddedPC = (mmd.hasMap() && mmd.getMap().keyIsPersistent() && mmd.getMap().isEmbeddedKey()) || 
                    (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getEmbeddedMetaData() != null);
            boolean keyPC = (mmd.hasMap() && mmd.getMap().keyIsPersistent());
            Class keyCls = clr.classForName(mmd.getMap().getKeyType());
            boolean keyReference = ClassUtils.isReferenceType(keyCls);

            if (serialised)
            {
                if (keyPC)
                {
                    // Serialised PC key
                    mc = SerialisedKeyPCMapping.class;
                }
                else if (keyReference)
                {
                    // Serialised Reference key
                    mc = SerialisedReferenceMapping.class;
                }
                else
                {
                    // Serialised Non-PC element
                    mc = SerialisedMapping.class;
                }
            }
            else if (embedded)
            {
                if (embeddedPC)
                {
                    // Embedded PC key
                    mc = EmbeddedKeyPCMapping.class;
                }
                else if (keyPC)
                {
                    // "Embedded" PC type but no <embedded> so dont embed for now. Is this correct?
                    mc = PersistableMapping.class;
                }
                else
                {
                    // Embedded Non-PC type
                    mcd = getMappingClass(keyCls, serialised, embedded, mmd.getKeyMetaData() != null ? mmd.getKeyMetaData().getColumnMetaData() : null, mmd.getFullFieldName());
                }
            }
            else
            {
                // Normal key mapping
                mcd = getMappingClass(keyCls, serialised, embedded, mmd.getKeyMetaData() != null ? mmd.getKeyMetaData().getColumnMetaData() : null, mmd.getFullFieldName());
            }
        }

        if (mcd != null && mcd.typeConverter == null)
        {
            mc = mcd.mappingClass;
        }
        if (mc != null && (mcd == null || mcd.typeConverter == null))
        {
            // Create the mapping of the selected type
            JavaTypeMapping m = null;
            try
            {
                m = (JavaTypeMapping)mc.newInstance();
                m.setRoleForMember(FieldRole.ROLE_MAP_KEY);
                m.initialize(mmd, table, clr);
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
            }
        }
        else if (mcd != null && mcd.typeConverter != null)
        {
            try
            {
                JavaTypeMapping m = (JavaTypeMapping)mcd.mappingClass.newInstance();
                m.setRoleForMember(FieldRole.ROLE_MAP_KEY);
                if (m instanceof TypeConverterMapping)
                {
                    ((TypeConverterMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                else if (m instanceof TypeConverterMultiMapping)
                {
                    ((TypeConverterMultiMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc != null ? mc.getName() : null, e), e).setFatal();
            }
        }

        throw new NucleusException("Unable to create mapping for key of map at " + mmd.getFullFieldName() + " - no available mapping");
    }

    /**
     * Convenience accessor for the mapping of the value for a map.
     * @param table The container
     * @param mmd MetaData for the field/property containing the map that this value is for
     * @param clr ClassLoader resolver
     * @return The mapping
     */
    protected JavaTypeMapping getValueMapping(Table table, AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        if (mmd.getMap() == null)
        {
            // TODO Localise this
            throw new NucleusException("Attempt to get value mapping for field " + mmd.getFullFieldName() + " that has no map!").setFatal();
        }

        MappingConverterDetails mcd = null;
        Class mc = null;
        String userTypeConverterName = null;
        String userMappingClassName = null;
        if (mmd.getValueMetaData() != null)
        {
            userTypeConverterName = mmd.getValueMetaData().getValueForExtension(MetaData.EXTENSION_MEMBER_TYPE_CONVERTER_NAME);
            userMappingClassName = mmd.getValueMetaData().getValueForExtension("mapping-class");
        }
        if (userTypeConverterName != null)
        {
            TypeConverter conv = storeMgr.getNucleusContext().getTypeManager().getTypeConverterForName(userTypeConverterName);
            if (conv == null)
            {
                throw new NucleusUserException("Field " + mmd.getFullFieldName() + " VALUE has been specified to use type converter " + userTypeConverterName + " but not found!");
            }
            mcd = new MappingConverterDetails(TypeConverterMapping.class, conv); // TODO Could be TypeConverterMultiMapping?
        }
        else if (userMappingClassName != null)
        {
            // User has defined their own mapping class for this value so use that
            try
            {
                mc = clr.classForName(userMappingClassName);
            }
            catch (NucleusException jpe)
            {
                throw new NucleusUserException(Localiser.msg("041014", userMappingClassName)).setFatal();
            }
        }
        else
        {
            boolean serialised = (mmd.hasMap() && mmd.getMap().isSerializedValue());
            boolean embedded = (mmd.hasMap() && mmd.getMap().isEmbeddedValue());
            boolean embeddedPC = (mmd.hasMap() && mmd.getMap().valueIsPersistent() && mmd.getMap().isEmbeddedValue()) || 
                    (mmd.getValueMetaData() != null && mmd.getValueMetaData().getEmbeddedMetaData() != null);
            boolean valuePC = (mmd.hasMap() && mmd.getMap().valueIsPersistent());
            Class valueCls = clr.classForName(mmd.getMap().getValueType());
            boolean valueReference = ClassUtils.isReferenceType(valueCls);

            if (serialised)
            {
                if (valuePC)
                {
                    // Serialised PC value
                    mc = SerialisedValuePCMapping.class;
                }
                else if (valueReference)
                {
                    // Serialised Reference value
                    mc = SerialisedReferenceMapping.class;
                }
                else
                {
                    // Serialised Non-PC element
                    mc = SerialisedMapping.class;
                }
            }
            else if (embedded)
            {
                if (embeddedPC)
                {
                    // Embedded PC key
                    mc = EmbeddedValuePCMapping.class;
                }
                else if (valuePC)
                {
                    // "Embedded" PC type but no <embedded> so dont embed for now. Is this correct?
                    mc = PersistableMapping.class;
                }
                else
                {
                    // Embedded Non-PC type
                    mcd = getMappingClass(valueCls, serialised, embedded, mmd.getValueMetaData() != null ? mmd.getValueMetaData().getColumnMetaData() : null, mmd.getFullFieldName());
                }
            }
            else
            {
                // Normal value mapping
                mcd = getMappingClass(valueCls, serialised, embedded, mmd.getValueMetaData() != null ? mmd.getValueMetaData().getColumnMetaData() : null, mmd.getFullFieldName());
            }
        }

        if (mcd != null && mcd.typeConverter == null)
        {
            mc = mcd.mappingClass;
        }
        if (mc != null && (mcd == null || mcd.typeConverter == null))
        {
            // Create the mapping of the selected type
            JavaTypeMapping m = null;
            try
            {
                m = (JavaTypeMapping)mc.newInstance();
                m.setRoleForMember(FieldRole.ROLE_MAP_VALUE);
                m.initialize(mmd, table, clr);
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc.getName(), e), e).setFatal();
            }
        }
        else if (mcd != null && mcd.typeConverter != null)
        {
            try
            {
                JavaTypeMapping m = (JavaTypeMapping)mcd.mappingClass.newInstance();
                m.setRoleForMember(FieldRole.ROLE_MAP_VALUE);
                if (m instanceof TypeConverterMapping)
                {
                    ((TypeConverterMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                else if (m instanceof TypeConverterMultiMapping)
                {
                    ((TypeConverterMultiMapping)m).initialize(mmd, table, clr, mcd.typeConverter);
                }
                return m;
            }
            catch (Exception e)
            {
                throw new NucleusException(Localiser.msg("041009", mc != null ? mc.getName() : null, e), e).setFatal();
            }
        }

        throw new NucleusException("Unable to create mapping for value of map at " + mmd.getFullFieldName() + " - no available mapping");
    }

    /**
     * Method to return the default java type mapping class for a specified java type.
     * @param javaType java type
     * @param colmds Metadata for the column(s) (optional)
     * @return The mapping class to use (by default)
     */
    protected MappingConverterDetails getDefaultJavaTypeMapping(Class javaType, ColumnMetaData[] colmds)
    {
        // Check for an explicit mapping
        Class cls = storeMgr.getMappingManager().getMappingType(javaType.getName());
        if (cls == null)
        {
            // No explicit mapping for this java type, so fall back to TypeConverter if available
            TypeManager typeMgr = storeMgr.getNucleusContext().getTypeManager();
            if (colmds != null && colmds.length > 0)
            {
                if (colmds.length > 1)
                {
                    // Find TypeConverter with right number of columns
                    Collection<TypeConverter> converters = typeMgr.getTypeConvertersForType(javaType);
                    if (converters != null && !converters.isEmpty())
                    {
                        for (TypeConverter conv : converters)
                        {
                            if (conv instanceof MultiColumnConverter)
                            {
                                if (((MultiColumnConverter)conv).getDatastoreColumnTypes().length == colmds.length)
                                {
                                    return new MappingConverterDetails(TypeConverterMultiMapping.class, conv);
                                }
                            }
                        }
                    }
                }
                else
                {
                    JdbcType jdbcType = colmds[0].getJdbcType();
                    if (jdbcType != null)
                    {
                        // JDBC type specified so don't just take the default
                        TypeConverter conv = null;
                        if (MetaDataUtils.isJdbcTypeString(jdbcType))
                        {
                            conv = typeMgr.getTypeConverterForType(javaType, String.class);
                        }
                        else if (MetaDataUtils.isJdbcTypeNumeric(jdbcType))
                        {
                            conv = typeMgr.getTypeConverterForType(javaType, Long.class);
                        }
                        else if (jdbcType == JdbcType.TIMESTAMP)
                        {
                            conv = typeMgr.getTypeConverterForType(javaType, Timestamp.class);
                        }
                        else if (jdbcType == JdbcType.TIME)
                        {
                            conv = typeMgr.getTypeConverterForType(javaType, Time.class);
                        }
                        else if (jdbcType == JdbcType.DATE)
                        {
                            conv = typeMgr.getTypeConverterForType(javaType, Date.class);
                        }

                        if (conv != null)
                        {
                            return new MappingConverterDetails(TypeConverterMapping.class, conv);
                        }
                    }
                }
            }

            TypeConverter conv = typeMgr.getDefaultTypeConverterForType(javaType);
            if (conv != null)
            {
                if (conv instanceof MultiColumnConverter)
                {
                    return new MappingConverterDetails(TypeConverterMultiMapping.class, conv);
                }
                return new MappingConverterDetails(TypeConverterMapping.class, conv);
            }

            NucleusLogger.PERSISTENCE.debug(Localiser.msg("041000", javaType.getName()));
            return null;
        }
        return new MappingConverterDetails(cls);
    }

    /**
     * Method to create the datastore mapping for a java type mapping at a particular index.
     * @param mapping The java mapping
     * @param mmd MetaData for the field/property
     * @param index Index of the column
     * @param column The column
     * @return The datastore mapping
     */
    public DatastoreMapping createDatastoreMapping(JavaTypeMapping mapping, AbstractMemberMetaData mmd, int index, Column column)
    {
        Class datastoreMappingClass = null;

        if (mmd.getColumnMetaData().length > 0)
        {
            // Use "datastore-mapping-class" extension if provided
            if (mmd.getColumnMetaData()[index].hasExtension("datastore-mapping-class"))
            {
                datastoreMappingClass = clr.classForName(mmd.getColumnMetaData()[index].getValueForExtension("datastore-mapping-class"));
            }
        }
        if (datastoreMappingClass == null)
        {
            String javaType = mapping.getJavaTypeForDatastoreMapping(index);
            String jdbcType = null;
            String sqlType = null;
            if (mapping.getRoleForMember() == FieldRole.ROLE_ARRAY_ELEMENT || mapping.getRoleForMember() == FieldRole.ROLE_COLLECTION_ELEMENT)
            {
                // Element of a collection/array
                ColumnMetaData[] colmds = (mmd.getElementMetaData() != null ? mmd.getElementMetaData().getColumnMetaData() : null);
                if (colmds != null && colmds.length > 0)
                {
                    jdbcType = colmds[index].getJdbcTypeName();
                    sqlType = colmds[index].getSqlType();
                }
                if (mmd.getCollection() != null && mmd.getCollection().isSerializedElement())
                {
                    javaType = ClassNameConstants.JAVA_IO_SERIALIZABLE;
                }
                if (mmd.getArray() != null && mmd.getArray().isSerializedElement())
                {
                    javaType = ClassNameConstants.JAVA_IO_SERIALIZABLE;
                }
            }
            else if (mapping.getRoleForMember() == FieldRole.ROLE_MAP_KEY)
            {
                // Key of a map
                ColumnMetaData[] colmds = (mmd.getKeyMetaData() != null ? mmd.getKeyMetaData().getColumnMetaData() : null);
                if (colmds != null && colmds.length > 0)
                {
                    jdbcType = colmds[index].getJdbcTypeName();
                    sqlType = colmds[index].getSqlType();
                }
                if (mmd.getMap().isSerializedKey())
                {
                    javaType = ClassNameConstants.JAVA_IO_SERIALIZABLE;
                }
            }
            else if (mapping.getRoleForMember() == FieldRole.ROLE_MAP_VALUE)
            {
                // Value of a map
                ColumnMetaData[] colmds = (mmd.getValueMetaData() != null ? mmd.getValueMetaData().getColumnMetaData() : null);
                if (colmds != null && colmds.length > 0)
                {
                    jdbcType = colmds[index].getJdbcTypeName();
                    sqlType = colmds[index].getSqlType();
                }
                if (mmd.getMap().isSerializedValue())
                {
                    javaType = ClassNameConstants.JAVA_IO_SERIALIZABLE;
                }
            }
            else
            {
                // Normal field
                if (mmd.getColumnMetaData().length > 0)
                {
                    // Utilise the jdbc and sql types if specified
                    jdbcType = mmd.getColumnMetaData()[index].getJdbcTypeName();
                    sqlType = mmd.getColumnMetaData()[index].getSqlType();
                }

                // Special case where we have IDENTITY strategy and the datastore imposes a limitation on the required datastore type
                ValueGenerationStrategy strategy = mmd.getValueStrategy();
                if (strategy != null)
                {
                    String strategyName = strategy.toString();
                    if (strategy == ValueGenerationStrategy.NATIVE)
                    {
                        strategyName = storeMgr.getValueGenerationStrategyForNative(mmd.getAbstractClassMetaData(), mmd.getAbsoluteFieldNumber());
                    }
                    if (strategyName != null && ValueGenerationStrategy.IDENTITY.toString().equals(strategyName))
                    {
                        Class requestedType = clr.classForName(javaType);
                        Class requiredType = storeMgr.getDatastoreAdapter().getAutoIncrementJavaTypeForType(requestedType);
                        if (requiredType != mmd.getType())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug("Member " + mmd.getFullFieldName() + " uses IDENTITY strategy and rather than using memberType of " + mmd.getTypeName() +
                                    " for the column type, using " + requiredType + " since the datastore requires that");
                        }
                        javaType = requiredType.getName();
                    }
                }

                if (mmd.isSerialized())
                {
                    javaType = ClassNameConstants.JAVA_IO_SERIALIZABLE;
                }
            }

            datastoreMappingClass = storeMgr.getDatastoreAdapter().getDatastoreMappingClass(javaType, jdbcType, sqlType, clr, mmd.getFullFieldName());
        }

        DatastoreMapping datastoreMapping = DatastoreMappingFactory.createMapping(datastoreMappingClass, mapping, storeMgr, column);
        if (column != null)
        {
            column.setDatastoreMapping(datastoreMapping);
        }
        return datastoreMapping;
    }

    /**
     * Method to create the datastore mapping for a particular column and java type.
     * If the column is specified it is linked to the created datastore mapping.
     * @param mapping The java mapping
     * @param column The column (can be null)
     * @param javaType The java type
     * @return The datastore mapping
     */
    public DatastoreMapping createDatastoreMapping(JavaTypeMapping mapping, Column column, String javaType)
    {
        Column col = column;
        String jdbcType = null;
        String sqlType = null;
        if (col != null && col.getColumnMetaData() != null)
        {
            // Utilise the jdbc and sql types if specified
            jdbcType = col.getColumnMetaData().getJdbcTypeName();
            sqlType = col.getColumnMetaData().getSqlType();
        }

        Class datastoreMappingClass = storeMgr.getDatastoreAdapter().getDatastoreMappingClass(javaType, jdbcType, sqlType, clr, null);

        DatastoreMapping datastoreMapping = DatastoreMappingFactory.createMapping(datastoreMappingClass, mapping, storeMgr, column);
        if (column != null)
        {
            column.setDatastoreMapping(datastoreMapping);
        }
        return datastoreMapping;
    }

    /**
     * Method to create a column for a Java type mapping.
     * This is NOT used for persistable mappings - see method below.
     * @param mapping Java type mapping for the field
     * @param javaType The type of field being stored in this column
     * @param datastoreFieldIndex Index of the datastore field to use
     * @return The datastore field
     */
    public Column createColumn(JavaTypeMapping mapping, String javaType, int datastoreFieldIndex)
    {
        AbstractMemberMetaData mmd = mapping.getMemberMetaData();
        FieldRole roleForField = mapping.getRoleForMember();
        Table tbl = mapping.getTable();

        // Take the column MetaData from the component that this mappings role relates to
        ColumnMetaData colmd = null;
        ColumnMetaDataContainer columnContainer = mmd;
        if (roleForField == FieldRole.ROLE_COLLECTION_ELEMENT || roleForField == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            columnContainer = mmd.getElementMetaData();
        }
        else if (roleForField == FieldRole.ROLE_MAP_KEY)
        {
            columnContainer = mmd.getKeyMetaData();
        }
        else if (roleForField == FieldRole.ROLE_MAP_VALUE)
        {
            columnContainer= mmd.getValueMetaData();
        }

        Column col;
        ColumnMetaData[] colmds;
        if (columnContainer != null && columnContainer.getColumnMetaData().length > datastoreFieldIndex)
        {
            colmd = columnContainer.getColumnMetaData()[datastoreFieldIndex];
            colmds = columnContainer.getColumnMetaData();
        }
        else
        {
            // If column specified add one (use any column name specified on field element)
            colmd = new ColumnMetaData();
            if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > datastoreFieldIndex)
            {
                colmd.setName(mmd.getColumnMetaData()[datastoreFieldIndex].getName());
            }
            if (columnContainer != null)
            {
                columnContainer.addColumn(colmd);
                colmds = columnContainer.getColumnMetaData();
            }
            else
            {
                colmds = new ColumnMetaData[1];
                colmds[0] = colmd;
            }
        }

        // Generate the column identifier
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        DatastoreIdentifier identifier = null;
        if (colmd.getName() == null)
        {
            // No name specified, so generate the identifier from the field name
            if (roleForField == FieldRole.ROLE_COLLECTION_ELEMENT)
            {
                // Join table collection element
                identifier = idFactory.newJoinTableFieldIdentifier(mmd, null, null, true, FieldRole.ROLE_COLLECTION_ELEMENT);
            }
            else if (roleForField == FieldRole.ROLE_ARRAY_ELEMENT)
            {
                // Join table array element
                identifier = idFactory.newJoinTableFieldIdentifier(mmd, null, null, true, FieldRole.ROLE_ARRAY_ELEMENT);
            }
            else if (roleForField == FieldRole.ROLE_MAP_KEY)
            {
                // Join table map key
                identifier = idFactory.newJoinTableFieldIdentifier(mmd, null, null, true, FieldRole.ROLE_MAP_KEY);
            }
            else if (roleForField == FieldRole.ROLE_MAP_VALUE)
            {
                // Join table map value
                identifier = idFactory.newJoinTableFieldIdentifier(mmd, null, null, true, FieldRole.ROLE_MAP_VALUE);
            }
            else
            {
                identifier = idFactory.newIdentifier(IdentifierType.COLUMN, mmd.getName());
                int i=0;
                while (tbl.hasColumn(identifier))
                {
                    identifier = idFactory.newIdentifier(IdentifierType.COLUMN, mmd.getName() + "_" + i);
                    i++;
                }
            }

            colmd.setName(identifier.getName());
        }
        else
        {
            // User has specified a name, so try to keep this unmodified
            identifier = idFactory.newColumnIdentifier(colmds[datastoreFieldIndex].getName(), 
                storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(mmd.getType()), null, true);
        }

        // Create the column
        col = tbl.addColumn(javaType, identifier, mapping, colmd);
        if (mmd.isPrimaryKey())
        {
            col.setPrimaryKey();
        }

        if (!(mmd.getParent() instanceof AbstractClassMetaData))
        {
            // Embedded so can't be datastore-attributed
        }
        else
        {
            // Commented out this code because if a member is overridden it should use the override strategy here. Can't find a case which needs this
            /*if (!mmd.getClassName(true).equals(mmd.getAbstractClassMetaData().getFullClassName()))
            {
                if (storeMgr.isStrategyDatastoreAttributed(mmd.getAbstractClassMetaData(), mmd.getAbsoluteFieldNumber()) && tbl instanceof DatastoreClass)
                {
                    if ((mmd.isPrimaryKey() && ((DatastoreClass)tbl).isBaseDatastoreClass()) || !mmd.isPrimaryKey())
                    {
                        NucleusLogger.GENERAL.info(">> Column addition " + mmd.getFullFieldName() + " IGNORING use of IDENTITY since override of base metadata! See RDBMSMappingManager");
                    }
                }
                // Overriding member, so ignore TODO This can be incorrect in many cases
            }
            else
            {*/
                if (storeMgr.isValueGenerationStrategyDatastoreAttributed(mmd.getAbstractClassMetaData(), mmd.getAbsoluteFieldNumber()) && tbl instanceof DatastoreClass)
                {
                    if ((mmd.isPrimaryKey() && ((DatastoreClass)tbl).isBaseDatastoreClass()) || !mmd.isPrimaryKey())
                    {
                        // Increment any PK field if we are in base class, and increment any other field
                        col.setIdentity(true);
                    }
                }
            /*}*/
        }

        if (mmd.getValueForExtension("select-function") != null)
        {
            col.setWrapperFunction(mmd.getValueForExtension("select-function"),Column.WRAPPER_FUNCTION_SELECT);
        }
        if (mmd.getValueForExtension("insert-function") != null)
        {
            col.setWrapperFunction(mmd.getValueForExtension("insert-function"),Column.WRAPPER_FUNCTION_INSERT);
        }
        if (mmd.getValueForExtension("update-function") != null)
        {
            col.setWrapperFunction(mmd.getValueForExtension("update-function"),Column.WRAPPER_FUNCTION_UPDATE);
        }

        setColumnNullability(mmd, colmd, col);
        if (mmd.getNullValue() == NullValue.DEFAULT)
        {
            // Users default should be applied if a null is to be inserted
            col.setDefaultable(colmd.getDefaultValue());
        }

        return col;
    }

    /**
     * Method to create a datastore field for a Java type mapping.
     * This is used for serialised PC elements/keys/values in a join table.
     * TODO Merge this with the method above.
     * @param mapping Java type mapping for the field
     * @param javaType The type of field being stored in this column
     * @param colmd MetaData for the column
     * @return The column
     */
    public Column createColumn(JavaTypeMapping mapping, String javaType, ColumnMetaData colmd)
    {
        AbstractMemberMetaData mmd = mapping.getMemberMetaData();
        Table tbl = mapping.getTable();

        if (colmd == null)
        {
            // If column specified add one (use any column name specified on field element)
            colmd = new ColumnMetaData();
            if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length == 1)
            {
                colmd.setName(mmd.getColumnMetaData()[0].getName());
            }
            mmd.addColumn(colmd);
        }

        Column col;
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        if (colmd.getName() == null)
        {
            // No name specified, so generate the identifier from the field name
            DatastoreIdentifier identifier = idFactory.newIdentifier(IdentifierType.COLUMN, mmd.getName());
            int i=0;
            while (tbl.hasColumn(identifier))
            {
                identifier = idFactory.newIdentifier(IdentifierType.COLUMN, mmd.getName() + "_" + i);
                i++;
            }

            colmd.setName(identifier.getName());
            col = tbl.addColumn(javaType, identifier, mapping, colmd);
        }
        else
        {
            // User has specified a name, so try to keep this unmodified
            col = tbl.addColumn(javaType, idFactory.newColumnIdentifier(colmd.getName(), 
                    storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(mmd.getType()), null, true), mapping, colmd);
        }

        setColumnNullability(mmd, colmd, col);
        if (mmd.getNullValue() == NullValue.DEFAULT)
        {
            // Users default should be applied if a null is to be inserted
            col.setDefaultable(colmd.getDefaultValue());
        }

        return col;
    }

    /**
     * Method to create a datastore field for a persistable mapping.
     * @param mmd MetaData for the field whose mapping it is
     * @param table Datastore class where we create the datastore field
     * @param mapping The Java type for this field
     * @param colmd The columnMetaData for this datastore field
     * @param reference The datastore field we are referencing
     * @param clr ClassLoader resolver
     * @return The column
     */
    public Column createColumn(AbstractMemberMetaData mmd, Table table, JavaTypeMapping mapping, ColumnMetaData colmd, Column reference, ClassLoaderResolver clr)
    {
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        DatastoreIdentifier identifier = null;
        if (colmd.getName() == null)
        {
            // No name specified, so generate the identifier from the field name
            AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
            identifier = idFactory.newForeignKeyFieldIdentifier(relatedMmds != null ? relatedMmds[0] : null, mmd, reference.getIdentifier(), 
                storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(mmd.getType()), FieldRole.ROLE_OWNER);
            colmd.setName(identifier.getName());
        }
        else
        {
            // User has specified a name, so try to keep this unmodified
            identifier = idFactory.newColumnIdentifier(colmd.getName(), false, null, true);
        }
        Column col = table.addColumn(mmd.getType().getName(), identifier, mapping, colmd);

        // Copy the characteristics of the reference column to this one
        reference.copyConfigurationTo(col);

        if (mmd.isPrimaryKey())
        {
            col.setPrimaryKey();
        }

        if (!(mmd.getParent() instanceof AbstractClassMetaData))
        {
            // Embedded so can't be datastore-attributed
        }
        else
        {
            if (storeMgr.isValueGenerationStrategyDatastoreAttributed(mmd.getAbstractClassMetaData(), mmd.getAbsoluteFieldNumber()))
            {
                if ((mmd.isPrimaryKey() && ((DatastoreClass)table).isBaseDatastoreClass()) || !mmd.isPrimaryKey())
                {
                    // Increment any PK field if we are in base class, and increment any other field
                    col.setIdentity(true);
                }
            }
        }

        if (mmd.getValueForExtension("select-function") != null)
        {
            col.setWrapperFunction(mmd.getValueForExtension("select-function"),Column.WRAPPER_FUNCTION_SELECT);
        }
        if (mmd.getValueForExtension("insert-function") != null)
        {
            col.setWrapperFunction(mmd.getValueForExtension("insert-function"),Column.WRAPPER_FUNCTION_INSERT);
        }
        if (mmd.getValueForExtension("update-function") != null)
        {
            col.setWrapperFunction(mmd.getValueForExtension("update-function"),Column.WRAPPER_FUNCTION_UPDATE);
        }

        setColumnNullability(mmd, colmd, col);
        if (mmd.getNullValue() == NullValue.DEFAULT)
        {
            // Users default should be applied if a null is to be inserted
            col.setDefaultable(colmd.getDefaultValue());
        }

        return col;        
    }

    /**
     * Sets the column nullability based on metadata configuration.
     * <P>
     * Configuration is taken in this order:
     * <UL>
     * <LI>ColumnMetaData (allows-null)</LI>
     * <LI>AbstractMemberMetaData (null-value)</LI>
     * <LI>Field type (primitive does not allows null)</LI>
     * </UL>
     * @param mmd Metadata for the field/property
     * @param colmd the ColumnMetaData
     * @param col the column
     */
    private void setColumnNullability(AbstractMemberMetaData mmd, ColumnMetaData colmd, Column col)
    {
        // Provide defaults for "allows-null" in ColumnMetaData
        if (colmd != null && colmd.getAllowsNull() == null)
        {
            if (mmd.isPrimaryKey())
            {
                colmd.setAllowsNull(Boolean.valueOf(false));
            }
            else if (!mmd.getType().isPrimitive() && mmd.getNullValue() != NullValue.EXCEPTION)
            {
                colmd.setAllowsNull(Boolean.valueOf(true));
            }
            else
            {
                colmd.setAllowsNull(Boolean.valueOf(false));
            }
            if (colmd.isAllowsNull())
            {
                col.setNullable(true);
            }
        }
        // Set the nullability of the column in the datastore
        else if (colmd != null && colmd.getAllowsNull() != null)
        {
            if (colmd.isAllowsNull())
            {
                col.setNullable(true);
            }
        }
        else if (mmd.isPrimaryKey())
        {
            // field is never nullable
        }
        else if (!mmd.getType().isPrimitive() && mmd.getNullValue() != NullValue.EXCEPTION)
        {
            col.setNullable(true);
        }
    }
}