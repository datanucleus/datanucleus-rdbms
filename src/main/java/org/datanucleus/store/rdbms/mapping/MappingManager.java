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
2004 Erik Bengtson - added datastore mapping accessors
    ...
*****************************************************************/
package org.datanucleus.store.rdbms.mapping;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;

/**
 * Representation of a MappingManager, mapping a java mapping type to a column mapping type.
 * Allows a java mapping type to map to multiple column mapping types.
 * Allows a default column mapping type be assigned to each java mapping type.
 */
public interface MappingManager
{
    public static final String METADATA_EXTENSION_INSERT_FUNCTION = "insert-function";
    public static final String METADATA_EXTENSION_UPDATE_FUNCTION = "update-function";
    public static final String METADATA_EXTENSION_SELECT_FUNCTION = "select-function";

    /**
     * Accessor for whether a java type is supported as being mappable.
     * @param javaTypeName The java type name
     * @return Whether the class is supported (to some degree)
     */
    boolean isSupportedMappedType(String javaTypeName);

    /**
     * Accessor for the JavaTypeMapping class for the supplied java type.
     * @param javaTypeName The java type name
     * @return The JavaTypeMapping class to use
     */
    Class getMappingType(String javaTypeName);

    /**
     * Method to create the column mapping for a java type mapping at a particular index.
     * @param mapping The java mapping
     * @param fmd MetaData for the field
     * @param index Index of the column
     * @param column The column
     * @return The column mapping
     */
    ColumnMapping createColumnMapping(JavaTypeMapping mapping, AbstractMemberMetaData fmd, int index, Column column);

    /**
     * Method to create the column mapping for a particular column and java type.
     * @param mapping The java mapping
     * @param column The column
     * @param javaType The java type (isn't this stored in the java mapping ?)
     * @return The column mapping
     */
    ColumnMapping createColumnMapping(JavaTypeMapping mapping, Column column, String javaType);

    /**
     * Accessor for a mapping, for a java type.
     * Same as calling "getMapping(c, false, false, (String)null);"
     * @param c The java type
     * @return The mapping
     */
    JavaTypeMapping getMapping(Class c);

    /**
     * Accessor for a mapping, for a java type.
     * @param c The java type
     * @param serialised Whether the type is serialised
     * @param embedded Whether the type is embedded
     * @param fieldName Name of the field (for logging only)
     * @return The mapping
     */
    JavaTypeMapping getMapping(Class c, boolean serialised, boolean embedded, String fieldName);

    /**
     * Accessor for a mapping, for a java type complete with the column mapping.
     * @param c The java type
     * @param serialised Whether the type is serialised
     * @param embedded Whether the type is embedded
     * @param clr ClassLoader resolver
     * @return The mapping
     */
    JavaTypeMapping getMappingWithColumnMapping(Class c, boolean serialised, boolean embedded, ClassLoaderResolver clr);

    /**
     * Accessor for the mapping for the field of the specified table.
     * Can be used for fields of a class, elements of a collection of a class, elements of an array of a class, keys of a map of a class, values of a map of a class. 
     * This is controlled by the final argument "roleForMember".
     * @param table Table to add the mapping to
     * @param mmd MetaData for the field/property to map
     * @param clr The ClassLoaderResolver
     * @param fieldRole Role that this mapping plays for the field/property
     * @return The mapping for the field.
     */
    JavaTypeMapping getMapping(Table table, AbstractMemberMetaData mmd, ClassLoaderResolver clr, FieldRole fieldRole);

    /**
     * Method to create a column in a container (table).
     * @param mapping The java mapping
     * @param javaType The java type
     * @param datastoreFieldIndex The index of the column to create
     * @return The column
     */
    Column createColumn(JavaTypeMapping mapping, String javaType, int datastoreFieldIndex);

    /**
     * Method to create a column in a container (table).
     * To be used for serialised PC element/key/value in a join table.
     * @param mapping The java mapping
     * @param javaType The java type
     * @param colmd MetaData for the column to create
     * @return The column
     */
    Column createColumn(JavaTypeMapping mapping, String javaType, ColumnMetaData colmd);

    /**
     * Method to create a column for a persistable mapping.
     * @param fmd MetaData for the field
     * @param table Table in the datastore
     * @param mapping The java mapping
     * @param colmd MetaData for the column to create
     * @param referenceCol The column to reference
     * @param clr ClassLoader resolver
     * @return The column
     */
    Column createColumn(AbstractMemberMetaData fmd, Table table, JavaTypeMapping mapping, ColumnMetaData colmd, Column referenceCol, ClassLoaderResolver clr);
}