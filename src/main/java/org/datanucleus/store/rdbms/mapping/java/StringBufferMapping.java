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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.types.converters.StringBufferStringConverter;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Mapping for a StringBuffer type.
 * 
 * Note: A java.lang.StringBuffer is a final class and does not allow a
 * SCO implementation in order of implementing dirty detecting 
 */
public class StringBufferMapping extends StringMapping
{
    protected static final TypeConverter<StringBuffer, String> converter = new StringBufferStringConverter();

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        // All of the types extending this class will be using java-type of String for the datastore
        return ClassNameConstants.JAVA_LANG_STRING;
    }

    /**
     * Delegates to StringMapping the storage with giving a String
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        Object v = converter.toDatastoreType((StringBuffer)value);
        super.setObject(ec, ps, exprIndex, v);
    }

    /**
     * Delegates to StringMapping the retrieval of a String and constructs
     * a StringBuffer out of it
     */
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }
        Object value = getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
        if (value != null)
        {
            return converter.toMemberType((String)value);
        }
        return null;
    }
    
    public Class getJavaType()
    {
        return StringBuffer.class;
    }
}