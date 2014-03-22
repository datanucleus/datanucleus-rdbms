/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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

/**
 * Abstract SCO mapping for a java type that will be stored as a String type.
 * @deprecated Drop this, people should use TypeConverter. This will be removed before DN 4.0
 */
public abstract class ObjectAsStringMapping extends SingleFieldMapping
{
    /**
     * Method to return the Java type.
     * @return The Java type being represented.
     */
    public abstract Class getJavaType();

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
     * Method to set the object when updating the the datastore.
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#setObject(org.datanucleus.ExecutionContext, PreparedStatement, int[], java.lang.Object)
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        getDatastoreMapping(0).setObject(ps, exprIndex[0], objectToString(value));
    }

    /**
     * Method to get the object from the datastore and convert to an object.
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#getObject(org.datanucleus.ExecutionContext, ResultSet, int[])
     */
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        Object datastoreValue = getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
        Object value = null;
        if (datastoreValue != null)
        {
            value = stringToObject((String)datastoreValue);
        }
        return value;
    }

    /**
     * Method to set the datastore value based on the object value.
     * @param object The object
     * @return The value to pass to the datastore
     */
    protected abstract String objectToString(Object object);

    /**
     * Method to extract the objects value from the datastore object.
     * @param datastoreValue Value obtained from the datastore
     * @return The value of this object (derived from the datastore value)
     */
    protected abstract Object stringToObject(String datastoreValue);
}