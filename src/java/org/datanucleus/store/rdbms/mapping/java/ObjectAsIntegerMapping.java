/**********************************************************************
Copyright (c) 2006 Tony Lai and others. All rights reserved.
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
 * Abstract mapping for a java type that will be stored as a Integer type.
 * @deprecated Drop this, people should use TypeConverter. This will be removed before DN 4.0
 */
public abstract class ObjectAsIntegerMapping extends SingleFieldMapping
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
        // All of the types extending this class will be using java-type of Integer for the datastore
        return ClassNameConstants.JAVA_LANG_INTEGER;
    }

    /**
     * Method to set the object when updating the the datastore.
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#setObject(org.datanucleus.ExecutionContext, PreparedStatement, int[], java.lang.Object)
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        getDatastoreMapping(0).setObject(ps, exprIndex[0], objectToNumber(value));
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

        //TODO: It would be a lot more efficient if the DatastoreMapping class provides a
        // public Integer getIntegerObject(Object resultSet, int exprIndex) method.
        Object datastoreValue = getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
        Object value = null;
        if (datastoreValue != null)
        {
            value = numberToObject((Number)datastoreValue);
        }
        return value;
    }

    /**
     * Method to set the datastore value based on the object value.
     * @param object The object
     * @return The value to pass to the datastore
     */
    protected abstract Number objectToNumber(Object object);

    /**
     * Method to extract the objects value from the datastore object.
     * @param datastoreValue Value obtained from the datastore
     * @return The value of this object (derived from the datastore value)
     */
    protected abstract Object numberToObject(Number datastoreValue);
}