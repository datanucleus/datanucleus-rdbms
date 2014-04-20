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
2003 Andy Jefferson - coding standards
2006 Michael Brown - updated to cater for alternative types in the PK fields
	...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping;

import java.sql.PreparedStatement;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.util.Localiser;

/**
 * Parameter setter class to deal with application identity.
 * Parameters must be in the same order as the fields.
 */
public class AppIDObjectIdFieldManager extends AbstractFieldManager
{
    /** Localisation of messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /** Parameter positions in the PreparedStatement. */
    private int[] params;

    /** Next parameter index. Increased each call. */
    private int nextParam;

    /** ExecutionContext. */
    private ExecutionContext ec;

    /** The PreparedStatement to populate. */
    private PreparedStatement statement;

    /** The mappings for the object id */
    private JavaTypeMapping[] javaTypeMappings;

    /** Number of the mapping being processed. */
    private int mappingNum = 0;

    /**
     * Constructor.
     * @param param Parameter positions
     * @param ec ExecutionContext
     * @param statement PreparedStatement
     * @param javaTypeMappings Java mappings for the PC object
     */
    public AppIDObjectIdFieldManager(int[] param, ExecutionContext ec, PreparedStatement statement, JavaTypeMapping[] javaTypeMappings)
    {
        this.params = param;
        nextParam = 0;
        this.ec = ec;
        this.statement = statement;

        // Save all mappings in the same order, allowing for PersistableMapping having sub mappings
        int numMappings = 0;
        for (int i=0;i<javaTypeMappings.length;i++)
        {
            if (javaTypeMappings[i] instanceof PersistableMapping)
            {
                numMappings += ((PersistableMapping)javaTypeMappings[i]).getJavaTypeMapping().length;
            }
            else
            {
                numMappings++;
            }
        }
        this.javaTypeMappings = new JavaTypeMapping[numMappings];
        int mappingNum = 0;
        for (int i=0;i<javaTypeMappings.length;i++)
        {
            if (javaTypeMappings[i] instanceof PersistableMapping)
            {
                PersistableMapping m = (PersistableMapping)javaTypeMappings[i];
                JavaTypeMapping[] subMappings = m.getJavaTypeMapping();
                for (int j=0;j<subMappings.length;j++)
                {
                    this.javaTypeMappings[mappingNum++] = subMappings[j];
                }
            }
            else
            {
                this.javaTypeMappings[mappingNum++] = javaTypeMappings[i];
            }
        }
    }

    /**
     * Convenience method to return the statement param position(s) for a field.
     * @param mapping The mapping
     * @return The param positions
     */
    private int[] getParamsForField(JavaTypeMapping mapping)
    {
        if (javaTypeMappings.length == 1)
        {
            return params;
        }
        else
        {
            int numCols = mapping.getNumberOfDatastoreMappings();
            int[] fieldParams = new int[numCols];
            for (int i=0;i<numCols;i++)
            {
                fieldParams[i] = params[nextParam++];
            }
            return fieldParams;
        }
    }

    /**
     * Method to store a boolean in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setBoolean(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store a byte in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeByteField(int fieldNumber, byte value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setByte(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store a character in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeCharField(int fieldNumber, char value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setChar(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store a double in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeDoubleField(int fieldNumber, double value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setDouble(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store a float in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeFloatField(int fieldNumber, float value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setFloat(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store an integer in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeIntField(int fieldNumber, int value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setInt(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store a long in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeLongField(int fieldNumber, long value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setLong(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store a short in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeShortField(int fieldNumber, short value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setShort(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store a String in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeStringField(int fieldNumber, String value)
    {
        JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
        mapping.setString(ec, statement, getParamsForField(mapping), value);
    }

    /**
     * Method to store an object in a field.
     * @param fieldNumber Number of the field
     * @param value The value to use
     */
    public void storeObjectField(int fieldNumber, Object value)
    {
        ApiAdapter api = ec.getApiAdapter();
        if (api.isPersistable(value))
        {
            api.copyPkFieldsToPersistableObjectFromId(value, api.getObjectState(value), this);
        }
        else
        {
            JavaTypeMapping mapping = javaTypeMappings[mappingNum++];
            mapping.setObject(ec, statement, getParamsForField(mapping), value);
        }
    }
}