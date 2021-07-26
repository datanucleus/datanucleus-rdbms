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
package org.datanucleus.store.rdbms;

import org.datanucleus.properties.PropertyValidator;
import org.datanucleus.store.rdbms.query.RDBMSQueryUtils;

/**
 * Validator for persistence properties used by RDBMS.
 */
public class RDBMSPropertyValidator implements PropertyValidator
{
    /**
     * Validate the specified property.
     * @param name Name of the property
     * @param value Value
     * @return Whether it is valid
     */
    public boolean validate(String name, Object value)
    {
        if (name == null)
        {
            return false;
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_FETCH_DIRECTION))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase("forward") ||
                    strVal.equalsIgnoreCase("reverse") ||
                    strVal.equalsIgnoreCase("unknown"))
                {
                    return true;
                }
            }
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_TYPE))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase(RDBMSQueryUtils.QUERY_RESULTSET_TYPE_FORWARD_ONLY) ||
                    strVal.equalsIgnoreCase(RDBMSQueryUtils.QUERY_RESULTSET_TYPE_SCROLL_SENSITIVE) ||
                    strVal.equalsIgnoreCase(RDBMSQueryUtils.QUERY_RESULTSET_TYPE_SCROLL_INSENSITIVE))
                {
                    return true;
                }
            }
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_RESULT_SET_CONCURRENCY))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase(RDBMSQueryUtils.QUERY_RESULTSET_CONCURRENCY_READONLY) ||
                    strVal.equalsIgnoreCase(RDBMSQueryUtils.QUERY_RESULTSET_CONCURRENCY_UPDATEABLE))
                {
                    return true;
                }
            }
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_QUERY_MULTIVALUED_FETCH))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase("exists") ||
                    strVal.equalsIgnoreCase("none"))
                {
                    return true;
                }
            }
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase("DataNucleus") ||
                    strVal.equalsIgnoreCase("JDO2"))
                {
                    return true;
                }
            }
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_STRING_LENGTH_EXCEEDED_ACTION))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase("EXCEPTION") ||
                    strVal.equalsIgnoreCase("TRUNCATE"))
                {
                    return true;
                }
            }
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_INIT_COLUMN_INFO))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase("ALL") ||
                    strVal.equalsIgnoreCase("PK") ||
                    strVal.equalsIgnoreCase("NONE"))
                {
                    return true;
                }
            }
        }
        else if (name.equalsIgnoreCase(RDBMSPropertyNames.PROPERTY_RDBMS_STATEMENT_LOGGING))
        {
            if (value instanceof String)
            {
                String strVal = (String)value;
                if (strVal.equalsIgnoreCase("JDBC") ||
                    strVal.equalsIgnoreCase("PARAMS_INLINE") ||
                    strVal.equalsIgnoreCase("PARAMS_IN_BRACKETS"))
                {
                    return true;
                }
            }
        }
        return false;
    }
}
