/**********************************************************************
Copyright (c) 2012 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.fieldmanager;

import java.sql.PreparedStatement;

import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.query.StatementClassMapping;

/**
 * Parameter setter that uses old values when available.
 * Used as part of the nondurable update process.
 * Assumes that the old value for fields are stored by StateManager under name "FIELD_VALUE.ORIGINAL.{fieldNum}".
 */
public class OldValueParameterSetter extends ParameterSetter
{
    /**
     * Constructor.
     * @param op The ObjectProvider for the object.
     * @param stmt The Statement to set values on.
     * @param stmtMappings mappings for parameters in the statement.
     */
    public OldValueParameterSetter(ObjectProvider op, PreparedStatement stmt, StatementClassMapping stmtMappings)
    {
        super(op, stmt, stmtMappings);
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeBooleanField(fieldNumber, (Boolean) oldValue);
        }
        else
        {
            super.storeBooleanField(fieldNumber, value);
        }
    }

    public void storeCharField(int fieldNumber, char value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeCharField(fieldNumber, (Character) oldValue);
        }
        else
        {
            super.storeCharField(fieldNumber, value);
        }
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeByteField(fieldNumber, (Byte) oldValue);
        }
        else
        {
            super.storeByteField(fieldNumber, value);
        }
    }

    public void storeShortField(int fieldNumber, short value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeShortField(fieldNumber, (Short) oldValue);
        }
        else
        {
            super.storeShortField(fieldNumber, value);
        }
    }

    public void storeIntField(int fieldNumber, int value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeIntField(fieldNumber, (Integer) oldValue);
        }
        else
        {
            super.storeIntField(fieldNumber, value);
        }
    }

    public void storeLongField(int fieldNumber, long value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeLongField(fieldNumber, (Long) oldValue);
        }
        else
        {
            super.storeLongField(fieldNumber, value);
        }
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeFloatField(fieldNumber, (Float) oldValue);
        }
        else
        {
            super.storeFloatField(fieldNumber, value);
        }
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeDoubleField(fieldNumber, (Double) oldValue);
        }
        else
        {
            super.storeDoubleField(fieldNumber, value);
        }
    }

    public void storeStringField(int fieldNumber, String value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeStringField(fieldNumber, (String) oldValue);
        }
        else
        {
            super.storeStringField(fieldNumber, value);
        }
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumber);
        if (oldValue != null)
        {
            super.storeObjectField(fieldNumber, oldValue);
        }
        else
        {
            super.storeObjectField(fieldNumber, value);
        }
    }
}