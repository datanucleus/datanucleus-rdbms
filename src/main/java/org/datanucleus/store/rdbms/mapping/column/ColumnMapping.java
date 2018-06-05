/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Representation of the mapping of a column.
 */
public interface ColumnMapping
{
    /**
     * Whether the column is nullable.
     * @return true if is nullable
     */
    boolean isNullable();

    /**
     * Accessor for the column that this represents.
     * @return The Column
     */
    Column getColumn();

    /**
     * The mapping for the java type that this column mapping is used by.
     * This will return null if this simply maps a datastore field in the datastore and has no associated java type in a class.
     * @return the JavaTypeMapping
     */
    JavaTypeMapping getJavaTypeMapping();

    /**
     * Accessor for whether this mapping requires values inserting on an INSERT.
     * @return Whether values are to be inserted into this mapping on an INSERT
     */
    boolean insertValuesOnInsert();

    /**
     * Accessor for the string to put in any retrieval datastore statement for this field. This is typically a ? to be used in JDBC statements.
     * @return The input parameter
     */
    String getInsertionInputParameter();

    /**
     * Accessor for the string to put in any update datastore statements for this field. This is typically a ? to be used in JDBC statements.
     * @return The input parameter.
     */
    String getUpdateInputParameter();

    /**
     * Accessor for whether the mapping is decimal-based.
     * @return Whether the mapping is decimal based
     */
    boolean isDecimalBased();

    /**
     * Accessor for whether the mapping is integer-based.
     * @return Whether the mapping is integer based
     */
    boolean isIntegerBased();

    /**
     * Accessor for whether the mapping is string-based.
     * @return Whether the mapping is string based
     */
    boolean isStringBased();

    /**
     * Accessor for whether the mapping is bit-based.
     * @return Whether the mapping is bit based
     */
    boolean isBitBased();

    /**
     * Accessor for whether the mapping is boolean-based.
     * @return Whether the mapping is boolean based
     */
    boolean isBooleanBased();

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setBoolean(PreparedStatement ps, int paramIndex, boolean value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setChar(PreparedStatement ps, int paramIndex, char value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setByte(PreparedStatement ps, int paramIndex, byte value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setShort(PreparedStatement ps, int paramIndex, short value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setInt(PreparedStatement ps, int paramIndex, int value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setLong(PreparedStatement ps, int paramIndex, long value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setFloat(PreparedStatement ps, int paramIndex, float value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setDouble(PreparedStatement ps, int paramIndex, double value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setString(PreparedStatement ps, int paramIndex, String value);

    /**
     * Sets a <code>value</code> into <code>ps</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param ps PreparedStatement
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setObject(PreparedStatement ps, int paramIndex, Object value);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    boolean getBoolean(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    char getChar(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    byte getByte(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    short getShort(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    int getInt(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    long getLong(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    float getFloat(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    double getDouble(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    String getString(ResultSet resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    Object getObject(ResultSet resultSet, int exprIndex);
}