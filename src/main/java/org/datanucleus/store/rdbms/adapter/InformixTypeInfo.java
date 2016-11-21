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
package org.datanucleus.store.rdbms.adapter;

import java.sql.ResultSet;
import java.sql.Types;

import org.datanucleus.store.rdbms.schema.SQLTypeInfo;

/**
 * SQL Type info for Informix datastores.
 */
public class InformixTypeInfo extends SQLTypeInfo
{
    /**
     * Constructs a type information object from the current row of the given result set.
     * @param rs The result set returned from DatabaseMetaData.getTypeInfo().
     */
    public InformixTypeInfo(ResultSet rs)
    {
        super(rs);

        /**
         VARCHAR(m,r)
            The VARCHAR data type stores single-byte and multibyte character
            sequences of varying length, where m is the maximum byte size of the
            column and r is the minimum amount of byte space reserved for that column.
            For more information on multibyte VARCHAR sequences
            
            The VARCHAR data type is the Informix implementation of a character
            varying data type.
            The ANSI standard data type for varying character strings is CHARACTER
            VARYING.
            You must specify the maximum size (m) of the VARCHAR column. The size of
            this parameter can range from 1 to 255 bytes. If you are placing an index on
            a VARCHAR column, the maximum size is 254 bytes. You can store shorter,
            but not longer, character strings than the value that you specify.
            Specifying the minimum reserved space (r) parameter is optional. This value
            can range from 0 to 255 bytes but must be less than the maximum size (m) of
            the VARCHAR column. If you do not specify a minimum space value, it
            defaults to 0. You should specify this parameter when you initially intend to
            insert rows with short or null data in this column, but later expect the data to
            be updated with longer values.
            Although the use of VARCHAR economizes on space used in a table, it has no
            effect on the size of an index. In an index based on a VARCHAR column, each
            index key has length m, the maximum size of the column.
        **/
        if (dataType == Types.VARCHAR)
        {
            precision = 255;
            typeName = "VARCHAR";
        }
    }
}