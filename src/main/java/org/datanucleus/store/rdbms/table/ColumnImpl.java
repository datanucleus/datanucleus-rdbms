/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved.
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
2003 Erik Bengtson - added a replaceAll operation in substitution for
                    java.lang.String.replaceAll of J2SDK 1.4. Now we
                    can compile with J2SDK 1.3.1
2003 Erik Bengtson - moved replaceAll to StringUtils class
2003 Andy Jefferson - updated the DEFAULT_MAX_STRING_LENGTH to be controllable
                     via a property.
2004 Andy Jefferson - updated the NULL/NOT NULL so that we always specify the 
                     nullability of a column on creation
2004 Erik Bengtson - added columnOptions getters that can be read by
                    the SetTable or MapTable (Collection). columnOptions is
                    used for create an equivalent column on the set table
2004 Andy Jefferson - added Auto, Unique, Default, options to column SQL options
2006 Andy Jefferson - removed co, dba, lengthType, copying of ColumnMetaData
2008 Andy Jefferson - changed to implement interface, use byte "flags", cleanup
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.StringTokenizer;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.exceptions.ColumnDefinitionException;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.IncompatibleDataTypeException;
import org.datanucleus.store.rdbms.exceptions.WrongPrecisionException;
import org.datanucleus.store.rdbms.exceptions.WrongScaleException;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.datastore.DatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Implementation of a Column in an RDBMS datastore. 
 * Contains the full definition of the column, its type, size, whether it is identity, 
 * nullable, part of the PK etc. The SQL column definition is generated here.
 */ 
public class ColumnImpl implements Column
{
    private static final byte PK = (byte) (1<<0); // Part of PK?
    private static final byte NULLABLE = (byte) (1<<1); // Column can store nulls?
    private static final byte UNIQUE = (byte) (1<<2); // Values are unique?
    private static final byte DEFAULTABLE = (byte) (1<<3); // Defaultable column on inserts?
    private static final byte IDENTITY = (byte) (1<<4); // AUTO_INCREMENT, SERIAL etc?

    /** Identifier for the column in the datastore. */
    protected DatastoreIdentifier identifier;

    /** ColumnMetaData for this column. */
    protected ColumnMetaData columnMetaData;

    /** Table containing this column in the datastore. */
    protected final Table table;

    /** Datastore mapping for this column. */
    protected DatastoreMapping datastoreMapping = null;

    /** Java type that this column is storing. (can we just get this from the mapping above ?) */
    protected final String storedJavaType;

    /** SQL Type info for the JDBC type being stored in this column */
    protected SQLTypeInfo typeInfo;

    /** Optional constraints to apply to this column in its SQL specification. */
    protected String constraints;

    /** Operational flags, for nullability, PK, autoinc, etc. */
    protected byte flags;

    /** Default value accepted by the datastore for this column, from DatabaseMetaData. */
    protected Object defaultValue;

    /** Function wrapping the column (for example, SQRT(COLUMN)). */
    protected String[] wrapperFunction;

    /**
     * Constructor.
     * @param table The table in the datastore that this column belongs to.
     * @param javaType The type of data being stored in this column
     * @param identifier The identifier of the column (in the datastore).
     * @param colmd The ColumnMetaData for this column
     */
    public ColumnImpl(Table table, String javaType, DatastoreIdentifier identifier, ColumnMetaData colmd)
    {
        this.table = table;
        this.storedJavaType = javaType;

		typeInfo = null;
		constraints = null;
		flags = 0;

        setIdentifier(identifier);
        if (colmd == null)
        {
            // Create a default ColumnMetaData since none provided
            columnMetaData = new ColumnMetaData();
        }
        else
        {
            // TODO Consider making a copy here
            columnMetaData = colmd;
        }

        // Nullability
        if (columnMetaData.getAllowsNull() != null && columnMetaData.isAllowsNull())
        {
            // MetaData requires it to be nullable
            setNullable(true);
        }

        // Uniqueness
        if (columnMetaData.getUnique())
        {
            // MetaData requires it to be unique
            setUnique(true);
        }

        wrapperFunction = new String[3];
		wrapperFunction[WRAPPER_FUNCTION_SELECT]= "?";
		wrapperFunction[WRAPPER_FUNCTION_INSERT]= "?";
		wrapperFunction[WRAPPER_FUNCTION_UPDATE]= "?";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#getName()
     */
    @Override
    public String getName()
    {
        return identifier.toString();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#getMemberColumnMapping()
     */
    @Override
    public MemberColumnMapping getMemberColumnMapping()
    {
        throw new UnsupportedOperationException("Not supported on this Column");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#getColumnType()
     */
    @Override
    public ColumnType getColumnType()
    {
        throw new UnsupportedOperationException("Not supported on this Column");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setJdbcType(org.datanucleus.metadata.JdbcType)
     */
    @Override
    public Column setJdbcType(JdbcType jdbcType)
    {
        throw new UnsupportedOperationException("Not supported on this Column");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#getJdbcType()
     */
    @Override
    public JdbcType getJdbcType()
    {
        return typeInfo != null ? JdbcType.getEnumByValue(typeInfo.getDataType()) : null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setTypeName(java.lang.String)
     */
    @Override
    public Column setTypeName(String type)
    {
        throw new UnsupportedOperationException("Not supported on this Column");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#getTypeName()
     */
    @Override
    public String getTypeName()
    {
        return typeInfo.getTypeName();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setPosition(int)
     */
    @Override
    public Column setPosition(int pos)
    {
        throw new UnsupportedOperationException("Not supported on this Column");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#getPosition()
     */
    @Override
    public int getPosition()
    {
        throw new UnsupportedOperationException("Not supported on this Column");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#isUnlimitedLength()
     */
    public boolean isUnlimitedLength()
    {
        // TODO Enable the two commented out lines so that we can allow people to have "BLOB(1024)" etc
        if (columnMetaData.getJdbcType() != null && (columnMetaData.getJdbcType() == JdbcType.BLOB || columnMetaData.getJdbcType() == JdbcType.CLOB)/* &&
            !typeInfo.isAllowsPrecisionSpec()*/)
        {
            // Allow for jdbc-type=BLOB/CLOB
            return true;
        }
        else if (columnMetaData.getSqlType() != null && columnMetaData.getSqlType().toLowerCase().indexOf("lob") > 0/* &&
            !typeInfo.isAllowsPrecisionSpec()*/)
        {
            // Allow for sql-type=BLOB/CLOB
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getIdentifier()
     */
    public DatastoreIdentifier getIdentifier()
    {
        return identifier;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#setIdentifier(org.datanucleus.store.rdbms.DatastoreIdentifier)
     */
    public void setIdentifier(DatastoreIdentifier identifier)
    {
        this.identifier = identifier;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getTable()
     */
    public Table getTable()
    {
        return table;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getDatastoreMapping()
     */
    public DatastoreMapping getDatastoreMapping()
    {
        return datastoreMapping;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#setDatastoreMapping(org.datanucleus.store.rdbms.mapping.DatastoreMapping)
     */
    public void setDatastoreMapping(DatastoreMapping mapping)
    {
        datastoreMapping = mapping;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getJavaTypeMapping()
     */
    public JavaTypeMapping getJavaTypeMapping()
    {
        return datastoreMapping.getJavaTypeMapping();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getStoredJavaType()
     */
    public String getStoredJavaType()
    {
        return storedJavaType;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#setTypeInfo(org.datanucleus.store.rdbms.schema.SQLTypeInfo)
     */
    public final Column setTypeInfo(SQLTypeInfo typeInfo)
    {
        if (this.typeInfo == null)
        {
            this.typeInfo = typeInfo;
        }
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getTypeInfo()
     */
    public final SQLTypeInfo getTypeInfo()
    {
        return typeInfo;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getStoreManager()
     */
    public RDBMSStoreManager getStoreManager()
    {
        return table.getStoreManager();
    }

    /**
     * Accessor for the precision of data in the column.
     * @return The precision of data in the column
     */
    private int getSQLPrecision()
    {
        int sqlPrecision = - 1;
        if (columnMetaData.getLength() != null && columnMetaData.getLength().intValue() > 0)
        {
            // User-provided length, so use it
            sqlPrecision = columnMetaData.getLength().intValue();
        }
        else if (isUnlimitedLength())
        {
            // Use the precision for "unlimited length" if defined
            int ulpv = getStoreManager().getDatastoreAdapter().getUnlimitedLengthPrecisionValue(typeInfo);
            if (ulpv > 0)
            {
                sqlPrecision = ulpv;
            }
        }

        // Databases like Derby that use BIT types for binary need to have the length expressed in bits, not bytes.
        if (typeInfo.getTypeName().toLowerCase().startsWith("bit"))
        {
            return sqlPrecision * 8;
        }
        return sqlPrecision;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getSQLDefinition()
     */
    public String getSQLDefinition()
    {
        StringBuilder def = new StringBuilder(identifier.toString());

        if (!StringUtils.isWhitespace(columnMetaData.getColumnDdl()))
        {
            // User-defined DDL, so assume they set the type etc to something sensible
            // Note that the JPA spec doesn't explicitly specify if this has to include the type or not
            def.append(" ").append(columnMetaData.getColumnDdl());
            return def.toString();
        }

        StringBuilder typeSpec = new StringBuilder(typeInfo.getTypeName());
        DatastoreAdapter adapter = getStoreManager().getDatastoreAdapter();

        // Add type specification.
        boolean specifyType = true;
        if (adapter.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS) && isIdentity() &&
            !adapter.supportsOption(DatastoreAdapter.AUTO_INCREMENT_COLUMN_TYPE_SPECIFICATION))
        {
            specifyType = false;
        }

        if (specifyType)
        {
            // Parse and append createParams to the typeName if it looks like it's supposed to be appended,
            // i.e. if it contains parentheses, and the type name itself doesn't. createParams is mighty
            // ill-defined by the JDBC spec, but attempt to interpret it.
            if (typeInfo.getCreateParams() != null && typeInfo.getCreateParams().indexOf('(') >= 0 &&
                typeInfo.getTypeName().indexOf('(') < 0)
            {
                StringTokenizer toks = new StringTokenizer(typeInfo.getCreateParams());
                
                while (toks.hasMoreTokens())
                {
                    String tok = toks.nextToken();
                    
                    if (tok.startsWith("[") && tok.endsWith("]"))
                    {
                        // The brackets look like they indicate an optional param so
                        // skip
                        continue;
                    }
                    
                    typeSpec.append(" " + tok);
                }
            }

            // Add any precision - note that there is no obvious flag in the JDBC typeinfo to
            // tell us whether the type allows this specification or not, and many JDBC just
            // return crap anyway. We use the allowsPrecisionSpec flag for this
            StringBuilder precSpec = new StringBuilder();
            int sqlPrecision = getSQLPrecision();
            if (sqlPrecision > 0 && typeInfo.isAllowsPrecisionSpec())
            {
                precSpec.append(sqlPrecision);
                if (columnMetaData.getScale() != null)
                {
                    precSpec.append("," + columnMetaData.getScale());
                }
            }
            else if (sqlPrecision > 0 && !typeInfo.isAllowsPrecisionSpec())
            {
                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("020183", this.toString()));
            }

            int lParenIdx = typeSpec.toString().indexOf('(');
            int rParenIdx = typeSpec.toString().indexOf(')', lParenIdx);
            if (lParenIdx > 0 && rParenIdx > 0)
            {
                // Some databases (like DB2) give you typeNames with ()'s already
                // present ready for you to insert the values instead of appending them.
                if (precSpec.length() > 0)
                {
                    typeSpec.replace(lParenIdx + 1, rParenIdx, precSpec.toString());
                }
                else if (rParenIdx == lParenIdx + 1)
                {
                    throw new ColumnDefinitionException(Localiser.msg("020184", this.toString()));
                }
            }
            else if (precSpec.length() > 0)
            {
                typeSpec.append('(');
                typeSpec.append(precSpec.toString());
                typeSpec.append(')');
            }
            def.append(" " + typeSpec.toString());
        }

        // Add DEFAULT (if specifiable before NULL)
        if (adapter.supportsOption(DatastoreAdapter.DEFAULT_BEFORE_NULL_IN_COLUMN_OPTIONS) && 
            adapter.supportsOption(DatastoreAdapter.DEFAULT_KEYWORD_IN_COLUMN_OPTIONS) &&
            columnMetaData.getDefaultValue() != null)
        {
            def.append(" ").append(getDefaultDefinition());
        }

        if (isIdentity() && isPrimaryKey() && adapter.supportsOption(DatastoreAdapter.AUTO_INCREMENT_PK_IN_CREATE_TABLE_COLUMN_DEF))
        {
            def.append(" PRIMARY KEY");
        }

        // Nullability
        if (adapter.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS) && isIdentity() &&
            !adapter.supportsOption(DatastoreAdapter.AUTO_INCREMENT_KEYS_NULL_SPECIFICATION))
        {
            // Do nothing since the adapter doesn't allow NULL specifications with autoincrement/identity
        }
        else
        {
            if (!isNullable())
            {
                if (columnMetaData.getDefaultValue() == null ||
                    adapter.supportsOption(DatastoreAdapter.DEFAULT_KEYWORD_WITH_NOT_NULL_IN_COLUMN_OPTIONS))
                {
                    def.append(" NOT NULL");
                }
            }
            else if (typeInfo.getNullable() == DatabaseMetaData.columnNullable)
            {
                if (adapter.supportsOption(DatastoreAdapter.NULLS_KEYWORD_IN_COLUMN_OPTIONS))
                {
                    def.append(" NULL");
                }
            }
        }

        // Add DEFAULT (if specifiable after NULL)
        if (!adapter.supportsOption(DatastoreAdapter.DEFAULT_BEFORE_NULL_IN_COLUMN_OPTIONS) && 
            adapter.supportsOption(DatastoreAdapter.DEFAULT_KEYWORD_IN_COLUMN_OPTIONS) &&
            columnMetaData.getDefaultValue() != null)
        {
            def.append(" ").append(getDefaultDefinition());
        }

        // Constraints checks
        if (adapter.supportsOption(DatastoreAdapter.CHECK_IN_CREATE_STATEMENTS) && constraints != null)
        {
            def.append(" " + constraints.toString());
        }

        // Auto Increment
        if (adapter.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS) && isIdentity())
        {
            def.append(" " + adapter.getAutoIncrementKeyword());
        }

        // Uniqueness
        if (isUnique() && !adapter.supportsOption(DatastoreAdapter.UNIQUE_IN_END_CREATE_STATEMENTS))
        {
            def.append(" UNIQUE");
        }

        return def.toString();
    }

    /**
     * Convenience method to return the "DEFAULT" part of the column definition.
     * @return The default part of the column definition.
     */
    private String getDefaultDefinition()
    {
        if (columnMetaData.getDefaultValue().equalsIgnoreCase("#NULL"))
        {
            // Special case
            // Note that this really ought to be coordinated with "DEFAULT_KEYWORD_WITH_NOT_NULL_IN_COLUMN_OPTIONS"
            this.defaultValue = null;
            return "DEFAULT NULL";
        }

        // Quote any character types (CHAR, VARCHAR, BLOB, CLOB)
        if (typeInfo.getTypeName().toUpperCase().indexOf("CHAR") >= 0 || typeInfo.getTypeName().toUpperCase().indexOf("LOB") >= 0)
        {
            // NOTE We use single quote here but would be better to take
            // some character from the DatabaseMetaData. The "identifierQuoteString"
            // does not work for string quoting here for Postgres
            this.defaultValue = columnMetaData.getDefaultValue();
            return "DEFAULT '" + columnMetaData.getDefaultValue() + "'";
        }
        else if (typeInfo.getTypeName().toUpperCase().indexOf("BIT") == 0)
        {
            if (columnMetaData.getDefaultValue().equalsIgnoreCase("true") || columnMetaData.getDefaultValue().equalsIgnoreCase("false"))
            {
                // Quote any "true"/"false" values for BITs
                this.defaultValue = columnMetaData.getDefaultValue();
                return "DEFAULT '" + columnMetaData.getDefaultValue() + "'";
            }
        }

        this.defaultValue = columnMetaData.getDefaultValue();
        return "DEFAULT " + columnMetaData.getDefaultValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#initializeColumnInfoFromDatastore(org.datanucleus.store.rdbms.schema.RDBMSColumnInfo)
     */
    public void initializeColumnInfoFromDatastore(RDBMSColumnInfo ci)
    {
        String column_default = ci.getColumnDef();
        if (!StringUtils.isWhitespace(column_default))
        {
            // Set column defaultValue using defaultValue defined on the datastore column
            if (column_default.startsWith("'") && column_default.endsWith("'"))
            {
                // JDBC javadoc, if starts with single quotes then is a string
                String colDefString = column_default.replace("'", "");
                setDefaultable(colDefString);
            }
            else
            {
                if (!column_default.equalsIgnoreCase("null"))
                {
                    // Non-null value so use it (null is the default so don't need that)
                    String columnDef = column_default.replace("'", "").replace("\"", "").replace(")", "").replace("(", "");
                    if (!columnDef.equalsIgnoreCase("null"))
                    {
                        setDefaultable(columnDef);
                    }
                }
            }
        }

        // TODO Make sure that this lines up with the defaultValue when set.
        try
        {
            setIdentity(getStoreManager().getDatastoreAdapter().isIdentityFieldDataType(ci.getColumnDef()));
        }
        catch( UnsupportedOperationException ex)
        {
            //do nothing, or maybe log
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#validate(org.datanucleus.store.rdbms.schema.RDBMSColumnInfo)
     */
    public void validate(RDBMSColumnInfo ci)
    {
        if (!typeInfo.isCompatibleWith(ci))
        {
            throw new IncompatibleDataTypeException(this, typeInfo.getDataType(), ci.getDataType());
        }
        if (ci.getDataType() == Types.OTHER)
        {
            return;
        }
        if (table instanceof TableImpl)
        {
            // Only validate precision/scale/nullability for tables
            if (typeInfo.isAllowsPrecisionSpec())
            {
                int actualPrecision = ci.getColumnSize();
                int actualScale = ci.getDecimalDigits();
                int sqlPrecision = getSQLPrecision();
                
                if (sqlPrecision > 0 && actualPrecision > 0)
                {
                    if (sqlPrecision != actualPrecision)
                    {
                        if (this.columnMetaData != null && this.columnMetaData.getParent() != null && 
                            (this.columnMetaData.getParent() instanceof AbstractMemberMetaData))
                        {
                            //includes the field name in error msg
                            throw new WrongPrecisionException(this.toString(), sqlPrecision, 
                                actualPrecision, 
                                ((AbstractMemberMetaData)this.columnMetaData.getParent()).getFullFieldName());
                        }

                        throw new WrongPrecisionException(this.toString(), sqlPrecision, actualPrecision);
                    }
                }

                if (columnMetaData.getScale() != null && actualScale >= 0)
                {
                    if (columnMetaData.getScale().intValue() != actualScale)
                    {
                        if (this.columnMetaData.getParent() != null && this.columnMetaData.getParent() instanceof AbstractMemberMetaData)
                        {
                            //includes the field name in error msg
                            throw new WrongScaleException(this.toString(), columnMetaData.getScale().intValue(), actualScale, 
                                ((AbstractMemberMetaData)this.columnMetaData.getParent()).getFullFieldName());
                        }
                        throw new WrongScaleException(this.toString(), columnMetaData.getScale().intValue(), actualScale);
                    }
                }
            }

            String actualIsNullable = ci.getIsNullable();
            if (actualIsNullable.length() > 0)
            {
                switch (Character.toUpperCase(actualIsNullable.charAt(0)))
                {
                    case 'Y' :
                        if (!isNullable())
                        {
                            NucleusLogger.DATASTORE.warn(Localiser.msg("020025", this));
                        }
                        break;

                    case 'N' :
                        // TODO convert to default value somewhere at runtime if
                        // column has a default value and "default"
                        break;

                    default :
                        break;
                }
            }
            
            try
            {
                if (isIdentity() != getStoreManager().getDatastoreAdapter().isIdentityFieldDataType(ci.getColumnDef()))
                {
                    //TODO localise
                    if (isIdentity())
                    {
                        throw new NucleusException("Expected an auto increment column ("+getIdentifier()+
                            ") in the database, but it is not").setFatal();
                    }

                    throw new NucleusException("According to the user metadata, the column ("+
                            getIdentifier()+") is not auto incremented, but the database says it is.").setFatal();
                }
            }
            catch (UnsupportedOperationException ex)
            {
                //ignore this validation step
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#setConstraints(java.lang.String)
     */
    public final Column setConstraints(String constraints)
    {
        this.constraints = constraints;
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setPrimaryKey()
     */
    public final Column setPrimaryKey()
    {
        flags |= PK;
        //primary keys cannot be null
        flags &= ~NULLABLE;
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setNullable(boolean)
     */
    public final Column setNullable(boolean flag)
    {
        if (flag)
        {
            flags |= NULLABLE;
        }
        else
        {
            flags &= ~NULLABLE;
        }
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setDefaultable(java.lang.Object)
     */
    public final Column setDefaultable(Object defaultValue)
    {
        flags |= DEFAULTABLE;
        this.defaultValue = defaultValue;
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setUnique(boolean)
     */
    public final Column setUnique(boolean flag)
    {
        if (flag)
        {
            flags |= UNIQUE;
        }
        else
        {
            flags &= ~UNIQUE;
        }
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#setIdentity(boolean)
     */
    public Column setIdentity(boolean identity)
    {
        if (identity)
        {
            flags |= IDENTITY;
        }
        else
        {
            flags &= ~IDENTITY;
        }
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#isPrimaryKey()
     */
    public final boolean isPrimaryKey()
    {
        return ((flags & PK) != 0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#isNullable()
     */
    public final boolean isNullable()
    {
        return ((flags & NULLABLE) != 0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#isDefaultable()
     */
    public final boolean isDefaultable()
    {
        return ((flags & DEFAULTABLE) != 0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#isUnique()
     */
    public final boolean isUnique()
    {
        return ((flags & UNIQUE) != 0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#isIdentity()
     */
    public boolean isIdentity()
    {
        return ((flags & IDENTITY) != 0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#applySelectFunction(java.lang.String)
     */
    public String applySelectFunction(String replacementValue)
    {
        if (replacementValue == null)
        {
            return wrapperFunction[WRAPPER_FUNCTION_SELECT];
        }
        if (wrapperFunction[WRAPPER_FUNCTION_SELECT] != null)
        {
            return wrapperFunction[WRAPPER_FUNCTION_SELECT].replace("?", replacementValue);
        }
        return replacementValue;
    }    

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getDefaultValue()
     */
    public Object getDefaultValue()
    {
        return defaultValue;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getColumnMetaData()
     */
    public final ColumnMetaData getColumnMetaData()
    {
        return columnMetaData;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getFieldMetaData()
     */
    public AbstractMemberMetaData getMemberMetaData()
    {
        if (columnMetaData != null && columnMetaData.getParent() instanceof AbstractMemberMetaData)
        {
            return (AbstractMemberMetaData)columnMetaData.getParent();
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Column#setColumnMetaData(org.datanucleus.metadata.ColumnMetaData)
     */
    public Column setColumnMetaData(ColumnMetaData colmd)
    {
        if (colmd == null)
        {
            // Nothing to do since no definition of requirements
            return this;
        }

        if (colmd.getJdbcType() != null)
        {
            columnMetaData.setJdbcType(colmd.getJdbcType());
        }
        if (colmd.getSqlType() != null)
        {
            columnMetaData.setSqlType(colmd.getSqlType());
        }
        if (colmd.getName() != null)
        {
            columnMetaData.setName(colmd.getName());
        }
        if (colmd.getAllowsNull() != null)
        {
            columnMetaData.setAllowsNull(Boolean.valueOf(colmd.isAllowsNull()));
        }
        if (colmd.getLength() != null)
        {
            columnMetaData.setLength(colmd.getLength());
        }
        if (colmd.getScale() != null)
        {
            columnMetaData.setScale(colmd.getScale());
        }

        if (colmd.getAllowsNull() != null && colmd.isAllowsNull())
        {
            // MetaData requires it to be nullable
            setNullable(true);
        }
        if (colmd.getUnique())
        {
            // MetaData requires it to be unique
            setUnique(true);
        }
        return this;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getConstraints()
     */
    public String getConstraints()
    {
        return constraints;
    }
    
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#checkPrimitive()
     */
    public final void checkPrimitive() throws ColumnDefinitionException
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#checkInteger()
     */
    public final void checkInteger() throws ColumnDefinitionException
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#checkDecimal()
     */
    public final void checkDecimal() throws ColumnDefinitionException
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#checkString()
     */
    public final void checkString() throws ColumnDefinitionException
    {
        if (columnMetaData.getJdbcType() == null)
        {
            columnMetaData.setJdbcType(JdbcType.VARCHAR);
        }
        if (columnMetaData.getLength() == null)
        {
            // Use the default string length
            columnMetaData.setLength(getStoreManager().getIntProperty(RDBMSPropertyNames.PROPERTY_RDBMS_STRING_DEFAULT_LENGTH));
        }
    }

	/* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#copyConfigurationTo(org.datanucleus.store.rdbms.table.Column)
     */
	public void copyConfigurationTo(Column colIn)
    {
        ColumnImpl col = (ColumnImpl) colIn;
        col.typeInfo = this.typeInfo;
        col.flags |= this.flags;
        col.flags &= ~PK;
        col.flags &= ~UNIQUE;
        col.flags &= ~NULLABLE;
        col.flags &= ~IDENTITY;
        col.flags &= ~DEFAULTABLE;
        col.defaultValue = this.defaultValue;
        col.wrapperFunction = this.wrapperFunction;

        // Copy key aspects of ColumnMetaData across. May need to copy other parts in future
        if (this.columnMetaData.getJdbcType() != null)
        {
            col.columnMetaData.setJdbcType(this.columnMetaData.getJdbcType());
        }
        if (this.columnMetaData.getSqlType() != null)
        {
            col.columnMetaData.setSqlType(this.columnMetaData.getSqlType());
        }
        if (this.columnMetaData.getLength() != null)
        {
            col.getColumnMetaData().setLength(this.columnMetaData.getLength());
        }
        if (this.columnMetaData.getScale() != null)
        {
            col.getColumnMetaData().setScale(this.getColumnMetaData().getScale());
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#setWrapperFunction(java.lang.String, int)
     */
    public void setWrapperFunction(String wrapperFunction, int wrapperMode)
    {
        if (wrapperFunction != null && wrapperMode == WRAPPER_FUNCTION_SELECT && wrapperFunction.indexOf("?") < 0)
        {
            throw new NucleusUserException("Wrapping function must have one ? (question mark). e.g. SQRT(?)");
        }
        this.wrapperFunction[wrapperMode] = wrapperFunction;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Column#getWrapperFunction(int)
     */
    public String getWrapperFunction(int wrapperMode)
    {
        return wrapperFunction[wrapperMode];
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof ColumnImpl))
        {
            return false;
        }

        ColumnImpl col = (ColumnImpl)obj;
        return table.equals(col.table) && identifier.equals(col.identifier);
    }

    public int hashCode()
    {
        return table.hashCode() ^ identifier.hashCode();
    }

    public String toString()
    {
        return table.toString() + "." + identifier;
    }
}