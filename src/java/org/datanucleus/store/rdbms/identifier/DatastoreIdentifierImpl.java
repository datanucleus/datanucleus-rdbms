/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved.
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
2003 Erik Bengtson - refactored
2004 Andy Jefferson - coding standards
2004 Andy Jefferson - fix to case of UpperCase requested, but not supported
2004 Andy Jefferson - fix to conversion of names in setJavaName() when
                     user has selected PreserveCase
2004 Andy Jefferson - updates for use of quoted identifiers
2005 Andy Jefferson - removed javaName, identifierQuoteString
2008 Andy Jefferson - refactored to be RDBMS-independent
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.identifier;

import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.schema.naming.NamingCase;

/**
 * Implementation of a datastore identifier.
 * This can be something like a table name, column name, PK name, etc.
 */
public class DatastoreIdentifierImpl implements DatastoreIdentifier
{
    /** Datastore adapter */
    protected final DatastoreAdapter dba;

    /** Datastore Identifier factory. */
    protected final IdentifierFactory factory;

    /** The identifier name. */
    protected String identifierName;

    /** catalog name. **/
    protected String catalogName;
    
    /** schema/owner name. **/
    protected String schemaName;

    /** String representation to reduce number of times the toString is constructed **/
    private String toString;

    /**
     * Constructor.
     * @param factory Identifier factory
     * @param sqlIdentifier The SQL identifier name
     */
    protected DatastoreIdentifierImpl(IdentifierFactory factory, String sqlIdentifier)
    {
        this.dba = factory.getDatastoreAdapter();
        this.factory = factory;
        this.identifierName = toCase(sqlIdentifier);
    }

    /**
     * Mutator for the sql identifier. Takes account of the DB policy on upper,
     * lower and mixed case identifiers. 
     * Optionally can truncate the identifier if it exceeds the "getMaxLength()".
     * @param identifierName The Identifier
     */
    protected String toCase(String identifierName)
    {
        if (factory.getNamingCase() == NamingCase.LOWER_CASE ||
            factory.getNamingCase() == NamingCase.LOWER_CASE_QUOTED)
        {
            return identifierName.toLowerCase();
        }
        else if (factory.getNamingCase() == NamingCase.UPPER_CASE ||
            factory.getNamingCase() == NamingCase.UPPER_CASE_QUOTED)
        {
            return identifierName.toUpperCase();
        }
        return identifierName;
    }

    /**
     * Accessor for the identifier name part of the identifier.
     * @return identifier name
     */
    public String getIdentifierName()
    {
        return identifierName;
    }

    /**
     * Sets the catalog name
     * @param catalogName The catalog name
     */
    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }
    
    /**
     * Sets the schema name
     * @param schemaName The schema name
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * Accessor for the catalog name
     * @return The catalog name
     */
    public String getCatalogName()
    {
        return catalogName;
    }

    /**
     * Accessor for the schema name
     * @return The schema name
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * Hash code method.
     * @return The hash code
     */
    public int hashCode()
    {
        return identifierName.hashCode();
    }

    /**
     * Equality operator to judge if 2 identifiers are equal.
     * <ul>
     * <li>Comparing NULL schema/owner names in one or both objects evaluates to true</li>
     * <li>Comparing NULL catalog names in one or both objects evaluates to true</li>
     * </ul>
     * TODO change the below behavior. will require changing all places creating an identifier to set the 
     * catalog name and schema name from jdo metadata and/or metadata collected from the db 
     * @param obj Object to compare against
     * @return Whether they are equal
     */
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof DatastoreIdentifierImpl))
        {
            return false;
        }

        DatastoreIdentifierImpl id = (DatastoreIdentifierImpl)obj;
        return this.identifierName.equals(id.identifierName) &&
               (this.schemaName == null ? true : (id.schemaName == null || this.schemaName.equals(id.schemaName))) &&
               (this.catalogName == null ? true : (id.catalogName == null || this.catalogName.equals(id.catalogName)));
    }

    /**
     * Method to output the name of the identifier. This will be quoted where necessary.
     * Will not include the catalog/schema names.
     * @return The identifier name with any necessary quoting
     */
    public String toString()
    {
        if (toString == null)
        {
            String identifierQuoteString = dba.getIdentifierQuoteString();
    
            // Note that by adding on the quotes here we assume that the limits on the size of
            // the SQLIdentifier are not including the quotes. This may or may not be correct
            if (dba.isReservedKeyword(identifierName))
            {
                // Identifier should be quoted since its a SQL keyword
                toString = identifierQuoteString + identifierName + identifierQuoteString;
            }
            else
            {
                if (factory.getNamingCase() == NamingCase.LOWER_CASE_QUOTED ||
                    factory.getNamingCase() == NamingCase.MIXED_CASE_QUOTED ||
                    factory.getNamingCase() == NamingCase.UPPER_CASE_QUOTED)
                {
                    // Identifier should be quoted since our case requires it
                    toString = identifierQuoteString + identifierName + identifierQuoteString;
                }
                else
                {
                    toString = identifierName;
                }
            }
        }
        return toString;
    }

    /**
     * Accessor for a fully-qualified version of the identifier name.
     * Allows for catalog/schema (if specified and if supported)
     * @param adapterCase Whether to use adapter case for the returned name
     * @return The fully-qualified name
     */
    public final String getFullyQualifiedName(boolean adapterCase)
    {
        // Check whether the adapter supports the fully qualified name
        boolean supportsCatalogName = dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS);
        boolean supportsSchemaName = dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS);
        String separator = dba.getCatalogSeparator();

        // Fully qualify the name with catalog/schema if required
        StringBuilder name = new StringBuilder();

        if (supportsCatalogName && catalogName != null)
        {
            if (adapterCase)
            {
                name.append(factory.getIdentifierInAdapterCase(catalogName));
            }
            else
            {
                name.append(catalogName);
            }
            name.append(separator);
        }
        if (supportsSchemaName && schemaName != null)
        {
            if (adapterCase)
            {
                name.append(factory.getIdentifierInAdapterCase(schemaName));
            }
            else
            {
                name.append(schemaName);
            }
            name.append(separator);
        }
        if (adapterCase)
        {
            name.append(factory.getIdentifierInAdapterCase(toString()));
        }
        else
        {
            name.append(toString());
        }

        return name.toString();
    }
}