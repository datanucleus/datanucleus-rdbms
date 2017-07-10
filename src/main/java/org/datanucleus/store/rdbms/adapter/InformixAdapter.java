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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Provides methods for adapting SQL language elements to the Informix
 * database. Overrides some methods in DatabaseAdapter where Informix behaviour differs.
 * 
 * Informix databases must be created WITH LOG MODE ANSI, otherwise
 * errors like "Transaction Not Supported", "Not in transaction" will appear.
 * See the informix info.
 */
public class InformixAdapter extends BaseDatastoreAdapter
{
    /**
     * Constructor.
     * @param metadata MetaData for the DB
     **/
    public InformixAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(PROJECTION_IN_TABLE_REFERENCE_JOINS);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);

        // Informix 11.x: We create indexes before foreign keys to avoid duplicate indexes error
        // since Informix creates an index automatically
        supportedOptions.add(CREATE_INDEXES_BEFORE_FOREIGN_KEYS);

        supportedOptions.remove(AUTO_INCREMENT_KEYS_NULL_SPECIFICATION);
        supportedOptions.remove(AUTO_INCREMENT_COLUMN_TYPE_SPECIFICATION);
        supportedOptions.remove(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
    }

    /**
     * Creates the auxiliary functions/procedures in the schema 
     * @param conn the connection to the datastore
     */
    public void initialiseDatastore(Connection conn)
    {
        try
        {
            Statement st = conn.createStatement();
            try
            {
                st.execute(getSTRPOSDropFunction());
            }
            catch (SQLException e)
            {
                NucleusLogger.DATASTORE.warn(Localiser.msg("051027",e));
            }
            try
            {
                st.execute(getSTRPOSFunction());
            }
            catch (SQLException e)
            {
                NucleusLogger.DATASTORE.warn(Localiser.msg("051027",e));
            }
            st.close();
        }
        catch (SQLException e)
        {
            NucleusLogger.DATASTORE_SCHEMA.warn("Exception when trying to initialise datastore", e);
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }
    
    public String getVendorID()
    {
        return "informix";
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.adapter.InformixTypeInfo(rs);
    }

    /**
     * Accessor for an identifier quote string.
     * @return Identifier quote string.
     */
    public String getIdentifierQuoteString()
    {
    	// the documentation says a double quote, but it seems to not work
        return "";
    }   

    // ---------------------------- AutoIncrement Support ---------------------------

    /**
     * Accessor for the autoincrement sql access statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     */
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        String useSerial = (String)getValueForProperty(RDBMSPropertyNames.PROPERTY_RDBMS_INFORMIX_USE_SERIAL_FOR_IDENTITY);
        if (useSerial != null && useSerial.equalsIgnoreCase("true"))
        {
            // Default in JPOX, but equates to int
            return "SELECT first 1 dbinfo('sqlca.sqlerrd1') from systables";
        }

        // Default in DataNucleus, equating to long
        // Refer to http://www.jpox.org/servlet/jira/browse/NUCRDBMS-161
        return "SELECT first 1 dbinfo('serial8') from systables";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     */
    public String getAutoIncrementKeyword()
    {
        String useSerial = (String)getValueForProperty(RDBMSPropertyNames.PROPERTY_RDBMS_INFORMIX_USE_SERIAL_FOR_IDENTITY);
        if (useSerial != null && useSerial.equalsIgnoreCase("true"))
        {
            // Default in JPOX, but equates to int
            return "SERIAL";
        }

        // Default in DataNucleus, equating to long
        // Refer to http://www.jpox.org/servlet/jira/browse/NUCRDBMS-161
        return "SERIAL8";
    }

    /**
     * Informix 11.x does not support ALTER TABLE to define a primary key
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        // PK is created by the CREATE TABLE statement so we just return null
        return null;
    }    
    
    /**
     * Returns the appropriate SQL to add a foreign key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOREIGN KEY (BAR, BAZ) REFERENCES ABC (COL1, COL2) CONSTRAINT FOO_FK1
     * ALTER TABLE FOO ADD FOREIGN KEY (BAR, BAZ) REFERENCES ABC (COL1, COL2)
     * </pre>
     * @param fk An object describing the foreign key.
     * @param factory Identifier factory
     * @return  The text of the SQL statement.
     */
    public String getAddForeignKeyStatement(ForeignKey fk, IdentifierFactory factory)
    {
        if (fk.getName() != null)
        {
            String identifier = factory.getIdentifierInAdapterCase(fk.getName());
            return "ALTER TABLE " + fk.getTable().toString() + " ADD CONSTRAINT" + ' ' + fk + ' ' + "CONSTRAINT" + ' ' + identifier;
        }
        return "ALTER TABLE " + fk.getTable().toString() + " ADD " + fk;
    }
    
    
    /**
     * Returns the appropriate SQL to add a candidate key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_CK UNIQUE (BAZ)
     * ALTER TABLE FOO ADD UNIQUE (BAZ)
     * </pre>
     *
     * @param ck An object describing the candidate key.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getAddCandidateKeyStatement(CandidateKey ck, IdentifierFactory factory)
    {
        if (ck.getName() != null)
        {
            String identifier = factory.getIdentifierInAdapterCase(ck.getName());
            return "ALTER TABLE " + ck.getTable().toString() + " ADD CONSTRAINT" + ' ' + ck + ' ' + "CONSTRAINT" + ' ' + identifier;
        }
        return "ALTER TABLE " + ck.getTable().toString() + " ADD " + ck;
    }

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "SELECT FIRST 1 (CURRENT) FROM SYSTABLES";
    }

    /**
     * Creates a NUCLEUS_STRPOS function for Informix
     * @return the SQL NUCLEUS_STRPOS function
     */
    private String getSTRPOSFunction()
    {
        return "create function NUCLEUS_STRPOS(str char(40),search char(40),from smallint) returning smallint\n"+
        "define i,pos,lenstr,lensearch smallint;\n"+
        "let lensearch = length(search);\n"+
        "let lenstr = length(str);\n"+
        "if lenstr=0 or lensearch=0 then return 0; end if;\n"+
        "let pos=-1;\n"+
        "for i=1+from to lenstr\n"+
        "if substr(str,i,lensearch)=search then\n"+
        "let pos=i;\n"+
        "exit for;\n"+
        "end if;\n"+
        "end for;\n"+
        "return pos;\n"+
        "end function;";
    }

    /**
     * DROP a NUCLEUS_STRPOS function for Informix
     * @return the SQL NUCLEUS_STRPOS function
     */
    private String getSTRPOSDropFunction()
    {
        return "drop function NUCLEUS_STRPOS;";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#isStatementTimeout(java.sql.SQLException)
     */
    @Override
    public boolean isStatementTimeout(SQLException sqle)
    {
        if (sqle.getErrorCode() == -213)
        {
            return true;
        }

        return super.isStatementTimeout(sqle);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLOperationClass(java.lang.String)
     */
    @Override
    public Class getSQLOperationClass(String operationName)
    {
        if ("mod".equals(operationName)) return org.datanucleus.store.rdbms.sql.operation.Mod2Operation.class;

        return super.getSQLOperationClass(operationName);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLMethodClass(java.lang.String, java.lang.String)
     */
    @Override
    public Class getSQLMethodClass(String className, String methodName)
    {
        if (className != null)
        {
            if ("java.lang.String".equals(className) && "substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring3Method.class;
        }

        return super.getSQLMethodClass(className, methodName);
    }
}