package org.datanucleus.store.rdbms.discriminatordefiner;

import java.sql.ResultSet;

public interface CustomClassNameResolver
{
    /**
     * Method for returning persistent object class name represented by ResultSet row.
     * Return null to allow for resolving class name in normal manner.
     * @param rs result-set for next row to get class name for
     * @return null if leave class name resolving up to normal framework,
     * but for custom implementations return class name for object represented in result-set
     */
    String getCustomClassName(ResultSet rs);
}
