/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.discriminator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.DiscriminatorMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Convenience methods for handling discriminators.
 */
public class DiscriminatorUtils
{
    /**
     * Convenience method to generate a BooleanExpression for the associated discriminator value for
     * the specified class.
     * @param stmt The Query Statement to be updated
     * @param className The class name
     * @param dismd MetaData for the discriminator
     * @param discriminatorMapping Mapping for the discriminator
     * @param discrimSqlTbl SQLTable for the table with the discriminator
     * @param clr ClassLoader resolver
     * @return Boolean expression for this discriminator value
     */
    public static BooleanExpression getExpressionForDiscriminatorForClass(SQLStatement stmt, String className, DiscriminatorMetaData dismd, JavaTypeMapping discriminatorMapping,
            SQLTable discrimSqlTbl, ClassLoaderResolver clr)
    {
        DiscriminatorDefiner discriminatorDefiner = stmt.getRDBMSManager().getDiscriminatorDefiner(discriminatorMapping.getTable().getClassMetaData(), clr);
        final BooleanExpression customExpr = discriminatorDefiner != null ?
                discriminatorDefiner.getExpressionForDiscriminatorForClass(stmt, className, dismd, discriminatorMapping, discrimSqlTbl, clr)
                :
                null;
        if (customExpr != null)
        {
            return customExpr;
        }

        Object discriminatorValue = getDiscriminatorValueForClass(stmt.getRDBMSManager().getNucleusContext(), className, dismd, discriminatorMapping, clr);

        SQLExpression discrExpr = stmt.getSQLExpressionFactory().newExpression(stmt, discrimSqlTbl, discriminatorMapping);
        SQLExpression discrVal = stmt.getSQLExpressionFactory().newLiteral(stmt, discriminatorMapping, discriminatorValue);
        return discrExpr.eq(discrVal);
    }

    /**
     * Convenience method to generate a BooleanExpression for the associated discriminator value for
     * the specified class.
     * @param nucleusCtx NucleusContext
     * @param className The class name
     * @param dismd MetaData for the discriminator
     * @param discriminatorMapping Mapping for the discriminator
     * @param clr ClassLoader resolver
     * @return The discriminator value for this class
     */
    public static Object getDiscriminatorValueForClass(NucleusContext nucleusCtx, String className, DiscriminatorMetaData dismd, JavaTypeMapping discriminatorMapping,
            ClassLoaderResolver clr)
    {
        AbstractClassMetaData targetCmd = nucleusCtx.getMetaDataManager().getMetaDataForClass(className, clr);
        Object discriminatorValue = targetCmd.getDiscriminatorValue();
        if (dismd.getStrategy() == DiscriminatorStrategy.VALUE_MAP)
        {
            String strValue = null;
            if (targetCmd.getInheritanceMetaData() != null && targetCmd.getInheritanceMetaData().getDiscriminatorMetaData() != null)
            {
                strValue = targetCmd.getInheritanceMetaData().getDiscriminatorMetaData().getValue();
            }
            if (strValue == null)
            {
                // No value defined for this clause of the map
                strValue = className;
            }
            if (discriminatorMapping instanceof DiscriminatorMapping.DiscriminatorLongMapping)
            {
                try
                {
                    discriminatorValue = Integer.valueOf(strValue);
                }
                catch (NumberFormatException nfe)
                {
                    throw new NucleusUserException("Discriminator for " + className + " is not integer-based but needs to be!");
                }
            }
            else
            {
                discriminatorValue = strValue;
            }
        }
        return discriminatorValue;
    }

    /**
     * Method to return all possible discriminator values for the supplied class and its subclasses.
     * @param className Name of the class
     * @param discMapping The discriminator mapping
     * @param storeMgr StoreManager
     * @param clr ClassLoader resolver
     * @return The possible discriminator values
     */
    public static List getDiscriminatorValuesForMember(String className, JavaTypeMapping discMapping, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        List discrimValues = new ArrayList();
        DiscriminatorStrategy strategy = discMapping.getTable().getDiscriminatorMetaData().getStrategy();
        if (strategy != DiscriminatorStrategy.NONE)
        {
            MetaDataManager mmgr = storeMgr.getMetaDataManager();
            AbstractClassMetaData cmd = mmgr.getMetaDataForClass(className, clr);
            discrimValues.add(cmd.getDiscriminatorValue());

            Collection<String> subclasses = storeMgr.getSubClassesForClass(className, true, clr);
            if (subclasses != null && subclasses.size() > 0)
            {
                Iterator<String> subclassesIter = subclasses.iterator();
                while (subclassesIter.hasNext())
                {
                    String subclassName = subclassesIter.next();
                    AbstractClassMetaData subclassCmd = mmgr.getMetaDataForClass(subclassName, clr);
                    discrimValues.add(subclassCmd.getDiscriminatorValue());
                }
            }
        }

        return discrimValues;
    }
}