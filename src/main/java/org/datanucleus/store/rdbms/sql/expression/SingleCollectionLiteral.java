package org.datanucleus.store.rdbms.sql.expression;

import java.util.Iterator;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.types.ContainerAdapter;
import org.datanucleus.store.types.TypeManager;

public class SingleCollectionLiteral extends SingleCollectionExpression implements SQLLiteral
{
    private Object value;

    public SingleCollectionLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.value = value;

        TypeManager typeManager = mapping.getStoreManager().getNucleusContext().getTypeManager();

        ContainerAdapter containerAdapter = typeManager.getContainerAdapter(value);

        Iterator iterator = containerAdapter.iterator();
        if (iterator.hasNext()){
            Object wrappedValue = iterator.next();

            JavaTypeMapping m = stmt.getRDBMSManager().getSQLExpressionFactory().getMappingForType(wrappedValue.getClass(), false);

            delegate = stmt.getSQLExpressionFactory().newLiteral(stmt, m, wrappedValue);    
        }
        else
        {
            delegate =  new NullLiteral(stmt, null, null, null);
        }
    }

    @Override
    public Object getValue()
    {
        return value;
    }

    @Override
    public void setNotParameter()
    {
        throw new NucleusException("Not implemented yet.");
    }
}
