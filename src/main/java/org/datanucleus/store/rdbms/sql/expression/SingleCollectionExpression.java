package org.datanucleus.store.rdbms.sql.expression;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.SingleCollectionMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

public class SingleCollectionExpression extends DelegatedExpression {
	protected SQLExpression[] wrappedExpressions;

	public SingleCollectionExpression(SQLStatement stmt, SQLTable table,
			JavaTypeMapping mapping) {
		super(stmt, table, mapping);
		SingleCollectionMapping wcm = (SingleCollectionMapping) mapping;
		
		JavaTypeMapping wrappedMapping = wcm.getWrappedMapping();
		if ( wrappedMapping != null )
		{
            delegate = stmt.getSQLExpressionFactory().newExpression(stmt, table,
    				wrappedMapping);
		}
	}
}
