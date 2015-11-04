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
package org.datanucleus.store.rdbms.query;

import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchGroup;
import org.datanucleus.FetchGroupManager;
import org.datanucleus.FetchPlan;
import org.datanucleus.FetchPlanForClass;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MapMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.query.NullOrderingType;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.ArrayExpression;
import org.datanucleus.query.expression.CaseExpression;
import org.datanucleus.query.expression.CaseExpression.ExpressionPair;
import org.datanucleus.query.expression.ClassExpression;
import org.datanucleus.query.expression.CreatorExpression;
import org.datanucleus.query.expression.DyadicExpression;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Expression.Operator;
import org.datanucleus.query.expression.InvokeExpression;
import org.datanucleus.query.expression.JoinExpression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.SubqueryExpression;
import org.datanucleus.query.expression.VariableExpression;
import org.datanucleus.query.symbol.Symbol;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.AbstractContainerMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.DatastoreIdMapping;
import org.datanucleus.store.rdbms.mapping.java.MapMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.StringMapping;
import org.datanucleus.store.rdbms.mapping.java.TemporalMapping;
import org.datanucleus.store.query.QueryCompilerSyntaxException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLJoin;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.DeleteStatement;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLTableGroup;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.UpdateStatement;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanLiteral;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.CollectionLiteral;
import org.datanucleus.store.rdbms.sql.expression.ColumnExpression;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.IntegerLiteral;
import org.datanucleus.store.rdbms.sql.expression.MapExpression;
import org.datanucleus.store.rdbms.sql.expression.NewObjectExpression;
import org.datanucleus.store.rdbms.sql.expression.NullLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.NumericSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.ResultAliasExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.SQLLiteral;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.expression.StringSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.SubqueryExpressionComponent;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.store.rdbms.sql.expression.TemporalLiteral;
import org.datanucleus.store.rdbms.sql.expression.TemporalSubqueryExpression;
import org.datanucleus.store.rdbms.sql.expression.UnboundExpression;
import org.datanucleus.store.rdbms.table.ClassTable;
import org.datanucleus.store.rdbms.table.CollectionTable;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.ElementContainerTable;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.rdbms.table.MapTable;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Imports;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Class which maps a compiled (generic) query to an SQL query for RDBMS datastores.
 * Takes in an SQLStatement, and use of compile() will update the SQLStatement to reflect
 * the filter, result, grouping, having, ordering etc.
 * <p>
 * Supports the following extensions currently :-
 * <ul>
 * <li><b>datanucleus.query.jdoql.{varName}.join</b> defines the join type for that alias. This only currently
 * applies if the variable is (finally) bound using the equality operator (e.g var.field == this.field).
 * The extension should be set to "LEFTOUTERJOIN", "INNERJOIN"</li>
 * </ul>
 */
public class QueryToSQLMapper extends AbstractExpressionEvaluator implements QueryGenerator
{
    public static final String OPTION_CASE_INSENSITIVE = "CASE_INSENSITIVE";
    public static final String OPTION_EXPLICIT_JOINS = "EXPLICIT_JOINS";
    public static final String OPTION_BULK_UPDATE_VERSION = "BULK_UPDATE_VERSION";
    public static final String OPTION_SELECT_CANDIDATE_ID_ONLY = "RESULT_CANDIDATE_ID";
    public static final String OPTION_NULL_PARAM_USE_IS_NULL = "USE_IS_NULL_FOR_NULL_PARAM";

    public static final String MAP_VALUE_ALIAS_SUFFIX = "_VALUE";

    final String candidateAlias;

    final AbstractClassMetaData candidateCmd;

    final boolean subclasses;

    final QueryCompilation compilation;

    /** Input parameter values, keyed by the parameter name. Will be null if compiled pre-execution. */
    final Map parameters;

    /** Work Map for keying parameter value for the name, for case where parameters input as positional. */
    Map<String, Object> parameterValueByName = null;

    Map<Integer, String> paramNameByPosition = null;

    /** Positional parameter that we are up to (-1 implies not being used). */
    int positionalParamNumber = -1;

    /** Map of query extensions defining behaviour on the compilation. */
    Map<String, Object> extensionsByName = null;

    /** SQL statement that we are updating. */
    final SQLStatement stmt;

    /** Definition of mapping for the results of this SQL statement (candidate). */
    final StatementClassMapping resultDefinitionForClass;

    /** Definition of mapping for the results of this SQL statement (result). */
    final StatementResultMapping resultDefinition;

    Map<Object, SQLExpression> expressionForParameter;

    final RDBMSStoreManager storeMgr;

    final FetchPlan fetchPlan;

    final SQLExpressionFactory exprFactory;

    ExecutionContext ec;

    ClassLoaderResolver clr;

    Imports importsDefinition = null;

    Map<String, Object> compileProperties = new HashMap();

    /** State variable for the component being compiled. */
    CompilationComponent compileComponent;

    /** Stack of expressions, used for compilation of the query into SQL. */
    Deque<SQLExpression> stack = new ArrayDeque<SQLExpression>();

    /** Map of SQLTable/mapping keyed by the name of the primary that it relates to. */
    Map<String, SQLTableMapping> sqlTableByPrimary = new HashMap<String, SQLTableMapping>();

    /** Aliases defined in the result, populated during compileResult. */
    Set<String> resultAliases = null;

    /** Map of the join primary expression string keyed by the join alias (explicit joins only). */
    Map<String, String> explicitJoinPrimaryByAlias = null;

    Map<String, JavaTypeMapping> paramMappingForName = new HashMap();

    /** Options for the SQL generation process. See OPTION_xxx above. */
    Set<String> options = new HashSet<String>();

    /** Parent mapper if we are processing a subquery. */
    public QueryToSQLMapper parentMapper = null;

    org.datanucleus.store.rdbms.sql.SQLJoin.JoinType defaultJoinType = null;

    /**
     * State variable for whether this query is precompilable (hence whether it is cacheable).
     * Or in other words, whether we can compile it without knowing parameter values.
     */
    boolean precompilable = true;

    static class SQLTableMapping
    {
        SQLTable table;
        AbstractClassMetaData cmd;
        JavaTypeMapping mapping;

        AbstractMemberMetaData mmd;

        public SQLTableMapping(SQLTable tbl, AbstractClassMetaData cmd, JavaTypeMapping m)
        {
            this.table = tbl;
            this.cmd = cmd;
            this.mmd = null;
            this.mapping = m;
        }

        /**
         * Constructor for use when we have a Map field.
         */
        public SQLTableMapping(SQLTable tbl, AbstractMemberMetaData mmd, JavaTypeMapping m)
        {
            this.table = tbl;
            this.cmd = mmd.getAbstractClassMetaData();
            this.mmd = mmd;
            this.mapping = m;
        }

        public String toString()
        {
            if (mmd != null)
            {
                return "SQLTableMapping: tbl=" + table + " member=" + mmd.getFullFieldName() + " mapping=" + mapping;
            }
            return "SQLTableMapping: tbl=" + table + " class=" + (cmd != null ? cmd.getFullClassName() : "null") + " mapping=" + mapping;
        }
    }

    /**
     * Constructor.
     * @param stmt SQL Statement to update with the query contents.
     * @param compilation The generic query compilation
     * @param parameters Parameters needed
     * @param resultDefForClass Definition of results for the statement (populated here)
     * @param resultDef Definition of results if we have a result clause (populated here)
     * @param cmd Metadata for the candidate class
     * @param subclasses Whether to include subclasses
     * @param fetchPlan Fetch Plan to apply
     * @param ec execution context
     * @param importsDefinition Import definition to use in class lookups (optional)
     * @param options Options controlling behaviour
     * @param extensions Any query extensions that may define compilation behaviour
     */
    public QueryToSQLMapper(SQLStatement stmt, QueryCompilation compilation, Map parameters,
            StatementClassMapping resultDefForClass, StatementResultMapping resultDef,
            AbstractClassMetaData cmd, boolean subclasses, FetchPlan fetchPlan, ExecutionContext ec, Imports importsDefinition,
            Set<String> options, Map<String, Object> extensions)
    {
        this.parameters = parameters;
        this.compilation = compilation;
        this.stmt = stmt;
        this.resultDefinitionForClass = resultDefForClass;
        this.resultDefinition = resultDef;
        this.candidateCmd = cmd;
        this.candidateAlias = compilation.getCandidateAlias();
        this.subclasses = subclasses;
        this.fetchPlan = fetchPlan;
        this.storeMgr = stmt.getRDBMSManager();
        this.exprFactory = stmt.getRDBMSManager().getSQLExpressionFactory();
        this.ec = ec;
        this.clr = ec.getClassLoaderResolver();
        this.importsDefinition = importsDefinition;
        if (options != null)
        {
            this.options.addAll(options);
        }
        this.extensionsByName = extensions;

        this.stmt.setQueryGenerator(this);

        // Register the candidate
        SQLTableMapping tblMapping = new SQLTableMapping(stmt.getPrimaryTable(), candidateCmd, stmt.getPrimaryTable().getTable().getIdMapping());
        setSQLTableMappingForAlias(candidateAlias, tblMapping);
    }

    /**
     * Method to set the default join type to be used.
     * By default we use the nullability of the field to define whether to use LEFT OUTER, or INNER.
     * In JPQL if a relation isn't specified explicitly in the FROM clause then it should use INNER
     * hence where this method will be used, to define the default.
     * @param joinType The default join type
     */
    void setDefaultJoinType(org.datanucleus.store.rdbms.sql.SQLJoin.JoinType joinType)
    {
        this.defaultJoinType = joinType;
    }

    void setParentMapper(QueryToSQLMapper parent)
    {
        this.parentMapper = parent;
    }

    /**
     * Accessor for the query language that this query pertains to.
     * Can be used to decide how to handle the input.
     * @return The query language
     */
    public String getQueryLanguage()
    {
        return compilation.getQueryLanguage();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.query.QueryGenerator#getClassLoaderResolver()
     */
    public ClassLoaderResolver getClassLoaderResolver()
    {
        return clr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.query.QueryGenerator#getCompilationComponent()
     */
    public CompilationComponent getCompilationComponent()
    {
        return compileComponent;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.query.QueryGenerator#getExecutionContext()
     */
    public ExecutionContext getExecutionContext()
    {
        return ec;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.query.QueryGenerator#getProperty(java.lang.String)
     */
    public Object getProperty(String name)
    {
        return compileProperties.get(name);
    }

    /**
     * Accessor for whether the query is precompilable (doesn't need to know parameter values
     * to be able to compile it).
     * @return Whether the query is precompilable and hence cacheable
     */
    public boolean isPrecompilable()
    {
        return precompilable;
    }

    protected void setNotPrecompilable()
    {
        if (parentMapper != null)
        {
            parentMapper.setNotPrecompilable();
        }
        this.precompilable = false;
    }

    /**
     * Convenience accessor for a map of the parameter name keyed by its position.
     * This is only available after <pre>compile</pre> is called.
     * @return Map of parameter name keyed by position
     */
    public Map<Integer, String> getParameterNameByPosition()
    {
        return paramNameByPosition;
    }

    /**
     * Method to update the supplied SQLStatement with the components of the specified query.
     * During the compilation process this updates the SQLStatement "compileComponent" to the
     * component of the query being compiled.
     */
    public void compile()
    {
        compileFrom();
        compileFilter();

        if (stmt instanceof UpdateStatement)
        {
            compileUpdate((UpdateStatement)stmt);
        }
        else if (stmt instanceof DeleteStatement)
        {
            
        }
        else if (stmt instanceof SelectStatement)
        {
            SelectStatement selectStmt = (SelectStatement)stmt;

            // Set whether the statement returns distinct. Do this before processing the result in case 
            // the datastore doesn't allow select of some field types when used with DISTINCT
            if (compilation.getResultDistinct())
            {
                selectStmt.setDistinct(true);
            }
            else if (!options.contains(OPTION_EXPLICIT_JOINS) && compilation.getExprResult() == null)
            {
                // Joins are made implicitly and no result so set distinct based on whether joining to other table groups
                if (selectStmt.getNumberOfTableGroups() > 1)
                {
                    // Queries against an extent always consider only distinct candidate instances, regardless of
                    // whether distinct is specified (JDO spec)
                    selectStmt.setDistinct(true);
                }
            }

            compileResult(selectStmt);
            compileGrouping(selectStmt);
            compileHaving(selectStmt);
            compileOrdering(selectStmt);
        }

        // Check for variables that haven't been bound to the query (declared but not used)
        Collection<String> symbols = compilation.getSymbolTable().getSymbolNames();
        Iterator<String> symIter = symbols.iterator();
        while (symIter.hasNext())
        {
            Symbol sym = compilation.getSymbolTable().getSymbol(symIter.next());
            if (sym.getType() == Symbol.VARIABLE)
            {
                if (compilation.getCompilationForSubquery(sym.getQualifiedName()) == null && !hasSQLTableMappingForAlias(sym.getQualifiedName()))
                {
                    // Variable not a subquery, nor had its table allocated
                    throw new QueryCompilerSyntaxException("Query has variable \"" + sym.getQualifiedName() + "\" which is not bound to the query");
                }
            }
        }
    }

    /**
     * Method to compile the FROM clause of the query into the SQLStatement.
     */
    protected void compileFrom()
    {
        if (compilation.getExprFrom() != null)
        {
            // Process all ClassExpression(s) in the FROM, adding joins to the statement as required
            compileComponent = CompilationComponent.FROM;
            Expression[] fromExprs = compilation.getExprFrom();
            for (int i=0;i<fromExprs.length;i++)
            {
                ClassExpression clsExpr = (ClassExpression)fromExprs[i];
                compileFromClassExpression(clsExpr);
            }
            compileComponent = null;
        }
    }

    /**
     * Method to compile the WHERE clause of the query into the SQLStatement.
     */
    protected void compileFilter()
    {
        if (compilation.getExprFilter() != null)
        {
            // Apply the filter to the SQLStatement
            compileComponent = CompilationComponent.FILTER;
            if (QueryUtils.expressionHasOrOperator(compilation.getExprFilter()))
            {
                compileProperties.put("Filter.OR", true);
            }
            if (QueryUtils.expressionHasNotOperator(compilation.getExprFilter()))
            {
                // Really we want to know if there is a NOT contains, but just check NOT for now
                compileProperties.put("Filter.NOT", true);
            }

            SQLExpression filterSqlExpr = (SQLExpression) compilation.getExprFilter().evaluate(this);
            if (!(filterSqlExpr instanceof BooleanExpression))
            {
                throw new QueryCompilerSyntaxException("Filter compiles to something that is not a boolean expression. Kindly fix your query : " + filterSqlExpr);
            }
            BooleanExpression filterExpr = (BooleanExpression)filterSqlExpr;
            filterExpr = getBooleanExpressionForUseInFilter(filterExpr);
            stmt.whereAnd(filterExpr, true);
            compileComponent = null;
        }
    }

    /**
     * Method to compile the result clause of the query into the SQLStatement.
     * Note that this also compiles queries of the candidate (no specified result), selecting the candidate field(s).
     * @param stmt SELECT statement
     */
    protected void compileResult(SelectStatement stmt)
    {
        // Select Statement : select any specified result, or the candidate
        if (compilation.getExprResult() != null)
        {
            compileComponent = CompilationComponent.RESULT;

            // Select any result expressions
            Expression[] resultExprs = compilation.getExprResult();
            for (int i=0;i<resultExprs.length;i++)
            {
                String alias = resultExprs[i].getAlias();
                if (alias != null && resultAliases == null)
                {
                    resultAliases = new HashSet<String>();
                }

                if (resultExprs[i] instanceof InvokeExpression)
                {
                    InvokeExpression invokeExpr = (InvokeExpression)resultExprs[i];
                    processInvokeExpression((InvokeExpression)resultExprs[i]);
                    SQLExpression sqlExpr = stack.pop();
                    validateExpressionForResult(sqlExpr);

                    String invokeMethod = invokeExpr.getOperation();
                    if ((invokeMethod.equalsIgnoreCase("mapKey") || invokeMethod.equalsIgnoreCase("mapValue")) && alias == null)
                    {
                        if (sqlExpr.getJavaTypeMapping() instanceof PersistableMapping)
                        {
                            // Method returns persistable object, so select the FetchPlan
                            String selectedType = ((PersistableMapping)sqlExpr.getJavaTypeMapping()).getType();
                            AbstractClassMetaData selectedCmd = ec.getMetaDataManager().getMetaDataForClass(selectedType, clr);
                            FetchPlanForClass fpForCmd = fetchPlan.getFetchPlanForClass(selectedCmd);
                            int[] membersToSelect = fpForCmd.getMemberNumbers();
                            ClassTable selectedTable = (ClassTable) sqlExpr.getSQLTable().getTable();
                            StatementClassMapping map = new StatementClassMapping(selectedCmd.getFullClassName(), null);

                            if (selectedCmd.getIdentityType() == IdentityType.DATASTORE)
                            {
                                int[] cols = stmt.select(sqlExpr.getSQLTable(), selectedTable.getDatastoreIdMapping(), alias);
                                StatementMappingIndex idx = new StatementMappingIndex(selectedTable.getDatastoreIdMapping());
                                idx.setColumnPositions(cols);
                                map.addMappingForMember(StatementClassMapping.MEMBER_DATASTORE_ID, idx);
                            }

                            // Select the FetchPlan members
                            for (int j=0;j<membersToSelect.length;j++)
                            {
                                AbstractMemberMetaData selMmd = selectedCmd.getMetaDataForManagedMemberAtAbsolutePosition(j);
                                SQLStatementHelper.selectMemberOfSourceInStatement(stmt, map, fetchPlan, sqlExpr.getSQLTable(), selMmd, clr, 1, null); // TODO Arbitrary penultimate argument
                            }

                            resultDefinition.addMappingForResultExpression(i, map);
                            continue;
                        }
                        else if (sqlExpr.getJavaTypeMapping() instanceof EmbeddedMapping)
                        {
                            // Method returns embedded object, so select the FetchPlan
                            EmbeddedMapping embMapping = (EmbeddedMapping)sqlExpr.getJavaTypeMapping();
                            String selectedType = embMapping.getType();
                            AbstractClassMetaData selectedCmd = ec.getMetaDataManager().getMetaDataForClass(selectedType, clr);
                            FetchPlanForClass fpForCmd = fetchPlan.getFetchPlanForClass(selectedCmd);
                            int[] membersToSelect = fpForCmd.getMemberNumbers();
                            StatementClassMapping map = new StatementClassMapping(selectedCmd.getFullClassName(), null);

                            // Select the FetchPlan members
                            for (int j=0;j<membersToSelect.length;j++)
                            {
                                AbstractMemberMetaData selMmd = selectedCmd.getMetaDataForManagedMemberAtAbsolutePosition(j);
                                JavaTypeMapping selMapping = embMapping.getJavaTypeMapping(selMmd.getName());
                                if (selMapping.includeInFetchStatement())
                                {
                                    int[] cols = stmt.select(sqlExpr.getSQLTable(), selMapping, alias);
                                    StatementMappingIndex idx = new StatementMappingIndex(selMapping);
                                    idx.setColumnPositions(cols);
                                    map.addMappingForMember(membersToSelect[j], idx);
                                }
                            }

                            resultDefinition.addMappingForResultExpression(i, map);
                            continue;
                        }
                    }

                    StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                    int[] cols = stmt.select(sqlExpr, alias);
                    idx.setColumnPositions(cols);
                    if (alias != null)
                    {
                        resultAliases.add(alias);
                        idx.setColumnAlias(alias);
                    }
                    resultDefinition.addMappingForResultExpression(i, idx);
                }
                else if (resultExprs[i] instanceof PrimaryExpression)
                {
                    PrimaryExpression primExpr = (PrimaryExpression)resultExprs[i];
                    if (primExpr.getId().equals(candidateAlias))
                    {
                        // "this", so select fetch plan fields
                        StatementClassMapping map = new StatementClassMapping(candidateCmd.getFullClassName(), null);
                        SQLStatementHelper.selectFetchPlanOfCandidateInStatement(stmt, map, candidateCmd, fetchPlan, 1);
                        resultDefinition.addMappingForResultExpression(i, map);
                    }
                    else
                    {
                        processPrimaryExpression(primExpr);
                        SQLExpression sqlExpr = stack.pop();
                        validateExpressionForResult(sqlExpr);
                        if (sqlExpr instanceof SQLLiteral)
                        {
                            int[] cols = stmt.select(sqlExpr, alias);
                            StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                            idx.setColumnPositions(cols);
                            if (alias != null)
                            {
                                resultAliases.add(alias);
                                idx.setColumnAlias(alias);
                            }
                            resultDefinition.addMappingForResultExpression(i, idx);
                        }
                        else
                        {
                            int[] cols = stmt.select(sqlExpr.getSQLTable(), sqlExpr.getJavaTypeMapping(), alias);
                            StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                            idx.setColumnPositions(cols);
                            if (alias != null)
                            {
                                resultAliases.add(alias);
                                idx.setColumnAlias(alias);
                            }
                            resultDefinition.addMappingForResultExpression(i, idx);
                        }
                    }
                }
                else if (resultExprs[i] instanceof ParameterExpression)
                {
                    processParameterExpression((ParameterExpression)resultExprs[i], true); // Params are literals in result
                    SQLExpression sqlExpr = stack.pop();
                    validateExpressionForResult(sqlExpr);
                    int[] cols = stmt.select(sqlExpr, alias);
                    StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                    idx.setColumnPositions(cols);
                    if (alias != null)
                    {
                        resultAliases.add(alias);
                        idx.setColumnAlias(alias);
                    }
                    resultDefinition.addMappingForResultExpression(i, idx);
                }
                else if (resultExprs[i] instanceof VariableExpression)
                {
                    processVariableExpression((VariableExpression)resultExprs[i]);
                    SQLExpression sqlExpr = stack.pop();
                    validateExpressionForResult(sqlExpr);
                    if (sqlExpr instanceof UnboundExpression)
                    {
                        // Variable wasn't bound in the compilation so far, so handle as cross-join
                        processUnboundExpression((UnboundExpression) sqlExpr);
                        sqlExpr = stack.pop();
                        NucleusLogger.QUERY.debug("QueryToSQL.exprResult variable was still unbound, so binding via cross-join");
                    }
                    StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                    int[] cols = stmt.select(sqlExpr, alias);
                    idx.setColumnPositions(cols);
                    if (alias != null)
                    {
                        resultAliases.add(alias);
                        idx.setColumnAlias(alias);
                    }
                    resultDefinition.addMappingForResultExpression(i, idx);
                }
                else if (resultExprs[i] instanceof Literal)
                {
                    processLiteral((Literal)resultExprs[i]);
                    SQLExpression sqlExpr = stack.pop();
                    validateExpressionForResult(sqlExpr);
                    int[] cols = stmt.select(sqlExpr, alias);
                    StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                    idx.setColumnPositions(cols);
                    if (alias != null)
                    {
                        resultAliases.add(alias);
                        idx.setColumnAlias(alias);
                    }
                    resultDefinition.addMappingForResultExpression(i, idx);
                }
                else if (resultExprs[i] instanceof CreatorExpression)
                {
                    processCreatorExpression((CreatorExpression)resultExprs[i]);
                    NewObjectExpression sqlExpr = (NewObjectExpression)stack.pop();
                    StatementNewObjectMapping stmtMap = getStatementMappingForNewObjectExpression(sqlExpr, stmt);
                    resultDefinition.addMappingForResultExpression(i, stmtMap);
                }
                else if (resultExprs[i] instanceof DyadicExpression)
                {
                    // Something like {a+b} perhaps. Maybe we should check for invalid expressions?
                    resultExprs[i].evaluate(this);
                    SQLExpression sqlExpr = stack.pop();
                    int[] cols = stmt.select(sqlExpr, alias);
                    StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                    idx.setColumnPositions(cols);
                    if (alias != null)
                    {
                        resultAliases.add(alias);
                        idx.setColumnAlias(alias);
                    }
                    resultDefinition.addMappingForResultExpression(i, idx);
                }
                else if (resultExprs[i] instanceof CaseExpression)
                {
                    resultExprs[i].evaluate(this);
                    SQLExpression sqlExpr = stack.pop();
                    int[] cols = stmt.select(sqlExpr, alias);
                    StatementMappingIndex idx = new StatementMappingIndex(sqlExpr.getJavaTypeMapping());
                    idx.setColumnPositions(cols);
                    if (alias != null)
                    {
                        resultAliases.add(alias);
                        idx.setColumnAlias(alias);
                    }
                    resultDefinition.addMappingForResultExpression(i, idx);
                }
                else
                {
                    throw new NucleusException("Dont currently support result clause containing expression of type " + resultExprs[i]);
                }
            }

            if (stmt.getNumberOfSelects() == 0)
            {
                // Nothing selected so likely the user had some "new MyClass()" expression, so select "1"
                stmt.select(exprFactory.newLiteral(stmt, storeMgr.getMappingManager().getMapping(Integer.class), 1), null);
            }
        }
        else
        {
            // Select of the candidate (no result)
            compileComponent = CompilationComponent.RESULT;
            if (candidateCmd.getIdentityType() == IdentityType.NONDURABLE)
            {
                // Nondurable identity cases have no "id" for later fetching so get all fields now
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("052520", candidateCmd.getFullClassName()));
                }
                fetchPlan.setGroup("all");
            }

            if (subclasses)
            {
                // TODO Check for special case of candidate+subclasses stores in same table with discriminator, so select FetchGroup of subclasses also
            }

            int maxFetchDepth = fetchPlan.getMaxFetchDepth();
            if (maxFetchDepth < 0)
            {
                maxFetchDepth = 3; // TODO Arbitrary
            }

            if (options.contains(OPTION_SELECT_CANDIDATE_ID_ONLY))
            {
                SQLStatementHelper.selectIdentityOfCandidateInStatement(stmt, resultDefinitionForClass, candidateCmd);
            }
            else if (parentMapper != null && resultDefinitionForClass == null)
            {
                // Subquery, with no result specified, so select id only
                SQLStatementHelper.selectIdentityOfCandidateInStatement(stmt, resultDefinitionForClass, candidateCmd);
            }
            else if (stmt.allUnionsForSamePrimaryTable())
            {
                // Select fetch-plan members of the candidate (and optionally the next level of sub-objects)
                // Don't select next level when we are processing a subquery
                SQLStatementHelper.selectFetchPlanOfCandidateInStatement(stmt, resultDefinitionForClass, candidateCmd, fetchPlan, parentMapper == null ? maxFetchDepth : 0);
            }
            else if (candidateCmd.getInheritanceMetaData() != null && candidateCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
            {
                // complete-table should have all fields of superclass present in all unions, so try to select fetch plan
                SQLStatementHelper.selectFetchPlanOfCandidateInStatement(stmt, resultDefinitionForClass, candidateCmd, fetchPlan, parentMapper == null ? maxFetchDepth : 0);
            }
            else
            {
                // Select identity of the candidates since use different base tables
                SQLStatementHelper.selectIdentityOfCandidateInStatement(stmt, resultDefinitionForClass, candidateCmd);
            }
        }
        compileComponent = null;
    }

    /**
     * Method to compile the result clause of the query into the SQLStatement.
     * @param stmt UPDATE statement
     */
    protected void compileUpdate(UpdateStatement stmt)
    {
        if (compilation.getExprUpdate() != null)
        {
            // Update statement, so generate update expression(s)
            compileComponent = CompilationComponent.UPDATE;
            Expression[] updateExprs = compilation.getExprUpdate();
            SQLExpression[] updateSqlExprs = new SQLExpression[updateExprs.length];
            // TODO If the field being set is in a different table omit it
            boolean performingUpdate = false;
            for (int i=0;i<updateExprs.length;i++)
            {
                // "field = value"
                DyadicExpression updateExpr = (DyadicExpression)updateExprs[i];

                // Left-side has to be PrimaryExpression
                SQLExpression leftSqlExpr = null;
                if (updateExpr.getLeft() instanceof PrimaryExpression)
                {
                    processPrimaryExpression((PrimaryExpression)updateExpr.getLeft());
                    leftSqlExpr = stack.pop();
                    if (leftSqlExpr.getSQLTable() != stmt.getPrimaryTable())
                    {
                        // Set left to null to signify that it is not applicable to the table of this UPDATE statement
                        leftSqlExpr = null;
                    }
                }
                else
                {
                    throw new NucleusException("Dont currently support update clause containing left expression of type " + updateExpr.getLeft());
                }

                if (leftSqlExpr != null)
                {
                    if (!stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.UPDATE_STATEMENT_ALLOW_TABLE_ALIAS_IN_SET_CLAUSE))
                    {
                        // This datastore doesn't allow table alias in UPDATE SET clause, so just use column name
                        for (int j=0;j<leftSqlExpr.getNumberOfSubExpressions();j++)
                        {
                            ColumnExpression colExpr = leftSqlExpr.getSubExpression(j);
                            colExpr.setOmitTableFromString(true);
                        }
                    }
                    performingUpdate = true;

                    // Right-side can be Literal, or Parameter, or PrimaryExpression
                    SQLExpression rightSqlExpr = null;
                    if (updateExpr.getRight() instanceof Literal)
                    {
                        processLiteral((Literal)updateExpr.getRight());
                        rightSqlExpr = stack.pop();
                    }
                    else if (updateExpr.getRight() instanceof ParameterExpression)
                    {
                        ParameterExpression paramExpr = (ParameterExpression)updateExpr.getRight();
                        paramMappingForName.put(paramExpr.getId(), leftSqlExpr.getJavaTypeMapping());
                        processParameterExpression(paramExpr);
                        rightSqlExpr = stack.pop();
                    }
                    else if (updateExpr.getRight() instanceof PrimaryExpression)
                    {
                        processPrimaryExpression((PrimaryExpression)updateExpr.getRight());
                        rightSqlExpr = stack.pop();
                    }
                    else if (updateExpr.getRight() instanceof DyadicExpression)
                    {
                        updateExpr.getRight().evaluate(this);
                        rightSqlExpr = stack.pop();
                    }
                    else if (updateExpr.getRight() instanceof CaseExpression)
                    {
                        updateExpr.getRight().evaluate(this);
                        rightSqlExpr = stack.pop();
                    }
                    else
                    {
                        throw new NucleusException("Dont currently support update clause containing right expression of type " + updateExpr.getRight());
                    }

                    if (rightSqlExpr != null)
                    {
                        updateSqlExprs[i] = leftSqlExpr.eq(rightSqlExpr);
                    }
                }
            }

            if (candidateCmd.isVersioned() && options.contains(OPTION_BULK_UPDATE_VERSION))
            {
                SQLExpression updateSqlExpr = null;

                ClassTable table = (ClassTable)stmt.getPrimaryTable().getTable();
                JavaTypeMapping verMapping = table.getVersionMapping(true);
                ClassTable verTable = table.getTableManagingMapping(verMapping);
                if (verTable == stmt.getPrimaryTable().getTable())
                {
                    VersionMetaData vermd = candidateCmd.getVersionMetaDataForClass();
                    if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                    {
                        // Increment the version
                        SQLTable verSqlTbl = stmt.getTable(verTable, stmt.getPrimaryTable().getGroupName());
                        SQLExpression verExpr = new NumericExpression(stmt, verSqlTbl, verMapping);
                        SQLExpression incrExpr = verExpr.add(new IntegerLiteral(stmt, 
                            stmt.getSQLExpressionFactory().getMappingForType(Integer.class, false), Integer.valueOf(1), null));

                        updateSqlExpr = verExpr.eq(incrExpr);

                        SQLExpression[] oldArray = updateSqlExprs;
                        updateSqlExprs = new SQLExpression[oldArray.length+1];
                        System.arraycopy(oldArray, 0, updateSqlExprs, 0, oldArray.length);
                        updateSqlExprs[oldArray.length] = updateSqlExpr;
                        performingUpdate = true;
                    }
                    else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                    {
                        // Set version to the time of update
                        SQLTable verSqlTbl = stmt.getTable(verTable, stmt.getPrimaryTable().getGroupName());
                        SQLExpression verExpr = new NumericExpression(stmt, verSqlTbl, verMapping);
                        Object newVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), null);
                        JavaTypeMapping valMapping = stmt.getSQLExpressionFactory().getMappingForType(newVersion.getClass(), false);
                        SQLExpression valExpr = new TemporalLiteral(stmt, valMapping, newVersion, null);

                        updateSqlExpr = verExpr.eq(valExpr);

                        SQLExpression[] oldArray = updateSqlExprs;
                        updateSqlExprs = new SQLExpression[oldArray.length+1];
                        System.arraycopy(oldArray, 0, updateSqlExprs, 0, oldArray.length);
                        updateSqlExprs[oldArray.length] = updateSqlExpr;
                        performingUpdate = true;
                    }
                }
            }

            if (performingUpdate)
            {
                // Only set the updates component of the SQLStatement if anything to update in this table
                stmt.setUpdates(updateSqlExprs);
            }
        }
        compileComponent = null;
    }

    /**
     * Method that validates that the specified expression is valid for use in a result clause.
     * Throws a NucleusUserException if it finds a multi-value mapping selected (collection, map).
     * @param sqlExpr The SQLExpression
     */
    protected void validateExpressionForResult(SQLExpression sqlExpr)
    {
        JavaTypeMapping m = sqlExpr.getJavaTypeMapping();
        if (m != null && m instanceof AbstractContainerMapping && m.getNumberOfDatastoreMappings() != 1)
        {
            throw new NucleusUserException(Localiser.msg("021213"));
        }
    }

    /**
     * Method to compile the grouping clause of the query into the SQLStatement.
     * @param stmt SELECT statement
     */
    protected void compileGrouping(SelectStatement stmt)
    {
        if (compilation.getExprGrouping() != null)
        {
            // Apply any grouping to the statement
            compileComponent = CompilationComponent.GROUPING;
            Expression[] groupExprs = compilation.getExprGrouping();
            for (int i = 0; i < groupExprs.length; i++)
            {
                Expression groupExpr = groupExprs[i];
                SQLExpression sqlGroupExpr = (SQLExpression)groupExpr.evaluate(this);
                stmt.addGroupingExpression(sqlGroupExpr);
            }
            compileComponent = null;
        }
    }

    /**
     * Method to compile the having clause of the query into the SQLStatement.
     * @param stmt SELECT statement
     */
    protected void compileHaving(SelectStatement stmt)
    {
        if (compilation.getExprHaving() != null)
        {
            // Apply any having to the statement
            compileComponent = CompilationComponent.HAVING;
            Expression havingExpr = compilation.getExprHaving();
            Object havingEval = havingExpr.evaluate(this);
            if (!(havingEval instanceof BooleanExpression))
            {
                // Non-boolean having clause should be user exception
                throw new NucleusUserException(Localiser.msg("021051", havingExpr));
            }
            stmt.setHaving((BooleanExpression)havingEval);
            compileComponent = null;
        }
    }

    /**
     * Method to compile the ordering clause of the query into the SQLStatement.
     * @param stmt SELECT statement
     */
    protected void compileOrdering(SelectStatement stmt)
    {
        if (compilation.getExprOrdering() != null)
        {
            compileComponent = CompilationComponent.ORDERING;
            Expression[] orderingExpr = compilation.getExprOrdering();
            SQLExpression[] orderSqlExprs = new SQLExpression[orderingExpr.length];
            boolean[] directions = new boolean[orderingExpr.length];
            NullOrderingType[] nullOrders = new NullOrderingType[orderingExpr.length];
            for (int i = 0; i < orderingExpr.length; i++)
            {
                OrderExpression orderExpr = (OrderExpression)orderingExpr[i];
                Expression expr = orderExpr.getLeft();
                if (expr instanceof PrimaryExpression)
                {
                    PrimaryExpression orderPrimExpr = (PrimaryExpression)expr;
                    if (orderPrimExpr.getTuples().size() == 1 && resultAliases != null)
                    {
                        if (resultAliases.contains(orderPrimExpr.getId().toLowerCase()))
                        {
                            // Order by a result alias
                            orderSqlExprs[i] = new ResultAliasExpression(stmt, orderPrimExpr.getId());
                        }
                    }
                }

                if (orderSqlExprs[i] == null)
                {
                    orderSqlExprs[i] = (SQLExpression)orderExpr.getLeft().evaluate(this);
                }

                String orderDir = orderExpr.getSortOrder();
                directions[i] = ((orderDir == null || orderDir.equals("ascending")) ? false : true);
                nullOrders[i] = orderExpr.getNullOrder();
            }
            stmt.setOrdering(orderSqlExprs, directions, nullOrders);
            compileComponent = null;
        }
    }

    /**
     * Method to take a ClassExpression (in a FROM clause) and process the candidate and any
     * linked JoinExpression(s), adding joins to the SQLStatement as required.
     * @param clsExpr The ClassExpression
     */
    protected void compileFromClassExpression(ClassExpression clsExpr)
    {
        Symbol clsExprSym = clsExpr.getSymbol();
        Class baseCls = (clsExprSym != null ? clsExprSym.getValueType() : null);
        SQLTable candSqlTbl = stmt.getPrimaryTable();
        MetaDataManager mmgr = storeMgr.getMetaDataManager();
        AbstractClassMetaData cmd = mmgr.getMetaDataForClass(baseCls, clr);
        if (baseCls != null && baseCls != compilation.getCandidateClass())
        {
            // Not candidate class so must be cross join (JPA spec 4.4.5)
            DatastoreClass candTbl = storeMgr.getDatastoreClass(baseCls.getName(), clr);
            candSqlTbl = stmt.crossJoin(candTbl, clsExpr.getAlias(), null);
            SQLTableMapping tblMapping = new SQLTableMapping(candSqlTbl, cmd, candTbl.getIdMapping());
            setSQLTableMappingForAlias(clsExpr.getAlias(), tblMapping);
        }

        if (clsExpr.getCandidateExpression() != null && parentMapper != null)
        {
            // User defined the candidate of the subquery as an implied join to the outer query
            // e.g SELECT c FROM Customer c WHERE EXISTS (SELECT o FROM c.orders o ...)
            // so add the join(s) to the outer query
            String[] tokens = StringUtils.split(clsExpr.getCandidateExpression(), ".");

            String leftAlias = tokens[0];
            SQLTableMapping outerSqlTblMapping = parentMapper.getSQLTableMappingForAlias(leftAlias);
            AbstractClassMetaData leftCmd = outerSqlTblMapping.cmd;

            // Get array of the left-right sides of this expression so we can work back from the subquery candidate
            AbstractMemberMetaData[] leftMmds = new AbstractMemberMetaData[tokens.length-1];
            AbstractMemberMetaData[] rightMmds = new AbstractMemberMetaData[tokens.length-1];
            for (int i=0;i<tokens.length-1;i++)
            {
                String joinedField = tokens[i+1];

                AbstractMemberMetaData leftMmd = leftCmd.getMetaDataForMember(joinedField);
                AbstractMemberMetaData rightMmd = null;
                AbstractClassMetaData rightCmd = null;
                RelationType relationType = leftMmd.getRelationType(clr);

                if (RelationType.isBidirectional(relationType))
                {
                    rightMmd = leftMmd.getRelatedMemberMetaData(clr)[0]; // Take first possible
                    rightCmd = rightMmd.getAbstractClassMetaData();
                }
                else if (relationType == RelationType.ONE_TO_ONE_UNI)
                {
                    rightCmd = mmgr.getMetaDataForClass(leftMmd.getType(), clr);
                }
                else if (relationType == RelationType.ONE_TO_MANY_UNI)
                {
                    if (leftMmd.hasCollection())
                    {
                        rightCmd = mmgr.getMetaDataForClass(leftMmd.getCollection().getElementType(), clr);
                    }
                    else if (leftMmd.hasMap())
                    {
                        rightCmd = mmgr.getMetaDataForClass(leftMmd.getMap().getValueType(), clr);
                    }
                }
                else
                {
                    throw new NucleusUserException("Subquery has been specified with a candidate-expression that includes \"" + tokens[i] + "\" that isnt a relation field!!");
                }

                leftMmds[i] = leftMmd;
                rightMmds[i] = rightMmd;
                leftCmd = rightCmd;
            }

            // Work from subquery candidate back to outer query table, adding joins and where clause as appropriate
            SQLTable rSqlTbl = candSqlTbl;
            SQLTable outerSqlTbl = outerSqlTblMapping.table;
            for (int i=leftMmds.length-1;i>=0;i--)
            {
                AbstractMemberMetaData leftMmd = leftMmds[i];
                AbstractMemberMetaData rightMmd = rightMmds[i];
                DatastoreClass leftTbl = storeMgr.getDatastoreClass(leftMmd.getClassName(true), clr);
                SQLTable lSqlTbl = null;
                RelationType relationType = leftMmd.getRelationType(clr);

                if (relationType == RelationType.ONE_TO_ONE_UNI)
                {
                    // 1-1 with FK in left table
                    if (i == 0)
                    {
                        // Add where clause right table to outer table
                        SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                        SQLExpression rightExpr = exprFactory.newExpression(stmt, rSqlTbl, rSqlTbl.getTable().getIdMapping());
                        stmt.whereAnd(outerExpr.eq(rightExpr), false);
                    }
                    else
                    {
                        // Join to left table
                        JavaTypeMapping leftMapping = leftTbl.getMemberMapping(leftMmd);
                        lSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), leftTbl, null, leftMapping, null, null, true);
                    }
                }
                else if (relationType == RelationType.ONE_TO_ONE_BI)
                {
                    if (leftMmd.getMappedBy() != null)
                    {
                        // 1-1 with FK in right table
                        JavaTypeMapping rightMapping = rSqlTbl.getTable().getMemberMapping(rightMmd);
                        if (i == 0)
                        {
                            // Add where clause right table to outer table
                            SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getIdMapping());
                            SQLExpression rightExpr = exprFactory.newExpression(stmt, rSqlTbl, rightMapping);
                            stmt.whereAnd(outerExpr.eq(rightExpr), false);
                        }
                        else
                        {
                            // Join to left table
                            lSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rightMapping, leftTbl, null, leftTbl.getIdMapping(), null, null, true);
                        }
                    }
                    else
                    {
                        // 1-1 with FK in left table
                        if (i == 0)
                        {
                            // Add where clause right table to outer table
                            SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                            SQLExpression rightExpr = exprFactory.newExpression(stmt, rSqlTbl, rSqlTbl.getTable().getIdMapping());
                            stmt.whereAnd(outerExpr.eq(rightExpr), false);
                        }
                        else
                        {
                            // Join to left table
                            lSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), leftTbl, null, leftTbl.getMemberMapping(leftMmd), null, null, true);
                        }
                    }
                }
                else if (relationType == RelationType.ONE_TO_MANY_UNI)
                {
                    if (leftMmd.getJoinMetaData() != null || rightMmd.getJoinMetaData() != null)
                    {
                        // 1-N with join table to right table, so join from right to join table
                        JoinTable joinTbl = (JoinTable) storeMgr.getTable(leftMmd);
                        SQLTable joinSqlTbl = null;
                        if (leftMmd.hasCollection())
                        {
                            joinSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), joinTbl, null, ((ElementContainerTable)joinTbl).getElementMapping(),
                                null, null, true);
                        }
                        else if (leftMmd.hasMap())
                        {
                            joinSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), joinTbl, null, ((MapTable)joinTbl).getValueMapping(), null, null, true);
                        }

                        if (i == 0)
                        {
                            // Add where clause join table to outer table
                            SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                            SQLExpression joinExpr = exprFactory.newExpression(stmt, joinSqlTbl, joinTbl.getOwnerMapping());
                            stmt.whereAnd(outerExpr.eq(joinExpr), false);
                        }
                        else
                        {
                            // Join to left table
                            lSqlTbl = stmt.join(JoinType.INNER_JOIN, joinSqlTbl, joinTbl.getOwnerMapping(), leftTbl, null, leftTbl.getIdMapping(), null, null, true);
                        }
                    }
                    else
                    {
                        // 1-N with FK in right table
                        if (i == 0)
                        {
                            // Add where clause right table to outer table
                            SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                            SQLExpression rightExpr = exprFactory.newExpression(stmt, rSqlTbl, rSqlTbl.getTable().getMemberMapping(rightMmd));
                            stmt.whereAnd(outerExpr.eq(rightExpr), false);
                        }
                        else
                        {
                            // Join to left table
                            lSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getMemberMapping(rightMmd), leftTbl, null, leftTbl.getIdMapping(), null, null, true);
                        }
                    }
                }
                else if (relationType == RelationType.ONE_TO_MANY_BI)
                {
                    if (leftMmd.getJoinMetaData() != null || rightMmd.getJoinMetaData() != null)
                    {
                        // 1-N with join table to right table, so join from right to join table
                        JoinTable joinTbl = (JoinTable) storeMgr.getTable(leftMmd);
                        SQLTable joinSqlTbl = null;
                        if (leftMmd.hasCollection())
                        {
                            joinSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), joinTbl, null, ((ElementContainerTable)joinTbl).getElementMapping(),
                                null, null, true);
                        }
                        else if (leftMmd.hasMap())
                        {
                            joinSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), joinTbl, null, ((MapTable)joinTbl).getValueMapping(), null, null, true);
                        }

                        if (i == 0)
                        {
                            // Add where clause join table to outer table
                            SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                            SQLExpression joinExpr = exprFactory.newExpression(stmt, joinSqlTbl, joinTbl.getOwnerMapping());
                            stmt.whereAnd(outerExpr.eq(joinExpr), false);
                        }
                        else
                        {
                            // Join to left table
                            lSqlTbl = stmt.join(JoinType.INNER_JOIN, joinSqlTbl, joinTbl.getOwnerMapping(), leftTbl, null, leftTbl.getIdMapping(), null, null, true);
                        }
                    }
                    else
                    {
                        // 1-N with FK in right table
                        if (i == 0)
                        {
                            // Add where clause right table to outer table
                            SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getIdMapping());
                            SQLExpression rightExpr = exprFactory.newExpression(stmt, rSqlTbl, rSqlTbl.getTable().getMemberMapping(rightMmd));
                            stmt.whereAnd(outerExpr.eq(rightExpr), false);
                        }
                        else
                        {
                            // Join to left table
                            lSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getMemberMapping(rightMmd), leftTbl, null, leftTbl.getIdMapping(), null, null, true);
                        }
                    }
                }
                else if (relationType == RelationType.MANY_TO_ONE_BI)
                {
                    if (leftMmd.getJoinMetaData() != null || rightMmd.getJoinMetaData() != null)
                    {
                        // 1-N with join table to right table, so join from right to join table
                        JoinTable joinTbl = (JoinTable) storeMgr.getTable(leftMmd);
                        SQLTable joinSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), joinTbl, null, joinTbl.getOwnerMapping(), null, null, true);

                        if (leftMmd.hasCollection())
                        {
                            if (i == 0)
                            {
                                // Add where clause join table to outer table
                                SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                                SQLExpression joinExpr = exprFactory.newExpression(stmt, joinSqlTbl, ((ElementContainerTable)joinTbl).getElementMapping());
                                stmt.whereAnd(outerExpr.eq(joinExpr), false);
                            }
                            else
                            {
                                // Join to left table
                                lSqlTbl = stmt.join(JoinType.INNER_JOIN, joinSqlTbl, ((ElementContainerTable)joinTbl).getElementMapping(), leftTbl, null, leftTbl.getIdMapping(), null, null, true);
                            }
                        }
                        else if (leftMmd.hasMap())
                        {
                            if (i == 0)
                            {
                                // Add where clause join table to outer table
                                SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                                SQLExpression joinExpr = exprFactory.newExpression(stmt, joinSqlTbl, ((MapTable)joinTbl).getValueMapping());
                                stmt.whereAnd(outerExpr.eq(joinExpr), false);
                            }
                            else
                            {
                                // Join to left table
                                lSqlTbl = stmt.join(JoinType.INNER_JOIN, joinSqlTbl, ((MapTable)joinTbl).getValueMapping(), leftTbl, null, leftTbl.getIdMapping(), null, null, true);
                            }
                        }
                    }
                    else
                    {
                        if (i == 0)
                        {
                            // Add where clause right table to outer table
                            SQLExpression outerExpr = exprFactory.newExpression(outerSqlTbl.getSQLStatement(), outerSqlTbl, outerSqlTbl.getTable().getMemberMapping(leftMmd));
                            SQLExpression rightExpr = exprFactory.newExpression(stmt, rSqlTbl, rSqlTbl.getTable().getIdMapping());
                            stmt.whereAnd(outerExpr.eq(rightExpr), false);
                        }
                        else
                        {
                            // Join to left table
                            lSqlTbl = stmt.join(JoinType.INNER_JOIN, rSqlTbl, rSqlTbl.getTable().getIdMapping(), leftTbl, null, leftTbl.getMemberMapping(leftMmd), null, null, true);
                        }
                    }
                }
                rSqlTbl = lSqlTbl;
            }
        }

        // Process all join expressions
        Expression rightExpr = clsExpr.getRight();
        SQLTable sqlTbl = candSqlTbl;
        while (rightExpr != null)
        {
            if (rightExpr instanceof JoinExpression)
            {
                JoinExpression joinExpr = (JoinExpression)rightExpr;
                org.datanucleus.query.expression.JoinExpression.JoinType exprJoinType = joinExpr.getType();
                JoinType joinType = org.datanucleus.store.rdbms.sql.SQLJoin.getJoinTypeForJoinExpressionType(exprJoinType);
                
                String joinAlias = joinExpr.getAlias();
                PrimaryExpression joinPrimExpr = joinExpr.getPrimaryExpression();

                Iterator<String> iter = joinPrimExpr.getTuples().iterator();
                String rootId = iter.next();
                String joinTableGroupName = null;

                if (rootId.equalsIgnoreCase(candidateAlias))
                {
                    // Join relative to the candidate
                    // Name table group of joined-to as per the relation
                    // Note : this will only work for one level out from the candidate TODO Extend this
                    cmd = candidateCmd;
                    joinTableGroupName = joinPrimExpr.getId();
                    sqlTbl = candSqlTbl;
                }
                else
                {
                    // Join relative to some other alias
                    SQLTableMapping sqlTblMapping = getSQLTableMappingForAlias(rootId);
                    if (sqlTblMapping != null)
                    {
                        if (sqlTblMapping.mmd != null)
                        {
                            // Map alias : assumed to be referring to the value of the Map
                            cmd = sqlTblMapping.mmd.getMap().getValueClassMetaData(clr, mmgr);
                            // TODO Clean this up so we match when it is present
                            sqlTbl = stmt.getTable(rootId + MAP_VALUE_ALIAS_SUFFIX);
                            if (sqlTbl == null)
                            {
                                sqlTbl = stmt.getTable((rootId + MAP_VALUE_ALIAS_SUFFIX).toUpperCase());
                            }
                        }
                        else
                        {
                            cmd = sqlTblMapping.cmd;
                            sqlTbl = sqlTblMapping.table;
                        }
                        joinTableGroupName = sqlTbl.getGroupName() + joinPrimExpr.getId().substring(rootId.length());
                    }
                    else
                    {
                        throw new NucleusUserException("Query has " + joinPrimExpr.getId() + " yet the first component " + rootId + " is unknown!");
                    }
                }

                SQLTable tblMappingSqlTbl = null;
                JavaTypeMapping tblIdMapping = null;
                AbstractMemberMetaData tblMmd = null;
                while (iter.hasNext())
                {
                    String id = iter.next();
                    String[] ids = null;
                    if (id.contains("."))
                    {
                        ids = StringUtils.split(id, ".");
                    }
                    else
                    {
                        ids = new String[] {id};
                    }

                    for (int k=0;k<ids.length;k++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForMember(ids[k]);
                        if (mmd == null)
                        {
                            if (exprJoinType == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_LEFT_OUTER ||
                                exprJoinType == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_LEFT_OUTER_FETCH)
                            {
                                // Polymorphic join, where the field exists in a subclass (doable since we have outer join)
                                String[] subclasses = mmgr.getSubclassesForClass(cmd.getFullClassName(), true);
                                for (int l=0;l<subclasses.length;l++)
                                {
                                    AbstractClassMetaData subCmd = mmgr.getMetaDataForClass(subclasses[l], clr);
                                    if (subCmd != null)
                                    {
                                        mmd = subCmd.getMetaDataForMember(ids[k]);
                                        if (mmd != null)
                                        {
                                            cmd = subCmd;
                                            break;
                                        }
                                    }
                                }
                            }
                            if (mmd == null)
                            {
                                throw new NucleusUserException("Query has " + joinPrimExpr.getId() + " yet " + ids[k] + " is not found. Fix your input");
                            }
                        }
                        tblMmd = null;

                        String aliasForJoin = null;
                        if (k == (ids.length-1) && !iter.hasNext())
                        {
                            aliasForJoin = joinAlias;
                        }

                        RelationType relationType = mmd.getRelationType(clr);
                        DatastoreClass relTable = null;
                        AbstractMemberMetaData relMmd = null;
                        if (relationType != RelationType.NONE)
                        {
                            if (exprJoinType == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_INNER_FETCH || 
                                exprJoinType == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_LEFT_OUTER_FETCH || 
                                exprJoinType == org.datanucleus.query.expression.JoinExpression.JoinType.JOIN_RIGHT_OUTER_FETCH)
                            {
                                // Add field to FetchPlan since marked for FETCH
                                String fgName = "QUERY_FETCH_" + mmd.getFullFieldName();
                                FetchGroupManager fetchGrpMgr = storeMgr.getNucleusContext().getFetchGroupManager();
                                if (fetchGrpMgr.getFetchGroupsWithName(fgName) == null)
                                {
                                    FetchGroup grp = new FetchGroup(storeMgr.getNucleusContext(), fgName, clr.classForName(cmd.getFullClassName()));
                                    grp.addMember(mmd.getName());
                                    fetchGrpMgr.addFetchGroup(grp);
                                }
                                fetchPlan.addGroup(fgName);
                            }
                        }

                        if (relationType == RelationType.ONE_TO_ONE_UNI)
                        {
                            cmd = mmgr.getMetaDataForClass(mmd.getType(), clr);
                            if (!mmd.isEmbedded())
                            {
                                relTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
                                JavaTypeMapping otherMapping = sqlTbl.getTable().getMemberMapping(mmd);
                                if (otherMapping == null)
                                {
                                    // Polymorphic join? : cannot find this member in the candidate of the main statement, so need to pick which UNION
                                    String tblGroupName = sqlTbl.getGroupName();
                                    SQLTableGroup grp = stmt.getTableGroup(tblGroupName);
                                    SQLTable nextSqlTbl = null;

                                    // Try to find subtable in the same group that has a mapping for this member (and join from that)
                                    SQLTable[] grpTbls = grp.getTables();
                                    for (SQLTable grpTbl : grpTbls)
                                    {
                                        if (grpTbl.getTable().getMemberMapping(mmd) != null)
                                        {
                                            otherMapping = grpTbl.getTable().getMemberMapping(mmd);
                                            break;
                                        }
                                    }
                                    SQLTable newSqlTbl = stmt.join(joinType, sqlTbl, otherMapping, relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, false);
                                    if (newSqlTbl != null)
                                    {
                                        nextSqlTbl = newSqlTbl;
                                    }

                                    List<SQLStatement> unionStmts = ((SelectStatement)stmt).getUnions();
                                    if (unionStmts != null)
                                    {
                                        for (SQLStatement unionStmt : unionStmts)
                                        {
                                            // Repeat the process for any unioned statements, find a subtable in the same group (and join from that)
                                            otherMapping = null;
                                            grp = unionStmt.getTableGroup(tblGroupName);
                                            SQLTable[] unionGrpTbls = grp.getTables();
                                            for (SQLTable grpTbl : unionGrpTbls)
                                            {
                                                if (grpTbl.getTable().getMemberMapping(mmd) != null)
                                                {
                                                    otherMapping = grpTbl.getTable().getMemberMapping(mmd);
                                                    break;
                                                }
                                            }
                                            newSqlTbl = unionStmt.join(joinType, sqlTbl, otherMapping, relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, false);
                                            if (newSqlTbl != null)
                                            {
                                                nextSqlTbl = newSqlTbl;
                                            }
                                        }
                                    }

                                    sqlTbl = nextSqlTbl;
                                }
                                else
                                {
                                    sqlTbl = stmt.join(joinType, sqlTbl, otherMapping, relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, true);
                                }
                            }
                            tblIdMapping = sqlTbl.getTable().getIdMapping();
                            tblMappingSqlTbl = sqlTbl;
                        }
                        else if (relationType == RelationType.ONE_TO_ONE_BI)
                        {
                            cmd = storeMgr.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                            if (!mmd.isEmbedded())
                            {
                                relTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
                                if (mmd.getMappedBy() != null)
                                {
                                    relMmd = mmd.getRelatedMemberMetaData(clr)[0];
                                    JavaTypeMapping relMapping = relTable.getMemberMapping(relMmd);
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), relTable, aliasForJoin, relMapping, null, joinTableGroupName, true);
                                }
                                else
                                {
                                    JavaTypeMapping otherMapping = sqlTbl.getTable().getMemberMapping(mmd);
                                    sqlTbl = stmt.join(joinType, sqlTbl, otherMapping, relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, true);
                                }
                            }
                            tblIdMapping = sqlTbl.getTable().getIdMapping();
                            tblMappingSqlTbl = sqlTbl;
                        }
                        else if (relationType == RelationType.ONE_TO_MANY_BI)
                        {
                            if (mmd.hasCollection())
                            {
                                // TODO Cater for embedded element
                                relTable = storeMgr.getDatastoreClass(mmd.getCollection().getElementType(), clr);
                                cmd = mmd.getCollection().getElementClassMetaData(clr, mmgr);
                                relMmd = mmd.getRelatedMemberMetaData(clr)[0];
                                if (mmd.getJoinMetaData() != null || relMmd.getJoinMetaData() != null)
                                {
                                    // Join to join table, then to related table
                                    ElementContainerTable joinTbl = (ElementContainerTable)storeMgr.getTable(mmd);
                                    SQLTable joinSqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, null, joinTbl.getOwnerMapping(), null, null, true);
                                    sqlTbl = stmt.join(joinType, joinSqlTbl, joinTbl.getElementMapping(), relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, true);
                                }
                                else
                                {
                                    // Join to related table
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), relTable, aliasForJoin, relTable.getMemberMapping(relMmd), null, joinTableGroupName, true);
                                }
                                tblIdMapping = sqlTbl.getTable().getIdMapping();
                                tblMappingSqlTbl = sqlTbl;
                            }
                            else if (mmd.hasMap())
                            {
                                MapMetaData mapmd = mmd.getMap();
                                tblMmd = mmd;
                                tblIdMapping = sqlTbl.getTable().getMemberMapping(mmd);
                                tblMappingSqlTbl = sqlTbl;

                                if (mapmd.getMapType() == MapType.MAP_TYPE_JOIN)
                                {
                                    // Add join to join table, then to related table (value)
                                    boolean embeddedValue = mapmd.isEmbeddedValue() || mapmd.isSerializedValue();
                                    MapTable joinTbl = (MapTable)storeMgr.getTable(mmd);
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, aliasForJoin, joinTbl.getOwnerMapping(), null, null, true);
                                    if (!embeddedValue)
                                    {
                                        // Join to value
                                        relTable = storeMgr.getDatastoreClass(mapmd.getValueType(), clr);
                                        String valueTableAlias = (aliasForJoin != null ? aliasForJoin+MAP_VALUE_ALIAS_SUFFIX : null);
                                        stmt.join(joinType, sqlTbl, joinTbl.getValueMapping(), relTable, valueTableAlias, relTable.getIdMapping(), null, joinTableGroupName, true);
                                    }
                                }
                                else if (mapmd.getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
                                {
                                    DatastoreClass valTable = storeMgr.getDatastoreClass(mapmd.getValueType(), clr);
                                    JavaTypeMapping mapTblOwnerMapping;
                                    if (mmd.getMappedBy() != null)
                                    {
                                        mapTblOwnerMapping = valTable.getMemberMapping(mapmd.getValueClassMetaData(clr, mmgr).getMetaDataForMember(mmd.getMappedBy()));
                                    }
                                    else
                                    {
                                        mapTblOwnerMapping = valTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                    }

                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), valTable, aliasForJoin, mapTblOwnerMapping, null, null, true);
                                }
                                else if (mapmd.getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
                                {
                                    DatastoreClass keyTable = storeMgr.getDatastoreClass(mapmd.getKeyType(), clr);
                                    JavaTypeMapping mapTblOwnerMapping;
                                    if (mmd.getMappedBy() != null)
                                    {
                                        mapTblOwnerMapping = keyTable.getMemberMapping(mapmd.getKeyClassMetaData(clr, mmgr).getMetaDataForMember(mmd.getMappedBy()));
                                    }
                                    else
                                    {
                                        mapTblOwnerMapping = keyTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                    }

                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), keyTable, aliasForJoin, mapTblOwnerMapping, null, null, true);
                                    // TODO Join to value if entity?
                                }
                            }
                        }
                        else if (relationType == RelationType.ONE_TO_MANY_UNI)
                        {
                            if (mmd.hasCollection())
                            {
                                // TODO Cater for embedded element
                                relTable = storeMgr.getDatastoreClass(mmd.getCollection().getElementType(), clr);
                                cmd = mmd.getCollection().getElementClassMetaData(clr, mmgr);
                                if (mmd.getJoinMetaData() != null)
                                {
                                    // Join to join table, then to related table
                                    ElementContainerTable joinTbl = (ElementContainerTable)storeMgr.getTable(mmd);
                                    SQLTable joinSqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, null, joinTbl.getOwnerMapping(), null, null, true);
                                    sqlTbl = stmt.join(joinType, joinSqlTbl, joinTbl.getElementMapping(), relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, true);
                                }
                                else
                                {
                                    // Join to related table
                                    JavaTypeMapping relMapping = relTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), relTable, aliasForJoin, relMapping, null, joinTableGroupName, true);
                                }

                                tblMappingSqlTbl = sqlTbl;
                                tblIdMapping = tblMappingSqlTbl.getTable().getIdMapping();
                            }
                            else if (mmd.hasMap())
                            {
                                MapMetaData mapmd = mmd.getMap();
                                cmd = mapmd.getValueClassMetaData(clr, mmgr);
                                tblMmd = mmd;
                                tblIdMapping = sqlTbl.getTable().getMemberMapping(mmd);
                                tblMappingSqlTbl = sqlTbl;

                                if (mapmd.getMapType() == MapType.MAP_TYPE_JOIN)
                                {
                                    // Add join to join table, then to related table (value)
                                    MapTable joinTbl = (MapTable)storeMgr.getTable(mmd);
                                    boolean embeddedValue = mapmd.isEmbeddedValue() || mmd.getMap().isSerializedValue();
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, aliasForJoin, joinTbl.getOwnerMapping(), null, null, true);
                                    if (!embeddedValue)
                                    {
                                        // Join to value table and use that
                                        relTable = storeMgr.getDatastoreClass(mapmd.getValueType(), clr);
                                        String valueTableAlias = (aliasForJoin != null ? aliasForJoin+MAP_VALUE_ALIAS_SUFFIX : null);
                                        stmt.join(joinType, sqlTbl, joinTbl.getValueMapping(), relTable, valueTableAlias, relTable.getIdMapping(), null, joinTableGroupName, true);
                                    }
                                }
                                else if (mapmd.getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
                                {
                                    DatastoreClass valTable = storeMgr.getDatastoreClass(mapmd.getValueType(), clr);
                                    JavaTypeMapping mapTblOwnerMapping;
                                    if (mmd.getMappedBy() != null)
                                    {
                                        mapTblOwnerMapping = valTable.getMemberMapping(mapmd.getValueClassMetaData(clr, mmgr).getMetaDataForMember(mmd.getMappedBy()));
                                    }
                                    else
                                    {
                                        mapTblOwnerMapping = valTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                    }

                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), valTable, aliasForJoin, mapTblOwnerMapping, null, null, true);
                                }
                                else if (mapmd.getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
                                {
                                    DatastoreClass keyTable = storeMgr.getDatastoreClass(mapmd.getKeyType(), clr);
                                    JavaTypeMapping mapTblOwnerMapping;
                                    if (mmd.getMappedBy() != null)
                                    {
                                        mapTblOwnerMapping = keyTable.getMemberMapping(mapmd.getKeyClassMetaData(clr, mmgr).getMetaDataForMember(mmd.getMappedBy()));
                                    }
                                    else
                                    {
                                        mapTblOwnerMapping = keyTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                    }

                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), keyTable, aliasForJoin, mapTblOwnerMapping, null, null, true);
                                    // TODO Join to value if entity?
                                }
                            }
                        }
                        else if (relationType == RelationType.MANY_TO_MANY_BI)
                        {
                            relTable = storeMgr.getDatastoreClass(mmd.getCollection().getElementType(), clr);
                            cmd = mmd.getCollection().getElementClassMetaData(clr, mmgr);
                            relMmd = mmd.getRelatedMemberMetaData(clr)[0];

                            // Join to join table, then to related table
                            CollectionTable joinTbl = (CollectionTable)storeMgr.getTable(mmd);
                            SQLTable joinSqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, null, joinTbl.getOwnerMapping(), null, null, true);
                            sqlTbl = stmt.join(joinType, joinSqlTbl, joinTbl.getElementMapping(), relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, true);

                            tblMappingSqlTbl = sqlTbl;
                            tblIdMapping = tblMappingSqlTbl.getTable().getIdMapping();
                        }
                        else if (relationType == RelationType.MANY_TO_ONE_BI)
                        {
                            relTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
                            cmd = storeMgr.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                            relMmd = mmd.getRelatedMemberMetaData(clr)[0];
                            if (mmd.getJoinMetaData() != null || relMmd.getJoinMetaData() != null)
                            {
                                // Join to join table, then to related table TODO Cater for Map case
                                CollectionTable joinTbl = (CollectionTable)storeMgr.getTable(relMmd);
                                SQLTable joinSqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, null, joinTbl.getElementMapping(), null, null, true);
                                sqlTbl = stmt.join(joinType, joinSqlTbl, joinTbl.getOwnerMapping(), relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, true);
                            }
                            else
                            {
                                // Join to owner table
                                JavaTypeMapping fkMapping = sqlTbl.getTable().getMemberMapping(mmd);
                                sqlTbl = stmt.join(joinType, sqlTbl, fkMapping, relTable, aliasForJoin, relTable.getIdMapping(), null, joinTableGroupName, true);
                            }

                            tblMappingSqlTbl = sqlTbl;
                            tblIdMapping = tblMappingSqlTbl.getTable().getIdMapping();
                        }
                        else
                        {
                            if (mmd.hasCollection())
                            {
                                cmd = null;
                                if (mmd.getJoinMetaData() != null)
                                {
                                    // Join to join table
                                    MapTable joinTbl = (MapTable)storeMgr.getTable(mmd);
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, aliasForJoin, joinTbl.getOwnerMapping(), null, null, true);

                                    tblMappingSqlTbl = sqlTbl;
                                    tblIdMapping = tblMappingSqlTbl.getTable().getIdMapping();
                                }
                                else
                                {
                                    throw new NucleusUserException("FROM clause contains join to Collection field at " + mmd.getFullFieldName() + " yet this has no join table");
                                }
                            }
                            else if (mmd.hasMap())
                            {
                                cmd = null;
                                MapMetaData mapmd = mmd.getMap();
                                tblMmd = mmd;
                                tblIdMapping = sqlTbl.getTable().getMemberMapping(mmd);
                                tblMappingSqlTbl = sqlTbl;

                                if (mapmd.getMapType() == MapType.MAP_TYPE_JOIN)
                                {
                                    // Add join to join table
                                    MapTable joinTbl = (MapTable)storeMgr.getTable(mmd);
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), joinTbl, aliasForJoin, joinTbl.getOwnerMapping(), null, null, true);
                                }
                                else if (mapmd.getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
                                {
                                    DatastoreClass valTable = storeMgr.getDatastoreClass(mapmd.getValueType(), clr);
                                    AbstractClassMetaData valCmd = mapmd.getValueClassMetaData(clr, mmgr);
                                    JavaTypeMapping mapTblOwnerMapping;
                                    if (mmd.getMappedBy() != null)
                                    {
                                        mapTblOwnerMapping = valTable.getMemberMapping(valCmd.getMetaDataForMember(mmd.getMappedBy()));
                                    }
                                    else
                                    {
                                        mapTblOwnerMapping = valTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                    }
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), valTable, aliasForJoin, mapTblOwnerMapping, null, null, true);
                                }
                                else if (mapmd.getMapType() == MapType.MAP_TYPE_VALUE_IN_KEY)
                                {
                                    DatastoreClass keyTable = storeMgr.getDatastoreClass(mapmd.getKeyType(), clr);
                                    AbstractClassMetaData keyCmd = mapmd.getKeyClassMetaData(clr, mmgr);
                                    JavaTypeMapping mapTblOwnerMapping;
                                    if (mmd.getMappedBy() != null)
                                    {
                                        mapTblOwnerMapping = keyTable.getMemberMapping(keyCmd.getMetaDataForMember(mmd.getMappedBy()));
                                    }
                                    else
                                    {
                                        mapTblOwnerMapping = keyTable.getExternalMapping(mmd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
                                    }
                                    sqlTbl = stmt.join(joinType, sqlTbl, sqlTbl.getTable().getIdMapping(), keyTable, aliasForJoin, mapTblOwnerMapping, null, null, true);
                                }
                            }
                        }
                    }
                }

                if (joinAlias != null)
                {
                    if (explicitJoinPrimaryByAlias == null)
                    {
                        explicitJoinPrimaryByAlias = new HashMap<String, String>();
                    }
                    explicitJoinPrimaryByAlias.put(joinAlias, joinPrimExpr.getId());

                    SQLTableMapping tblMapping = null;
                    if (tblMmd != null)
                    {
                        // Maps store the member so we can more easily navigate to the key/value
                        tblMapping = new SQLTableMapping(tblMappingSqlTbl, tblMmd, tblIdMapping);
                    }
                    else
                    {
                        tblMapping = new SQLTableMapping(tblMappingSqlTbl, cmd, tblIdMapping);
                    }
                    setSQLTableMappingForAlias(joinAlias, tblMapping);
                }

                Expression joinOnExpr = joinExpr.getOnExpression();
                if (joinOnExpr != null)
                {
                    // Convert the ON expression to a BooleanExpression
                    processingOnClause = true;
                    joinOnExpr.evaluate(this);
                    BooleanExpression joinOnSqlExpr = (BooleanExpression) stack.pop();
                    processingOnClause = false;

                    // Add the ON expression to the most recent SQLTable at the end of this chain
                    SQLJoin join = stmt.getJoinForTable(sqlTbl);
                    join.addAndCondition(joinOnSqlExpr);
                }
            }

            // Move on to next join in the chain
            rightExpr = rightExpr.getRight();
        }
    }

    boolean processingOnClause = false;
    public boolean processingOnClause()
    {
        return processingOnClause;
    }

    /**
     * Convenience method to convert a NewObjectExpression into a StatementNewObjectMapping.
     * Handles recursive new object calls (where a new object is an arg to a new object construction).
     * @param expr The NewObjectExpression
     * @return The mapping for the new object
     */
    protected StatementNewObjectMapping getStatementMappingForNewObjectExpression(NewObjectExpression expr, SelectStatement stmt)
    {
        List argExprs = expr.getConstructorArgExpressions();
        StatementNewObjectMapping stmtMap = new StatementNewObjectMapping(expr.getNewClass());
        if (argExprs != null)
        {
            Iterator<SQLExpression> argIter = argExprs.iterator();
            int j = 0;
            while (argIter.hasNext())
            {
                SQLExpression argExpr = argIter.next();
                if (argExpr instanceof SQLLiteral)
                {
                    stmtMap.addConstructorArgMapping(j, ((SQLLiteral)argExpr).getValue());
                }
                else if (argExpr instanceof NewObjectExpression)
                {
                    stmtMap.addConstructorArgMapping(j, getStatementMappingForNewObjectExpression((NewObjectExpression)argExpr, stmt));
                }
                else
                {
                    StatementMappingIndex idx = new StatementMappingIndex(argExpr.getJavaTypeMapping());
                    int[] cols = stmt.select(argExpr, null);
                    idx.setColumnPositions(cols);
                    stmtMap.addConstructorArgMapping(j, idx);
                }
                j++;
            }
        }
        return stmtMap;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processAndExpression(org.datanucleus.query.expression.Expression)
     */
    protected Object processAndExpression(Expression expr)
    {
        SQLExpression rightExpr = stack.pop();
        SQLExpression leftExpr = stack.pop();
        if (!(rightExpr instanceof BooleanExpression))
        {
            throw new NucleusUserException("Query has clause " + rightExpr + " used with AND. This is illegal, and should be a boolean expression");
        }
        if (!(leftExpr instanceof BooleanExpression))
        {
            throw new NucleusUserException("Query has clause " + leftExpr + " used with AND. This is illegal, and should be a boolean expression");
        }

        BooleanExpression right = (BooleanExpression)rightExpr;
        BooleanExpression left = (BooleanExpression)leftExpr;
        if (left.getSQLStatement() != null && right.getSQLStatement() != null &&
            left.getSQLStatement() != right.getSQLStatement())
        {
            if (left.getSQLStatement() == stmt &&
                right.getSQLStatement().isChildStatementOf(stmt))
            {
                // Apply right to its sub-statement now and return left
                right.getSQLStatement().whereAnd(right, true);
                stack.push(left);
                return left;
            }
            else if (right.getSQLStatement() == stmt &&
                left.getSQLStatement().isChildStatementOf(stmt))
            {
                // Apply left to its sub-statement now and return right
                left.getSQLStatement().whereAnd(left, true);
                stack.push(right);
                return right;
            }
            // TODO Cater for more situations
        }

        if (compileComponent == CompilationComponent.FILTER)
        {
            // Make sure any simple boolean field clauses are suitable
            left = getBooleanExpressionForUseInFilter(left);
            right = getBooleanExpressionForUseInFilter(right);
        }

        BooleanExpression opExpr = left.and(right);
        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processOrExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processOrExpression(Expression expr)
    {
        SQLExpression rightExpr = stack.pop();
        SQLExpression leftExpr = stack.pop();
        if (!(rightExpr instanceof BooleanExpression))
        {
            throw new NucleusUserException("Query has clause " + rightExpr + " used with AND. This is illegal, and should be a boolean expression");
        }
        if (!(leftExpr instanceof BooleanExpression))
        {
            throw new NucleusUserException("Query has clause " + leftExpr + " used with AND. This is illegal, and should be a boolean expression");
        }

        BooleanExpression right = (BooleanExpression)rightExpr;
        BooleanExpression left = (BooleanExpression)leftExpr;
        if (left.getSQLStatement() != null && right.getSQLStatement() != null && left.getSQLStatement() != right.getSQLStatement())
        {
            if (left.getSQLStatement() == stmt && right.getSQLStatement().isChildStatementOf(stmt))
            {
                // Apply right to its sub-statement now and return left
                right.getSQLStatement().whereAnd(right, true);
                stack.push(left);
                return left;
            }
            else if (right.getSQLStatement() == stmt && left.getSQLStatement().isChildStatementOf(stmt))
            {
                // Apply left to its sub-statement now and return right
                left.getSQLStatement().whereAnd(left, true);
                stack.push(right);
                return right;
            }
            // TODO Cater for more situations
        }

        if (compileComponent == CompilationComponent.FILTER)
        {
            // Make sure any simple boolean field clauses are suitable
            left = getBooleanExpressionForUseInFilter(left);
            right = getBooleanExpressionForUseInFilter(right);
        }

        left.encloseInParentheses();
        right.encloseInParentheses();
        BooleanExpression opExpr = left.ior(right);
        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processBitAndExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processBitAndExpression(Expression expr)
    {
        SQLExpression rightExpr = stack.pop();
        SQLExpression leftExpr = stack.pop();
        if (rightExpr instanceof BooleanExpression && leftExpr instanceof BooleanExpression)
        {
            // Handle as Boolean logical AND
            stack.push(leftExpr);
            stack.push(rightExpr);
            return processAndExpression(expr);
        }
        else if (rightExpr instanceof NumericExpression && leftExpr instanceof NumericExpression)
        {
            if (storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.OPERATOR_BITWISE_AND))
            {
                SQLExpression bitAndExpr = new NumericExpression(leftExpr, Expression.OP_BIT_AND, rightExpr).encloseInParentheses();
                stack.push(bitAndExpr);
                return bitAndExpr;
            }
        }

        throw new NucleusUserException("Operation BITWISE AND is not supported for " + leftExpr + " and " + rightExpr + " for this datastore");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processBitOrExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processBitOrExpression(Expression expr)
    {
        SQLExpression rightExpr = stack.pop();
        SQLExpression leftExpr = stack.pop();
        if (rightExpr instanceof BooleanExpression && leftExpr instanceof BooleanExpression)
        {
            // Handle as Boolean logical OR
            stack.push(leftExpr);
            stack.push(rightExpr);
            return processOrExpression(expr);
        }
        else if (rightExpr instanceof NumericExpression && leftExpr instanceof NumericExpression)
        {
            if (storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.OPERATOR_BITWISE_OR))
            {
                SQLExpression bitExpr = new NumericExpression(leftExpr, Expression.OP_BIT_OR, rightExpr).encloseInParentheses();
                stack.push(bitExpr);
                return bitExpr;
            }
        }

        // TODO Support BITWISE OR for more cases
        throw new NucleusUserException("Operation BITWISE OR is not supported for " + leftExpr + " and " + rightExpr + " is not supported by this datastore");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processBitXorExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processBitXorExpression(Expression expr)
    {
        SQLExpression rightExpr = stack.pop();
        SQLExpression leftExpr = stack.pop();
        if (rightExpr instanceof BooleanExpression && leftExpr instanceof BooleanExpression)
        {
            // Handle as Boolean logical OR
            stack.push(leftExpr);
            stack.push(rightExpr);
            return processOrExpression(expr);
        }
        else if (rightExpr instanceof NumericExpression && leftExpr instanceof NumericExpression)
        {
            if (storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.OPERATOR_BITWISE_XOR))
            {
                SQLExpression bitExpr = new NumericExpression(leftExpr, Expression.OP_BIT_XOR, rightExpr).encloseInParentheses();
                stack.push(bitExpr);
                return bitExpr;
            }
        }

        // TODO Support BITWISE XOR for more cases
        throw new NucleusUserException("Operation BITWISE XOR is not supported for " + leftExpr + " and " + rightExpr + " is not supported by this datastore");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processEqExpression(org.datanucleus.query.expression.Expression)
     */
    protected Object processEqExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        if (left.isParameter() && right.isParameter())
        {
            if (left.isParameter() && left instanceof SQLLiteral && ((SQLLiteral)left).getValue() != null)
            {
                // Change this parameter to a plain literal
                useParameterExpressionAsLiteral((SQLLiteral)left);
            }
            if (right.isParameter() && right instanceof SQLLiteral && ((SQLLiteral)right).getValue() != null)
            {
                // Change this parameter to a plain literal
                useParameterExpressionAsLiteral((SQLLiteral)right);
            }
        }

        ExpressionUtils.checkAndCorrectExpressionMappingsForBooleanComparison(left, right);

        if (left instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression) left);
            left = stack.pop();
        }
        if (right instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression)right);
            right = stack.pop();
        }

        // Logic for when one side is cross-joined (variable) and other side not, so transfer to a left outer join
        if (!options.contains(OPTION_EXPLICIT_JOINS))
        {
            boolean leftIsCrossJoin = (stmt.getJoinTypeForTable(left.getSQLTable()) == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.CROSS_JOIN);
            boolean rightIsCrossJoin = (stmt.getJoinTypeForTable(right.getSQLTable()) == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.CROSS_JOIN);
            if (leftIsCrossJoin && !rightIsCrossJoin && !(right instanceof SQLLiteral))
            {
                // "a == b" and a is cross-joined currently (includes variable) so change to left outer join
                String varName = getAliasForSQLTable(left.getSQLTable());
                org.datanucleus.store.rdbms.sql.SQLJoin.JoinType joinType = getRequiredJoinTypeForAlias(varName);
                if (joinType != null)
                {
                    NucleusLogger.QUERY.debug("QueryToSQL.eq variable " + varName + " is mapped to table " + left.getSQLTable() +
                        " was previously bound as CROSS JOIN but changing to " + joinType);
                    String leftTblAlias = stmt.removeCrossJoin(left.getSQLTable());
                    if (joinType == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.LEFT_OUTER_JOIN)
                    {
                        stmt.join(JoinType.LEFT_OUTER_JOIN, right.getSQLTable(), right.getJavaTypeMapping(), 
                            left.getSQLTable().getTable(), leftTblAlias, left.getJavaTypeMapping(), null, left.getSQLTable().getGroupName(), true);
                    }
                    else
                    {
                        stmt.join(JoinType.INNER_JOIN, right.getSQLTable(), right.getJavaTypeMapping(), 
                            left.getSQLTable().getTable(), leftTblAlias, left.getJavaTypeMapping(), null, left.getSQLTable().getGroupName(), true);
                    }

                    JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
                    SQLExpression opExpr = exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, true));
                    stack.push(opExpr);
                    return opExpr;
                }
            }
            else if (!leftIsCrossJoin && rightIsCrossJoin && !(left instanceof SQLLiteral))
            {
                // "a == b" and b is cross-joined currently (includes variable) so change to left outer join
                String varName = getAliasForSQLTable(right.getSQLTable());
                org.datanucleus.store.rdbms.sql.SQLJoin.JoinType joinType = getRequiredJoinTypeForAlias(varName);
                if (joinType != null)
                {
                    NucleusLogger.QUERY.debug("QueryToSQL.eq variable " + varName + " is mapped to table " + right.getSQLTable() +
                        " was previously bound as CROSS JOIN but changing to " + joinType);
                    String rightTblAlias = stmt.removeCrossJoin(right.getSQLTable());
                    if (joinType == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.LEFT_OUTER_JOIN)
                    {
                        stmt.join(JoinType.LEFT_OUTER_JOIN, left.getSQLTable(), left.getJavaTypeMapping(), 
                            right.getSQLTable().getTable(), rightTblAlias, right.getJavaTypeMapping(), null, right.getSQLTable().getGroupName(), true);
                    }
                    else
                    {
                        stmt.join(JoinType.INNER_JOIN, left.getSQLTable(), left.getJavaTypeMapping(), 
                            right.getSQLTable().getTable(), rightTblAlias, right.getJavaTypeMapping(), null, right.getSQLTable().getGroupName(), true);
                    }

                    JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
                    SQLExpression opExpr = exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, true));
                    stack.push(opExpr);
                    return opExpr;
                }
            }
        }

        BooleanExpression opExpr = left.eq(right);
        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNoteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        if (left.isParameter() && right.isParameter())
        {
            if (left.isParameter() && left instanceof SQLLiteral && ((SQLLiteral)left).getValue() != null)
            {
                useParameterExpressionAsLiteral((SQLLiteral)left);
            }
            if (right.isParameter() && right instanceof SQLLiteral && ((SQLLiteral)right).getValue() != null)
            {
                useParameterExpressionAsLiteral((SQLLiteral)right);
            }
        }

        ExpressionUtils.checkAndCorrectExpressionMappingsForBooleanComparison(left, right);

        if (left instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression) left);
            left = stack.pop();
        }
        if (right instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression)right);
            right = stack.pop();
        }
        BooleanExpression opExpr = left.ne(right);

        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGteqExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        ExpressionUtils.checkAndCorrectExpressionMappingsForBooleanComparison(left, right);

        if (left instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression) left);
            left = stack.pop();
        }
        if (right instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression)right);
            right = stack.pop();
        }

        BooleanExpression opExpr = left.ge(right);

        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGtExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        ExpressionUtils.checkAndCorrectExpressionMappingsForBooleanComparison(left, right);

        if (left instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression) left);
            left = stack.pop();
        }
        if (right instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression)right);
            right = stack.pop();
        }

        BooleanExpression opExpr = left.gt(right);

        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLteqExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        ExpressionUtils.checkAndCorrectExpressionMappingsForBooleanComparison(left, right);

        if (left instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression) left);
            left = stack.pop();
        }
        if (right instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression)right);
            right = stack.pop();
        }

        BooleanExpression opExpr = left.le(right);

        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLtExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        ExpressionUtils.checkAndCorrectExpressionMappingsForBooleanComparison(left, right);

        if (left instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression) left);
            left = stack.pop();
        }
        if (right instanceof UnboundExpression)
        {
            processUnboundExpression((UnboundExpression)right);
            right = stack.pop();
        }

        BooleanExpression opExpr = left.lt(right);

        stack.push(opExpr);
        return opExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLiteral(org.datanucleus.query.expression.Literal)
     */
    protected Object processLiteral(Literal expr)
    {
        Object litValue = expr.getLiteral();
        SQLExpression sqlExpr = getSQLLiteralForLiteralValue(litValue);
        stack.push(sqlExpr);
        return sqlExpr;
    }

    protected SQLExpression getSQLLiteralForLiteralValue(Object litValue)
    {
        if (litValue instanceof Class)
        {
            // Convert Class literals (instanceof) into StringLiteral
            litValue = ((Class)litValue).getName();
        }
        else if (litValue instanceof String)
        {
            String litStr = (String)litValue;
            if (litStr.startsWith("{d ") || litStr.startsWith("{t ") || litStr.startsWith("{ts "))
            {
                JavaTypeMapping m = exprFactory.getMappingForType(Date.class, false);
                return exprFactory.newLiteral(stmt, m, litValue);
            }
        }

        JavaTypeMapping m = (litValue != null) ? exprFactory.getMappingForType(litValue.getClass(), false) : null;
        return exprFactory.newLiteral(stmt, m, litValue);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processPrimaryExpression(org.datanucleus.query.expression.PrimaryExpression)
     */
    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
        SQLExpression sqlExpr = null;
        if (expr.getLeft() != null)
        {
            if (expr.getLeft() instanceof DyadicExpression && expr.getLeft().getOperator() == Expression.OP_CAST)
            {
                String exprCastName = null;
                if (expr.getLeft().getLeft() instanceof PrimaryExpression)
                {
                    exprCastName = "CAST_" + ((PrimaryExpression)expr.getLeft().getLeft()).getId();
                }
                else if (expr.getLeft().getLeft() instanceof VariableExpression)
                {
                    exprCastName = "CAST_" + ((VariableExpression)expr.getLeft().getLeft()).getId();
                }
                else if (expr.getLeft().getLeft() instanceof InvokeExpression)
                {
                    exprCastName = "CAST_" + expr.getLeft().getLeft();
                }
                else
                {
                    throw new NucleusException("Don't currently support cast of " + expr.getLeft().getLeft());
                }
                expr.getLeft().getLeft().evaluate(this);
                sqlExpr = stack.pop();
                JavaTypeMapping mapping = sqlExpr.getJavaTypeMapping();
                if (mapping instanceof EmbeddedMapping)
                {
                    // Cast of an embedded field, so use same table

                    // Extract what we are casting it to
                    Literal castLitExpr = (Literal)expr.getLeft().getRight();
                    Class castType = resolveClass((String)castLitExpr.getLiteral());
                    AbstractClassMetaData castCmd = ec.getMetaDataManager().getMetaDataForClass(castType, clr);

                    JavaTypeMapping discMapping = ((EmbeddedMapping)mapping).getDiscriminatorMapping();
                    if (discMapping != null)
                    {
                        // Should have a discriminator always when casting this
                        AbstractClassMetaData fieldCmd = ec.getMetaDataManager().getMetaDataForClass(castType, clr);
                        DiscriminatorMetaData dismd = fieldCmd.getDiscriminatorMetaDataRoot();
                        SQLExpression discExpr = stmt.getSQLExpressionFactory().newExpression(stmt, sqlExpr.getSQLTable(), discMapping);
                        SQLExpression discVal = null;
                        if (dismd.getStrategy() == DiscriminatorStrategy.CLASS_NAME)
                        {
                            discVal = stmt.getSQLExpressionFactory().newLiteral(stmt, discMapping, castCmd.getFullClassName());
                        }
                        else
                        {
                            discVal = stmt.getSQLExpressionFactory().newLiteral(stmt, discMapping, castCmd.getDiscriminatorMetaData().getValue());
                        }
                        BooleanExpression discRestrictExpr = discExpr.eq(discVal);

                        Iterator<String> subclassIter = storeMgr.getSubClassesForClass(castType.getName(), true, clr).iterator();
                        while (subclassIter.hasNext())
                        {
                            String subclassName = subclassIter.next();
                            AbstractClassMetaData subtypeCmd =
                                storeMgr.getMetaDataManager().getMetaDataForClass(subclassName, clr);
                            if (dismd.getStrategy() == DiscriminatorStrategy.CLASS_NAME)
                            {
                                discVal = stmt.getSQLExpressionFactory().newLiteral(stmt, discMapping, subtypeCmd.getFullClassName());
                            }
                            else
                            {
                                discVal = stmt.getSQLExpressionFactory().newLiteral(stmt, discMapping, subtypeCmd.getDiscriminatorMetaData().getValue());
                            }
                            BooleanExpression subtypeExpr = discExpr.eq(discVal);

                            discRestrictExpr = discRestrictExpr.ior(subtypeExpr);
                        }

                        stmt.whereAnd(discRestrictExpr, true);
                    }

                    SQLTableMapping tblMapping = new SQLTableMapping(sqlExpr.getSQLTable(), castCmd, sqlExpr.getJavaTypeMapping());
                    setSQLTableMappingForAlias(exprCastName, tblMapping);

                    SQLTableMapping sqlMapping = getSQLTableMappingForPrimaryExpression(stmt, exprCastName, expr, Boolean.FALSE);
                    if (sqlMapping == null)
                    {
                        throw new NucleusException("PrimaryExpression " + expr + " is not yet supported");
                    }
                    sqlExpr = exprFactory.newExpression(stmt, sqlMapping.table, sqlMapping.mapping);
                    stack.push(sqlExpr);
                    return sqlExpr;
                }

                // Evaluate the cast
                expr.getLeft().evaluate(this);
                sqlExpr = stack.pop();

                // Extract what we are casting it to
                Literal castLitExpr = (Literal)expr.getLeft().getRight();
                AbstractClassMetaData castCmd = ec.getMetaDataManager().getMetaDataForClass(resolveClass((String)castLitExpr.getLiteral()), clr);

                SQLTableMapping tblMapping = new SQLTableMapping(sqlExpr.getSQLTable(), castCmd, sqlExpr.getJavaTypeMapping());
                setSQLTableMappingForAlias(exprCastName, tblMapping);

                SQLTableMapping sqlMapping = getSQLTableMappingForPrimaryExpression(stmt, exprCastName, expr, Boolean.FALSE);
                if (sqlMapping == null)
                {
                    throw new NucleusException("PrimaryExpression " + expr + " is not yet supported");
                }
                sqlExpr = exprFactory.newExpression(stmt, sqlMapping.table, sqlMapping.mapping);
                stack.push(sqlExpr);
                return sqlExpr;
            }
            else if (expr.getLeft() instanceof ParameterExpression)
            {
                // "{paramExpr}.field[.field[.field]]"
                setNotPrecompilable(); // Need parameter values to process this
                ParameterExpression paramExpr = (ParameterExpression)expr.getLeft();
                Symbol paramSym = compilation.getSymbolTable().getSymbol(paramExpr.getId());
                if (paramSym.getValueType() != null && paramSym.getValueType().isArray())
                {
                    // Special case : array "methods" (particularly "length")
                    String first = expr.getTuples().get(0);
                    processParameterExpression(paramExpr, true);
                    SQLExpression paramSqlExpr = stack.pop();
                    sqlExpr = exprFactory.invokeMethod(stmt, "ARRAY", first, paramSqlExpr, null);
                    stack.push(sqlExpr);
                    return sqlExpr;
                }

                // Create Literal for the parameter (since we need to perform operations on it)
                processParameterExpression(paramExpr, true);
                SQLExpression paramSqlExpr = stack.pop();
                SQLLiteral lit = (SQLLiteral)paramSqlExpr;
                Object paramValue = lit.getValue();

                List<String> tuples = expr.getTuples();
                Iterator<String> tuplesIter = tuples.iterator();
                Object objValue = paramValue;
                while (tuplesIter.hasNext())
                {
                    String fieldName = tuplesIter.next();
                    if (objValue == null)
                    {
                        NucleusLogger.QUERY.warn(">> Compilation of " + expr + " : need to direct through field \"" + fieldName + "\" on null value, hence not compilable!");
                        // Null value, and we have further path to navigate TODO Handle this "NPE"
                        break;
                    }
                    objValue = getValueForObjectField(objValue, fieldName);
                    setNotPrecompilable(); // Using literal value of parameter, so cannot precompile it
                }

                if (objValue == null)
                {
                    sqlExpr = exprFactory.newLiteral(stmt, null, null);
                    stack.push(sqlExpr);
                    return sqlExpr;
                }

                JavaTypeMapping m = exprFactory.getMappingForType(objValue.getClass(), false);
                sqlExpr = exprFactory.newLiteral(stmt, m, objValue);
                stack.push(sqlExpr);
                return sqlExpr;
            }
            else if (expr.getLeft() instanceof VariableExpression)
            {
                // "{varExpr}.field[.field[.field]]"
                VariableExpression varExpr = (VariableExpression)expr.getLeft();
                processVariableExpression(varExpr);
                SQLExpression varSqlExpr = stack.pop();
                if (varSqlExpr instanceof UnboundExpression)
                {
                    // Bind as CROSS JOIN for now
                    processUnboundExpression((UnboundExpression)varSqlExpr);
                    varSqlExpr = stack.pop();
                }

                Class varType = clr.classForName(varSqlExpr.getJavaTypeMapping().getType());
                if (varSqlExpr.getSQLStatement() == stmt.getParentStatement())
                {
                    // Use parent mapper to get the mapping for this field since it has the table
                    SQLTableMapping sqlMapping = parentMapper.getSQLTableMappingForPrimaryExpression(stmt, null, expr, Boolean.FALSE);
                    if (sqlMapping == null)
                    {
                        throw new NucleusException("PrimaryExpression " + expr.getId() + " is not yet supported");
                    }
                    // TODO Cater for the table required to join to not being the primary table of the outer query
                    // This should check on
                    // getDatastoreAdapter().supportsOption(RDBMSAdapter.ACCESS_PARENTQUERY_IN_SUBQUERY))

                    sqlExpr = exprFactory.newExpression(varSqlExpr.getSQLStatement(), sqlMapping.table, sqlMapping.mapping);
                    stack.push(sqlExpr);
                    return sqlExpr;
                }

                SQLTableMapping varTblMapping = getSQLTableMappingForAlias(varExpr.getId());
                if (varTblMapping == null)
                {
                    throw new NucleusUserException("Variable " + varExpr.getId() + " is not yet bound, so cannot get field " + expr.getId());
                }
                if (varTblMapping.cmd == null)
                {
                    throw new NucleusUserException("Variable " + varExpr.getId() + " of type " + varType.getName() + " cannot evaluate " + expr.getId());
                }

                SQLTableMapping sqlMapping = getSQLTableMappingForPrimaryExpression(varSqlExpr.getSQLStatement(), varExpr.getId(), expr, Boolean.FALSE);

                sqlExpr = exprFactory.newExpression(sqlMapping.table.getSQLStatement(), sqlMapping.table, sqlMapping.mapping);
                stack.push(sqlExpr);
                return sqlExpr;
            }
            else if (expr.getLeft() instanceof InvokeExpression)
            {
                processInvokeExpression((InvokeExpression)expr.getLeft());
                SQLExpression invokeSqlExpr = stack.pop();
                Table tbl = invokeSqlExpr.getSQLTable().getTable();
                if (expr.getTuples().size() > 1)
                {
                    throw new NucleusUserException("Dont currently support evaluating " + expr.getId() + " on " + invokeSqlExpr);
                }
                if (tbl instanceof DatastoreClass)
                {
                    // Table of a class, so assume to have field in the table of the class
                    // TODO Allow joins to superclasses if required
                    JavaTypeMapping mapping = ((DatastoreClass)tbl).getMemberMapping(expr.getId());
                    if (mapping == null)
                    {
                        throw new NucleusUserException("Dont currently support evaluating " + expr.getId() +
                            " on " + invokeSqlExpr + ". The field " + expr.getId() + " doesnt exist in table " + tbl);
                    }

                    sqlExpr = exprFactory.newExpression(stmt, invokeSqlExpr.getSQLTable(), mapping);
                    stack.push(sqlExpr);
                    return sqlExpr;
                }
                else if (tbl instanceof JoinTable)
                {
                    if (invokeSqlExpr.getJavaTypeMapping() instanceof EmbeddedMapping)
                    {
                        // Table containing an embedded element/key/value so assume we have a column in the join table
                        EmbeddedMapping embMapping = (EmbeddedMapping)invokeSqlExpr.getJavaTypeMapping();
                        JavaTypeMapping mapping = embMapping.getJavaTypeMapping(expr.getId());
                        if (mapping == null)
                        {
                            throw new NucleusUserException("Dont currently support evaluating " + expr.getId() +
                                " on " + invokeSqlExpr + ". The field " + expr.getId() + " doesnt exist in table " + tbl);
                        }
                        sqlExpr = exprFactory.newExpression(stmt, invokeSqlExpr.getSQLTable(), mapping);
                        stack.push(sqlExpr);
                        return sqlExpr;
                    }
                }

                throw new NucleusUserException("Dont currently support evaluating " + expr.getId() + " on " + invokeSqlExpr + " with invoke having table of " + tbl);
            }
            else
            {
                throw new NucleusUserException("Dont currently support PrimaryExpression with 'left' of " + expr.getLeft());
            }
        }

        // Real primary expression ("field.field", "alias.field.field" etc)
        SQLTableMapping sqlMapping = getSQLTableMappingForPrimaryExpression(stmt, null, expr, null);
        if (sqlMapping == null)
        {
            throw new NucleusException("PrimaryExpression " + expr.getId() + " is not yet supported");
        }

        sqlExpr = exprFactory.newExpression(stmt, sqlMapping.table, sqlMapping.mapping);
        if (sqlMapping.mmd != null && sqlExpr instanceof MapExpression)
        {
            // This sqlMapping is for something joined in a FROM clause, so set the alias on the returned MapExpression to avoid doing the same joins
            String alias = getAliasForSQLTableMapping(sqlMapping);
            if (alias == null && parentMapper != null)
            {
                alias = parentMapper.getAliasForSQLTableMapping(sqlMapping);
            }
            ((MapExpression)sqlExpr).setAliasForMapTable(alias);
        }

        stack.push(sqlExpr);
        return sqlExpr;
    }

    /**
     * Method to take in a PrimaryExpression and return the SQLTable mapping info that it signifies.
     * If the primary expression implies joining to other objects then adds the joins to the statement.
     * Only adds joins if necessary; so if there is a further component after the required join, or if
     * the "forceJoin" flag is set.
     * @param theStmt SQLStatement to use when looking for tables etc
     * @param exprName Name for an expression that this primary is relative to (optional)
     *                 If not specified then the tuples are relative to the candidate.
     *                 If specified then should have an entry in sqlTableByPrimary under this name.
     * @param primExpr The primary expression
     * @param forceJoin Whether to force a join if a relation member (or null if leaving to this method to decide)
     * @return The SQL table mapping information for the specified primary
     */
    private SQLTableMapping getSQLTableMappingForPrimaryExpression(SQLStatement theStmt, String exprName, PrimaryExpression primExpr, Boolean forceJoin)
    {
        if (forceJoin == null && primExpr.getParent() != null)
        {
            if (primExpr.getParent().getOperator() == Expression.OP_IS || 
                primExpr.getParent().getOperator() == Expression.OP_ISNOT)
            {
                // "instanceOf" needs to be in the table of the primary expression
                forceJoin = Boolean.TRUE;
            }
        }

        SQLTableMapping sqlMapping = null;

        List<String> tuples = primExpr.getTuples();

        // Find source object
        Iterator<String> iter = tuples.iterator();
        String first = tuples.get(0);
        String primaryName = null;
        if (exprName != null)
        {
            // Primary relative to some object etc
            sqlMapping = getSQLTableMappingForAlias(exprName);
            primaryName = exprName;
        }
        else
        {
            if (hasSQLTableMappingForAlias(first))
            {
                // Start from a candidate (e.g JPQL alias)
                sqlMapping = getSQLTableMappingForAlias(first);
                primaryName = first;
                iter.next(); // Skip first tuple
            }

            if (sqlMapping == null)
            {
                if (parentMapper != null && parentMapper.hasSQLTableMappingForAlias(first))
                {
                    // Try parent query
                    sqlMapping = parentMapper.getSQLTableMappingForAlias(first);
                    primaryName = first;
                    iter.next(); // Skip first tuple

                    // This expression is for the parent statement so any joins need to go on that statement
                    theStmt = sqlMapping.table.getSQLStatement();
                }
            }
            if (sqlMapping == null)
            {
                // Field of candidate, so use candidate
                sqlMapping = getSQLTableMappingForAlias(candidateAlias);
                primaryName = candidateAlias;
            }
        }

        AbstractClassMetaData cmd = sqlMapping.cmd;
        JavaTypeMapping mapping = sqlMapping.mapping;
        if (mapping instanceof MapMapping && compileComponent != CompilationComponent.FROM && iter.hasNext())
        {
            // JPQL allows people to refer to a "map" field as if it is the value of the map!
            NucleusLogger.QUERY.debug("Processing PrimaryExpression " + StringUtils.collectionToString(tuples) + " yet the first component is a map field! You should use VALUE(map)!");
            if (hasSQLTableMappingForAlias(first + MAP_VALUE_ALIAS_SUFFIX))
            {
                sqlMapping = getSQLTableMappingForAlias(first + MAP_VALUE_ALIAS_SUFFIX);
                cmd = sqlMapping.cmd;
            }
            else
            {
                String mapValueAlias = first + MAP_VALUE_ALIAS_SUFFIX;
                if (options.contains(OPTION_CASE_INSENSITIVE))
                {
                    mapValueAlias = mapValueAlias.toUpperCase();
                }
                SQLTable sqlTblValue = stmt.getTable(mapValueAlias);
                if (sqlTblValue != null)
                {
                    AbstractClassMetaData valueCmd = mapping.getMemberMetaData().getMap().getValueClassMetaData(clr, ec.getMetaDataManager());
                    sqlMapping = new SQLTableMapping(sqlTblValue, valueCmd, sqlTblValue.getTable().getIdMapping());
                    cmd = valueCmd;
                }
            }
        }

        while (iter.hasNext())
        {
            String component = iter.next();
            primaryName += "." + component; // fully-qualified primary name

            // Derive SQLTableMapping for this component
            SQLTableMapping sqlMappingNew = getSQLTableMappingForAlias(primaryName);
            if (sqlMappingNew == null)
            {
                // Table not present for this primary
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(component);
                if (mmd == null)
                {
                    // Not valid member name
                    throw new NucleusUserException(Localiser.msg("021062", component, cmd.getFullClassName()));
                }
                else if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
                {
                    throw new NucleusUserException("Field "+ mmd.getFullFieldName() + " is not marked as persistent so cannot be queried");
                }

                // Find the table and the mapping for this field in the table
                SQLTable sqlTbl = null;
                if (mapping instanceof EmbeddedMapping)
                {
                    // Embedded into the current table
                    sqlTbl = sqlMapping.table;
                    mapping = ((EmbeddedMapping)mapping).getJavaTypeMapping(component);
                }
                else if (mapping instanceof PersistableMapping && cmd.isEmbeddedOnly())
                {
                    // JPA EmbeddedId into current table
                    sqlTbl = sqlMapping.table;
                    JavaTypeMapping[] subMappings = ((PersistableMapping)mapping).getJavaTypeMapping();
                    if (subMappings.length == 1 && subMappings[0] instanceof EmbeddedPCMapping)
                    {
                        mapping = ((EmbeddedPCMapping)subMappings[0]).getJavaTypeMapping(component);
                    }
                    else
                    {
                        // TODO What situation is this?
                    }
                }
                else
                {
                    DatastoreClass table = storeMgr.getDatastoreClass(cmd.getFullClassName(), clr);
                    if (table == null)
                    {
                        AbstractClassMetaData[] subCmds = storeMgr.getClassesManagingTableForClass(cmd, clr);
                        if (subCmds.length == 1)
                        {
                            table = storeMgr.getDatastoreClass(subCmds[0].getFullClassName(), clr);
                        }
                        else
                        {
                            // TODO Support this situation - select of base class with no table of its own
                            // and multiple subclasses so primary statement is a UNION hence need to process
                            // all of UNIONs, and this primary expression refers to a mapping in each of subclass tables
                            throw new NucleusUserException("Unable to find table for primary " + primaryName +
                                " since the class " + cmd.getFullClassName() + " is managed in multiple tables");
                        }
                    }
                    if (table == null)
                    {
                        throw new NucleusUserException("Unable to find table for primary " + primaryName + " table for class=" + cmd.getFullClassName() + " is null : is the field correct? or using some inheritance pattern?");
                    }
                    mapping = table.getMemberMapping(mmd);
                    sqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(theStmt, sqlMapping.table, mapping);
                }

                RelationType relationType = mmd.getRelationType(clr);
                if (relationType == RelationType.NONE)
                {
                    sqlMappingNew = new SQLTableMapping(sqlTbl, cmd, mapping);
                    cmd = sqlMappingNew.cmd;
                    setSQLTableMappingForAlias(primaryName, sqlMappingNew);
                }
                else if (relationType == RelationType.ONE_TO_ONE_UNI || relationType == RelationType.ONE_TO_ONE_BI)
                {
                    if (mmd.getMappedBy() != null)
                    {
                        // FK in other table so join to that first
                        AbstractMemberMetaData relMmd = mmd.getRelatedMemberMetaData(clr)[0];
                        if (relMmd.getAbstractClassMetaData().isEmbeddedOnly())
                        {
                            // Member is embedded, so keep same SQL table mapping
                            sqlMappingNew = sqlMapping;
                            cmd = relMmd.getAbstractClassMetaData();
                        }
                        else
                        {
                            // Member is in own table, so move to that SQL table mapping
                            DatastoreClass relTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
                            JavaTypeMapping relMapping = relTable.getMemberMapping(relMmd);
                            // Join to related table unless we already have the join in place
                            sqlTbl = theStmt.getTable(relTable, primaryName);
                            if (sqlTbl == null)
                            {
                                sqlTbl = SQLStatementHelper.addJoinForOneToOneRelation(theStmt, 
                                    sqlMapping.table.getTable().getIdMapping(), sqlMapping.table,
                                    relMapping, relTable, null, null, primaryName, defaultJoinType);
                            }

                            if (iter.hasNext())
                            {
                                sqlMappingNew = new SQLTableMapping(sqlTbl, relMmd.getAbstractClassMetaData(), relTable.getIdMapping());
                                cmd = sqlMappingNew.cmd;
                            }
                            else
                            {
                                sqlMappingNew = new SQLTableMapping(sqlTbl, cmd, relTable.getIdMapping());
                                cmd = sqlMappingNew.cmd;
                            }
                        }
                    }
                    else
                    {
                        // FK is at this side, so only join if further component provided, or if forcing
                        if (forceJoin == null && !iter.hasNext())
                        {
                            if (primExpr.getParent() != null && primExpr.getParent().getOperator() == Expression.OP_CAST)
                            {
                                // Cast and not an interface field, so do a join to the table of the persistable object
                                if (mapping instanceof ReferenceMapping)
                                {
                                    // Don't join with interface field since represents multiple implementations
                                    // and the cast will be a restrict on which implementation to join to
                                }
                                else
                                {
                                    AbstractClassMetaData relCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                                    if (relCmd != null && !relCmd.isEmbeddedOnly())
                                    {
                                        DatastoreClass relTable = storeMgr.getDatastoreClass(relCmd.getFullClassName(), clr);
                                        if (relTable == null)
                                        {
                                        }
                                        else
                                        {
                                            forceJoin = Boolean.TRUE;
                                        }
                                    }
                                    else
                                    {
                                        forceJoin = Boolean.TRUE;
                                    }
                                }
                            }
                        }

                        // TODO If the only remaining component of the PrimaryExpression is the "id" and not persistable itself then don't join.
                        if (iter.hasNext() || Boolean.TRUE.equals(forceJoin))
                        {
                            AbstractClassMetaData relCmd = null;
                            JavaTypeMapping relMapping = null;
                            DatastoreClass relTable = null;
                            if (relationType == RelationType.ONE_TO_ONE_BI)
                            {
                                AbstractMemberMetaData relMmd = mmd.getRelatedMemberMetaData(clr)[0];
                                relCmd = relMmd.getAbstractClassMetaData();
                            }
                            else
                            {
                                String typeName = mmd.isSingleCollection() ? mmd.getCollection().getElementType() : mmd.getTypeName();
                                relCmd = ec.getMetaDataManager().getMetaDataForClass(typeName, clr);
                            }

                            if (relCmd != null && relCmd.isEmbeddedOnly())
                            {
                                // Member is embedded so use same table but embedded mapping
                                sqlMappingNew = new SQLTableMapping(sqlTbl, relCmd, mapping);
                                cmd = relCmd;
                            }
                            else
                            {
                                // Member is in own table, so move to that SQL table mapping
                                relTable = storeMgr.getDatastoreClass(relCmd.getFullClassName(), clr);
                                if (relTable == null)
                                {
                                    // No table for the related type (subclass-table), so see if this class has a single subclass with its own table
                                    Collection<String> relSubclassNames = storeMgr.getSubClassesForClass(relCmd.getFullClassName(), false, clr);
                                    if (relSubclassNames != null && relSubclassNames.size() == 1)
                                    {
                                        String relSubclassName = relSubclassNames.iterator().next();
                                        relTable = storeMgr.getDatastoreClass(relSubclassName, clr);
                                        // TODO Cater for this having no table and next level yes etc
                                        if (relTable != null)
                                        {
                                            relCmd = ec.getMetaDataManager().getMetaDataForClass(relSubclassName, clr);
                                        }
                                    }
                                    if (relTable == null)
                                    {
                                        // No table as such, so likely using subclass-table at other side and we don't know where to join to
                                        throw new NucleusUserException("Reference to PrimaryExpression " + primExpr + " yet this needs to join relation " + mmd.getFullFieldName() + 
                                                " and the other type has no table (subclass-table?). Maybe use a CAST to the appropriate subclass?");
                                    }
                                }
                                relMapping = relTable.getIdMapping();

                                // Join to other table unless we already have the join in place
                                sqlTbl = theStmt.getTable(relTable, primaryName);
                                if (sqlTbl == null)
                                {
                                    sqlTbl = SQLStatementHelper.addJoinForOneToOneRelation(theStmt, mapping, 
                                        sqlMapping.table, relMapping, relTable, null, null, primaryName, defaultJoinType);
                                }

                                sqlMappingNew = new SQLTableMapping(sqlTbl, relCmd, relMapping);
                                cmd = sqlMappingNew.cmd;
                                setSQLTableMappingForAlias(primaryName, sqlMappingNew);
                            }
                        }
                        else
                        {
                            sqlMappingNew = new SQLTableMapping(sqlTbl, cmd, mapping);
                            cmd = sqlMappingNew.cmd;
                            // Don't register the SQLTableMapping for this alias since only using FK
                        }
                    }
                }
                else if (relationType == RelationType.MANY_TO_ONE_BI)
                {
                    AbstractMemberMetaData relMmd = mmd.getRelatedMemberMetaData(clr)[0];
                    DatastoreClass relTable = storeMgr.getDatastoreClass(mmd.getTypeName(), clr);
                    if (mmd.getJoinMetaData() != null || relMmd.getJoinMetaData() != null)
                    {
                        // Has join table so use that
                        sqlTbl = theStmt.getTable(relTable, primaryName);
                        if (sqlTbl == null)
                        {
                            // Join to the join table
                            CollectionTable joinTbl = (CollectionTable)storeMgr.getTable(relMmd);
                            if (defaultJoinType == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.INNER_JOIN)
                            {
                                SQLTable joinSqlTbl = theStmt.join(JoinType.INNER_JOIN, sqlMapping.table, sqlMapping.table.getTable().getIdMapping(), joinTbl, null,
                                    joinTbl.getElementMapping(), null, null, true);
                                sqlTbl = theStmt.join(JoinType.INNER_JOIN, joinSqlTbl, joinTbl.getOwnerMapping(), relTable, null, relTable.getIdMapping(), null, primaryName, true);
                            }
                            else if (defaultJoinType == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.LEFT_OUTER_JOIN || defaultJoinType == null)
                            {
                                SQLTable joinSqlTbl = theStmt.join(JoinType.LEFT_OUTER_JOIN, sqlMapping.table, sqlMapping.table.getTable().getIdMapping(), joinTbl, null,
                                    joinTbl.getElementMapping(), null, null, true);
                                sqlTbl = theStmt.join(JoinType.LEFT_OUTER_JOIN, joinSqlTbl, joinTbl.getOwnerMapping(), relTable, null, relTable.getIdMapping(), null, primaryName, true);
                            }
                        }

                        sqlMappingNew = new SQLTableMapping(sqlTbl, relMmd.getAbstractClassMetaData(), relTable.getIdMapping());
                        cmd = sqlMappingNew.cmd;
                        setSQLTableMappingForAlias(primaryName, sqlMappingNew);
                    }
                    else
                    {
                        // FK in this table
                        sqlTbl = theStmt.getTable(relTable, primaryName);
                        if (sqlTbl == null)
                        {
                            Operator op = (primExpr.getParent() != null ? primExpr.getParent().getOperator() : null);
                            if (!iter.hasNext() && (op == Expression.OP_EQ || op == Expression.OP_GT || op == Expression.OP_LT || op == Expression.OP_GTEQ || 
                                    op == Expression.OP_LTEQ || op == Expression.OP_NOTEQ))
                            {
                                // Just return the FK mapping since in a "a.b == c.d" type expression and not needing
                                // to go further than the FK
                                sqlMappingNew = new SQLTableMapping(sqlMapping.table, relMmd.getAbstractClassMetaData(), mapping);
                            }
                            else
                            {
                                // Join to the related table
                                if (defaultJoinType == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.INNER_JOIN)
                                {
                                    sqlTbl = theStmt.join(JoinType.INNER_JOIN, sqlMapping.table, mapping, relTable, null, relTable.getIdMapping(), null, primaryName, true);
                                }
                                else if (defaultJoinType == org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.LEFT_OUTER_JOIN || defaultJoinType == null)
                                {
                                    sqlTbl = theStmt.join(JoinType.LEFT_OUTER_JOIN, sqlMapping.table, mapping, relTable, null, relTable.getIdMapping(), null, primaryName, true);
                                }
                                sqlMappingNew = new SQLTableMapping(sqlTbl, relMmd.getAbstractClassMetaData(), relTable.getIdMapping());
                                cmd = sqlMappingNew.cmd;
                                setSQLTableMappingForAlias(primaryName, sqlMappingNew);
                            }
                        }
                        else
                        {
                            sqlMappingNew = new SQLTableMapping(sqlTbl, relMmd.getAbstractClassMetaData(), relTable.getIdMapping());
                            cmd = sqlMappingNew.cmd;
                            setSQLTableMappingForAlias(primaryName, sqlMappingNew);
                        }
                    }
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    // Can't reference further than a collection/map so just return its mapping here
                    sqlMappingNew = new SQLTableMapping(sqlTbl, cmd, mapping);
                    cmd = sqlMappingNew.cmd;
                    setSQLTableMappingForAlias(primaryName, sqlMappingNew);
                }
            }
            else
            {
                cmd = sqlMappingNew.cmd;
            }

            sqlMapping = sqlMappingNew;
        }

        return sqlMapping;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processParameterExpression(org.datanucleus.query.expression.ParameterExpression)
     */
    @Override
    protected Object processParameterExpression(ParameterExpression expr)
    {
        return processParameterExpression(expr, false);
    }

    /**
     * Method to process a parameter expression. The optional argument controls whether we should
     * create this as a parameter or as a literal (i.e the param value is known etc).
     * If the parameter doesn't have its value defined then returns ParameterLiteral
     * otherwise we get an XXXLiteral of the (declared) type of the parameter
     * @param expr The ParameterExpression
     * @param asLiteral Whether to create a SQLLiteral rather than a parameter literal
     * @return The processed expression
     */
    protected Object processParameterExpression(ParameterExpression expr, boolean asLiteral)
    {
        if (compileComponent == CompilationComponent.ORDERING || compileComponent == CompilationComponent.RESULT)
        {
            // All JDBC drivers I know don't allow parameters in the order-by clause
            // Note that we also don't allow parameters in result clause since SQLStatement squashes all SELECT expression to a String so losing info about params
            asLiteral = true;
        }

        if (expr.getPosition() >= 0)
        {
            if (paramNameByPosition == null)
            {
                paramNameByPosition = new HashMap<Integer, String>();
            }
            paramNameByPosition.put(Integer.valueOf(expr.getPosition()), expr.getId());
        }

        // Find the parameter value if supplied
        Object paramValue = null;
        boolean paramValueSet = false;
        if (parameters != null && parameters.size() > 0)
        {
            // Check if the parameter has a value
            if (parameters.containsKey(expr.getId()))
            {
                // Named parameter
                paramValue = parameters.get(expr.getId());
                paramValueSet = true;
            }
            else if (parameterValueByName != null && parameterValueByName.containsKey(expr.getId()))
            {
                // Positional parameter, but already encountered
                paramValue = parameterValueByName.get(expr.getId());
                paramValueSet = true;
            }
            else
            {
                // Positional parameter, not yet encountered
                int position = positionalParamNumber;
                if (positionalParamNumber < 0)
                {
                    position = 0;
                }
                if (parameters.containsKey(Integer.valueOf(position)))
                {
                    paramValue = parameters.get(Integer.valueOf(position));
                    paramValueSet = true;
                    positionalParamNumber = position+1;
                    if (parameterValueByName == null)
                    {
                        parameterValueByName = new HashMap<String, Object>();
                    }
                    parameterValueByName.put(expr.getId(), paramValue);
                }
            }
        }

        // Find the type to use for the parameter
        JavaTypeMapping m = paramMappingForName.get(expr.getId());
        if (m == null)
        {
            // Try to determine from provided parameter value or from symbol table (declared type)
            if (paramValue != null)
            {
                String className = storeMgr.getClassNameForObjectID(paramValue, clr, ec);
                if (className != null)
                {
                    // Identity for persistable class
                    AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                    if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        Class cls = clr.classForName(className);
                        m = exprFactory.getMappingForType(cls, false);
                        m = new PersistableIdMapping((PersistableMapping) m);
                    }
                }

                if (m == null)
                {
                    // Use the type of the input parameter value
                    try
                    {
                        m = exprFactory.getMappingForType(paramValue.getClass(), false);
                    }
                    catch (NucleusUserException nue)
                    {
                        // Maybe it needs a TypeConverter so try with the (declared) symbol type of this parameter
                        m = exprFactory.getMappingForType(expr.getSymbol().getValueType(), false);
                    }
                }
                if (expr.getSymbol() != null && expr.getSymbol().getValueType() != null)
                {
                    if (!QueryUtils.queryParameterTypesAreCompatible(expr.getSymbol().getValueType(), paramValue.getClass()))
                    {
                        throw new QueryCompilerSyntaxException(Localiser.msg("021118", expr.getId(), expr.getSymbol().getValueType().getName(), paramValue.getClass().getName()));
                    }
                    if (expr.getSymbol().getValueType() != paramValue.getClass())
                    {
                        // Mark as not precompilable since the supplied type implies a subclass of the declared type
                        setNotPrecompilable();
                    }
                }
            }
            else if (expr.getSymbol() != null && expr.getSymbol().getValueType() != null)
            {
                Class valueType = expr.getSymbol().getValueType();
                if (!paramValueSet)
                {
                    if (valueType.isInterface())
                    {
                        // Special case where we have an interface parameter (not set), and don't know the type, so we pick the first implementation just to get something that works
                        // This is recompiled when the parameter is provided so is just for use in "compile()"
                        String[] implNames = storeMgr.getMetaDataManager().getClassesImplementingInterface(valueType.getName(), clr);
                        if (implNames != null && implNames.length > 0)
                        {
                            valueType = clr.classForName(implNames[0]);
                            setNotPrecompilable();
                        }
                    }
                }

                // Use the declared type of the parameter (explicit params)
                m = exprFactory.getMappingForType(valueType, false);
            }
        }

        if (asLiteral && m != null && !m.representableAsStringLiteralInStatement())
        {
            // Must keep this as a parameter since its String form is no good in statements
            asLiteral = false;
        }

        if (asLiteral)
        {
            // Parameter being represented as a literal (for whatever reason), so no longer precompilable
            if (isPrecompilable())
            {
                NucleusLogger.QUERY.debug("Parameter " + expr + " is being resolved as a literal, so the query is no longer precompilable");
            }
            setNotPrecompilable();
        } 
        else if (paramValue == null && expr.getSymbol() != null)
        {
            if (isPrecompilable())
            {
                NucleusLogger.QUERY.debug("Parameter " + expr + " is set to null so this has to be resolved as a NullLiteral, and the query is no longer precompilable");
            }
            setNotPrecompilable();
        }

        // Create the SQLExpression for this parameter, either as value-literal or as parameter-literal
        SQLExpression sqlExpr = null;
        boolean nullParamValueUsesIsNull = options.contains(OPTION_NULL_PARAM_USE_IS_NULL);

        if (paramValueSet && paramValue == null && nullParamValueUsesIsNull)
        {
            // Value is set to null, but we enforce a NullLiteral for the case of null comparisons e.g we don't want "field = ?", but instead "field IS NULL" 
            sqlExpr = exprFactory.newLiteral(stmt, null, null);
        }
        else if (asLiteral)
        {
            // Create a value-literal as requested
            sqlExpr = exprFactory.newLiteral(stmt, m, paramValue);
        }
        else
        {
            // Create a parameter-literal with it tied to the parameter name for later replacement in the statement
            sqlExpr = exprFactory.newLiteralParameter(stmt, m, paramValue, expr.getId());
            if (sqlExpr instanceof ParameterLiteral)
            {
                ((ParameterLiteral)sqlExpr).setName(expr.getId());
            }

            if (expressionForParameter == null)
            {
                expressionForParameter = new HashMap<Object, SQLExpression>();
            }
            expressionForParameter.put(expr.getId(), sqlExpr);

            paramMappingForName.put(expr.getId(), m);
        }

        stack.push(sqlExpr);
        return sqlExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processInvokeExpression(org.datanucleus.query.expression.InvokeExpression)
     */
    protected Object processInvokeExpression(InvokeExpression expr)
    {
        // Find object that we invoke on
        Expression invokedExpr = expr.getLeft();
        SQLExpression invokedSqlExpr = null;
        if (invokedExpr == null)
        {
            // Static method
        }
        else if (invokedExpr instanceof PrimaryExpression)
        {
            processPrimaryExpression((PrimaryExpression)invokedExpr);
            invokedSqlExpr = stack.pop();
        }
        else if (invokedExpr instanceof Literal)
        {
            processLiteral((Literal)invokedExpr);
            invokedSqlExpr = stack.pop();
        }
        else if (invokedExpr instanceof ParameterExpression)
        {
            // TODO May be needed to set the second parameter to "false" here and then if the method
            // being invoked needs the parameters as a normal SQLLiteral then allow it to convert it itself
            processParameterExpression((ParameterExpression)invokedExpr, true);
            invokedSqlExpr = stack.pop();
        }
        else if (invokedExpr instanceof InvokeExpression)
        {
            processInvokeExpression((InvokeExpression)invokedExpr);
            invokedSqlExpr = stack.pop();
        }
        else if (invokedExpr instanceof VariableExpression)
        {
            processVariableExpression((VariableExpression)invokedExpr);
            invokedSqlExpr = stack.pop();
        }
        else if (invokedExpr instanceof ArrayExpression)
        {
            ArrayExpression arrExpr = (ArrayExpression)invokedExpr;
            SQLExpression[] arrSqlExprs = new SQLExpression[arrExpr.getArraySize()];
            for (int i=0;i<arrExpr.getArraySize();i++)
            {
                Expression arrElemExpr = arrExpr.getElement(i);
                arrElemExpr.evaluate(this);
                arrSqlExprs[i] = stack.pop();
            }
            JavaTypeMapping m = exprFactory.getMappingForType(Object[].class, false);
            invokedSqlExpr = new org.datanucleus.store.rdbms.sql.expression.ArrayExpression(stmt, m, arrSqlExprs);
        }
        else if (invokedExpr instanceof DyadicExpression)
        {
            DyadicExpression dyExpr = (DyadicExpression)invokedExpr;
            dyExpr.evaluate(this);
            invokedSqlExpr = stack.pop();
        }
        else
        {
            throw new NucleusException("Dont currently support invoke expression " + invokedExpr);
        }

        if (invokedSqlExpr instanceof NullLiteral)
        {
            // We cannot invoke anything on a null TODO Handle this "NPE"
            NucleusLogger.QUERY.warn(">> Compilation of InvokeExpression needs to invoke method \"" + expr.getOperation() + "\" on " + invokedSqlExpr + " but not possible");
        }

        String operation = expr.getOperation();
        if (invokedSqlExpr instanceof MapExpression && operation.equals("contains") && compilation.getQueryLanguage().equalsIgnoreCase("JPQL"))
        {
            // JPQL "MEMBER OF" will be passed through from generic compilation as "contains" since we don't know types at that point
            operation = "containsValue";
        }

        // Process the arguments for invoking
        List args = expr.getArguments();
        List sqlExprArgs = null;
        if (args != null)
        {
            sqlExprArgs = new ArrayList<SQLExpression>();
            Iterator<Expression> iter = args.iterator();
            while (iter.hasNext())
            {
                Expression argExpr = iter.next();
                if (argExpr instanceof PrimaryExpression)
                {
                    processPrimaryExpression((PrimaryExpression)argExpr);
                    SQLExpression argSqlExpr = stack.pop();

                    if (compileComponent == CompilationComponent.RESULT && operation.equalsIgnoreCase("count") && stmt.getNumberOfTableGroups() > 1)
                    {
                        if (argSqlExpr.getSQLTable() == stmt.getPrimaryTable() && argSqlExpr.getJavaTypeMapping() == stmt.getPrimaryTable().getTable().getIdMapping())
                        {
                            // Result with "count(this)" and joins to other groups, so enforce distinct
                            argSqlExpr.distinct();
                        }
                    }
                    sqlExprArgs.add(argSqlExpr);
                }
                else if (argExpr instanceof ParameterExpression)
                {
                    processParameterExpression((ParameterExpression)argExpr);
                    sqlExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof InvokeExpression)
                {
                    processInvokeExpression((InvokeExpression)argExpr);
                    sqlExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof Literal)
                {
                    processLiteral((Literal)argExpr);
                    sqlExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof DyadicExpression)
                {
                    // Evaluate using this evaluator
                    argExpr.evaluate(this);
                    sqlExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof VariableExpression)
                {
                    processVariableExpression((VariableExpression)argExpr);
                    sqlExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof CaseExpression)
                {
                    processCaseExpression((CaseExpression)argExpr);
                    sqlExprArgs.add(stack.pop());
                }
                else
                {
                    throw new NucleusException("Dont currently support invoke expression argument " + argExpr);
                }
            }

            if (operation.equals("INDEX"))
            {
                // Special case of index expression
                List<Expression> indexArgs = expr.getArguments();
                if (indexArgs == null || indexArgs.size() > 1)
                {
                    throw new NucleusException("Can only use INDEX with single argument");
                }

                PrimaryExpression indexExpr = (PrimaryExpression)indexArgs.get(0);
                String joinAlias = indexExpr.getId();
                String collExprName = joinAlias;
                if (explicitJoinPrimaryByAlias != null)
                {
                    collExprName = explicitJoinPrimaryByAlias.get(joinAlias);
                    if (collExprName == null)
                    {
                        throw new NucleusException("Unable to locate primary expression for alias " + joinAlias);
                    }
                }

                // Find an expression for the collection field
                List<String> tuples = new ArrayList<String>();
                StringTokenizer primTokenizer = new StringTokenizer(collExprName, ".");
                while (primTokenizer.hasMoreTokens())
                {
                    String token = primTokenizer.nextToken();
                    tuples.add(token);
                }
                PrimaryExpression collPrimExpr = new PrimaryExpression(tuples);
                processPrimaryExpression(collPrimExpr);
                SQLExpression collSqlExpr = stack.pop();
                sqlExprArgs.add(collSqlExpr);
            }
        }

        // Invoke the method
        SQLExpression sqlExpr = null;
        if (invokedSqlExpr != null)
        {
            sqlExpr = invokedSqlExpr.invoke(operation, sqlExprArgs);
        }
        else
        {
            sqlExpr = exprFactory.invokeMethod(stmt, null, operation, null, sqlExprArgs);
        }

        stack.push(sqlExpr);
        return sqlExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processSubqueryExpression(org.datanucleus.query.expression.SubqueryExpression)
     */
    @Override
    protected Object processSubqueryExpression(SubqueryExpression expr)
    {
        String keyword = expr.getKeyword();
        Expression subqueryExpr = expr.getRight();
        if (subqueryExpr instanceof VariableExpression)
        {
            processVariableExpression((VariableExpression)subqueryExpr);
            SQLExpression subquerySqlExpr = stack.pop();
            if (keyword != null && keyword.equals("EXISTS"))
            {
                // EXISTS expressions need to be Boolean
                if (subquerySqlExpr instanceof org.datanucleus.store.rdbms.sql.expression.SubqueryExpression)
                {
                    SQLStatement subStmt = ((org.datanucleus.store.rdbms.sql.expression.SubqueryExpression)subquerySqlExpr).getSubqueryStatement();
                    subquerySqlExpr = new BooleanSubqueryExpression(stmt, keyword, subStmt);
                }
                else
                {
                    SQLStatement subStmt = ((SubqueryExpressionComponent)subquerySqlExpr).getSubqueryStatement();
                    subquerySqlExpr = new BooleanSubqueryExpression(stmt, keyword, subStmt);
                }
            }
            else if (subquerySqlExpr instanceof org.datanucleus.store.rdbms.sql.expression.SubqueryExpression)
            {
                SQLStatement subStmt = ((org.datanucleus.store.rdbms.sql.expression.SubqueryExpression)subquerySqlExpr).getSubqueryStatement();
                subquerySqlExpr = new BooleanSubqueryExpression(stmt, keyword, subStmt);
            }
            else if (subquerySqlExpr instanceof NumericSubqueryExpression)
            {
                if ((keyword.equalsIgnoreCase("SOME") || keyword.equalsIgnoreCase("ALL") || keyword.equalsIgnoreCase("ANY")) &&
                    !storeMgr.getDatastoreAdapter().supportsOption(DatastoreAdapter.SOME_ANY_ALL_SUBQUERY_EXPRESSIONS))
                {
                    throw new NucleusException("'SOME|ALL|ANY{subquery}' is not supported by this datastore");
                }

                // Apply keyword (e.g ALL, SOME, ANY) to numeric expressions
                ((NumericSubqueryExpression)subquerySqlExpr).setKeyword(keyword);
            }
            stack.push(subquerySqlExpr);
            return subquerySqlExpr;
        }

        throw new NucleusException("Dont currently support SubqueryExpression " + keyword + " for type " + subqueryExpr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processAddExpression(org.datanucleus.query.expression.Expression)
     */
    protected Object processAddExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        SQLExpression resultExpr = left.add(right);
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processDivExpression(org.datanucleus.query.expression.Expression)
     */
    protected Object processDivExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        SQLExpression resultExpr = left.div(right);
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processMulExpression(org.datanucleus.query.expression.Expression)
     */
    protected Object processMulExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        SQLExpression resultExpr = left.mul(right);
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processSubExpression(org.datanucleus.query.expression.Expression)
     */
    protected Object processSubExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (left instanceof ParameterLiteral && !(right instanceof ParameterLiteral))
        {
            left = replaceParameterLiteral((ParameterLiteral)left, right.getJavaTypeMapping());
        }
        else if (right instanceof ParameterLiteral && !(left instanceof ParameterLiteral))
        {
            right = replaceParameterLiteral((ParameterLiteral)right, left.getJavaTypeMapping());
        }

        SQLExpression resultExpr = left.sub(right);
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processDistinctExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processDistinctExpression(Expression expr)
    {
        SQLExpression sqlExpr = stack.pop();
        sqlExpr.distinct();
        stack.push(sqlExpr);
        return sqlExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processComExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processComExpression(Expression expr)
    {
        // Bitwise complement - only for integer values
        SQLExpression sqlExpr = stack.pop();
        SQLExpression resultExpr = sqlExpr.com();
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processModExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processModExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        SQLExpression resultExpr = left.mod(right);
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNegExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNegExpression(Expression expr)
    {
        SQLExpression sqlExpr = stack.pop();
        SQLExpression resultExpr = sqlExpr.neg();
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNotExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNotExpression(Expression expr)
    {
        // Logical complement - only for boolean values
        SQLExpression sqlExpr = stack.pop();
        SQLExpression resultExpr = sqlExpr.not();
        stack.push(resultExpr);
        return resultExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processCastExpression(org.datanucleus.query.expression.CastExpression)
     */
    @Override
    protected Object processCastExpression(Expression expr)
    {
        SQLExpression right = stack.pop(); // The cast literal
        SQLExpression left = stack.pop(); // The expression to cast
        SQLExpression castExpr = left.cast(right);
        stack.push(castExpr);
        return castExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processCaseExpression(org.datanucleus.query.expression.CaseExpression)
     */
    @Override
    protected Object processCaseExpression(CaseExpression expr)
    {
        boolean numericCase = false;
        boolean booleanCase = false;
        boolean stringCase = false;
        List<ExpressionPair> conditions = expr.getConditions();
        Iterator<ExpressionPair> whenExprIter = conditions.iterator();
        SQLExpression[] whenSqlExprs = new SQLExpression[conditions.size()];
        SQLExpression[] actionSqlExprs = new SQLExpression[conditions.size()];
        int i = 0;
        while (whenExprIter.hasNext())
        {
            ExpressionPair pair = whenExprIter.next();
            Expression whenExpr = pair.getWhenExpression();
            whenExpr.evaluate(this);
            whenSqlExprs[i] = stack.pop();
            if (!(whenSqlExprs[i] instanceof BooleanExpression))
            {
                throw new QueryCompilerSyntaxException("IF/ELSE conditional expression should return boolean but doesn't : " + expr);
            }

            Expression actionExpr = pair.getActionExpression();
            actionExpr.evaluate(this);
            actionSqlExprs[i] = stack.pop();
            if (actionSqlExprs[i] instanceof NumericExpression)
            {
                numericCase = true;
            }
            else if (actionSqlExprs[i] instanceof BooleanExpression)
            {
                booleanCase = true;
            }
            else if (actionSqlExprs[i] instanceof StringExpression)
            {
                stringCase = true;
            }

            i++;
        }

        Expression elseExpr = expr.getElseExpression();
        elseExpr.evaluate(this);
        SQLExpression elseActionSqlExpr = stack.pop();

        // Check that all action sql expressions are consistent
        for (int j=1;j<actionSqlExprs.length;j++)
        {
            if (!checkCaseExpressionsConsistent(actionSqlExprs[0], actionSqlExprs[j]))
            {
                throw new QueryCompilerSyntaxException("IF/ELSE action expression " + actionSqlExprs[j] + " is of different type to first action " + actionSqlExprs[0] + " - must be consistent");
            }
        }
        if (!checkCaseExpressionsConsistent(actionSqlExprs[0], elseActionSqlExpr))
        {
            throw new QueryCompilerSyntaxException("IF/ELSE action expression " + elseActionSqlExpr + " is of different type to first action " + actionSqlExprs[0] + " - must be consistent");
        }

        SQLExpression caseSqlExpr = null;
        if (numericCase)
        {
            caseSqlExpr = new org.datanucleus.store.rdbms.sql.expression.CaseNumericExpression(whenSqlExprs, actionSqlExprs, elseActionSqlExpr);
        }
        else if (booleanCase)
        {
            caseSqlExpr = new org.datanucleus.store.rdbms.sql.expression.CaseBooleanExpression(whenSqlExprs, actionSqlExprs, elseActionSqlExpr);
        }
        else if (stringCase)
        {
            caseSqlExpr = new org.datanucleus.store.rdbms.sql.expression.CaseStringExpression(whenSqlExprs, actionSqlExprs, elseActionSqlExpr);
        }
        else
        {
            caseSqlExpr = new org.datanucleus.store.rdbms.sql.expression.CaseExpression(whenSqlExprs, actionSqlExprs, elseActionSqlExpr);
        }
        stack.push(caseSqlExpr);
        return caseSqlExpr;
    }

    private boolean checkCaseExpressionsConsistent(SQLExpression expr1, SQLExpression expr2)
    {
        if (expr1 instanceof NumericExpression && expr2 instanceof NumericExpression)
        {
            return true;
        }
        else if (expr1 instanceof StringExpression && expr2 instanceof StringExpression)
        {
            return true;
        }
        else if (expr1 instanceof BooleanExpression && expr2 instanceof BooleanExpression)
        {
            return true;
        }
        else if (expr1 instanceof TemporalExpression && expr2 instanceof TemporalExpression)
        {
            return true;
        }

        if (expr1.getClass().isAssignableFrom(expr2.getClass()) || expr2.getClass().isAssignableFrom(expr1.getClass()))
        {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processIsExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processIsExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        SQLExpression instanceofExpr = left.is(right, false);
        stack.push(instanceofExpr);
        return instanceofExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processIsnotExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processIsnotExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        SQLExpression instanceofExpr = left.is(right, true);
        stack.push(instanceofExpr);
        return instanceofExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processInExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processInExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (right.getParameterName() != null)
        {
            setNotPrecompilable();
            if (right instanceof CollectionLiteral)
            {
                // Use Collection.contains(element)
                List<SQLExpression> sqlExprArgs = new ArrayList();
                sqlExprArgs.add(left);
                SQLExpression sqlExpr = right.invoke("contains", sqlExprArgs);
                stack.push(sqlExpr);
                return sqlExpr;
            }

            // Single valued parameter, so use equality
            SQLExpression inExpr = new BooleanExpression(left, Expression.OP_EQ, right);
            stack.push(inExpr);
            return inExpr;
        }

        SQLExpression inExpr = left.in(right, false);
        stack.push(inExpr);
        return inExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNotInExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNotInExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        if (right.getParameterName() != null)
        {
            setNotPrecompilable();
            if (right instanceof CollectionLiteral)
            {
                // Use !Collection.contains(element)
                List<SQLExpression> sqlExprArgs = new ArrayList();
                sqlExprArgs.add(left);
                SQLExpression sqlExpr = right.invoke("contains", sqlExprArgs);
                sqlExpr.not();
                stack.push(sqlExpr);
                return sqlExpr;
            }

            // Single valued parameter, so use equality
            SQLExpression inExpr = new BooleanExpression(left, Expression.OP_NOTEQ, right);
            stack.push(inExpr);
            return inExpr;
        }

        SQLExpression inExpr = left.in(right, true);
        stack.push(inExpr);
        return inExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processCreatorExpression(org.datanucleus.query.expression.CreatorExpression)
     */
    @Override
    protected Object processCreatorExpression(CreatorExpression expr)
    {
        String className = expr.getId();
        Class cls = null;
        try
        {
            cls = clr.classForName(className);
        }
        catch (ClassNotResolvedException cnre)
        {
            if (importsDefinition != null)
            {
                cls = importsDefinition.resolveClassDeclaration(className, clr, null);
            }
        }

        List<SQLExpression> ctrArgExprs = null;
        List args = expr.getArguments();
        if (args != null)
        {
            Class[] ctrArgTypes = new Class[args.size()];
            ctrArgExprs = new ArrayList<SQLExpression>(args.size());
            Iterator iter = args.iterator();
            int i = 0;
            while (iter.hasNext())
            {
                Expression argExpr = (Expression)iter.next();
                SQLExpression sqlExpr = (SQLExpression)evaluate(argExpr);
                ctrArgExprs.add(sqlExpr);
                if (sqlExpr instanceof NewObjectExpression)
                {
                    ctrArgTypes[i] = ((NewObjectExpression)sqlExpr).getNewClass();
                }
                else if (sqlExpr.getJavaTypeMapping() instanceof DatastoreIdMapping || sqlExpr.getJavaTypeMapping() instanceof PersistableMapping)
                {
                    ctrArgTypes[i] = clr.classForName(sqlExpr.getJavaTypeMapping().getType());
                }
                else
                {
                    ctrArgTypes[i] = sqlExpr.getJavaTypeMapping().getJavaType();
                }
                i++;
            }

            // Check that this class has the required constructor
            Constructor ctr = ClassUtils.getConstructorWithArguments(cls, ctrArgTypes);
            if (ctr == null)
            {
                throw new NucleusUserException(Localiser.msg("021033", className, StringUtils.objectArrayToString(ctrArgTypes)));
            }
        }

        NewObjectExpression newExpr = new NewObjectExpression(stmt, cls, ctrArgExprs);
        stack.push(newExpr);
        return newExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLikeExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLikeExpression(Expression expr)
    {
        SQLExpression right = stack.pop();
        SQLExpression left = stack.pop();
        List args = new ArrayList();
        args.add(right);

        SQLExpression likeExpr = exprFactory.invokeMethod(stmt, String.class.getName(), "like", left, args);
        stack.push(likeExpr);
        return likeExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processVariableExpression(org.datanucleus.query.expression.VariableExpression)
     */
    @Override
    protected Object processVariableExpression(VariableExpression expr)
    {
        String varName = expr.getId();
        Symbol varSym = expr.getSymbol();
        if (varSym != null)
        {
            // Use name from symbol if possible
            varName = varSym.getQualifiedName();
        }

        if (hasSQLTableMappingForAlias(varName))
        {
            // Variable already found
            SQLTableMapping tblMapping = getSQLTableMappingForAlias(varName);
            SQLExpression sqlExpr = exprFactory.newExpression(tblMapping.table.getSQLStatement(), tblMapping.table, tblMapping.mapping);
            stack.push(sqlExpr);
            return sqlExpr;
        }
        else if (compilation.getCompilationForSubquery(varName) != null)
        {
            // Subquery variable
            QueryCompilation subCompilation = compilation.getCompilationForSubquery(varName);
            AbstractClassMetaData subCmd = ec.getMetaDataManager().getMetaDataForClass(subCompilation.getCandidateClass(), ec.getClassLoaderResolver());

            // Create subquery statement, using any provided alias if possible
            String subAlias = null;
            if (subCompilation.getCandidateAlias() != null && !subCompilation.getCandidateAlias().equals(candidateAlias))
            {
                subAlias = subCompilation.getCandidateAlias();
            }
            StatementResultMapping subqueryResultMapping = new StatementResultMapping();
            // TODO Fix "avg(something)" arg - not essential but is a hack right now
            SQLStatement subStmt = RDBMSQueryUtils.getStatementForCandidates(storeMgr, stmt, subCmd, null, ec, subCompilation.getCandidateClass(), true, "avg(something)", subAlias, null);

            QueryToSQLMapper sqlMapper = new QueryToSQLMapper(subStmt, subCompilation, parameters, null, subqueryResultMapping, subCmd, true, fetchPlan, ec, importsDefinition, options,
                extensionsByName);
            sqlMapper.setParentMapper(this);
            sqlMapper.compile();

            if (subqueryResultMapping.getNumberOfResultExpressions() > 1)
            {
                throw new NucleusUserException("Number of result expressions in subquery should be 1");
            }

            SQLExpression subExpr = null;
            // TODO Cater for subquery select of its own candidate
            if (subqueryResultMapping.getNumberOfResultExpressions() == 0)
            {
                subExpr = new org.datanucleus.store.rdbms.sql.expression.SubqueryExpression(stmt, subStmt);
            }
            else
            {
                JavaTypeMapping subMapping = ((StatementMappingIndex)subqueryResultMapping.getMappingForResultExpression(0)).getMapping();
                if (subMapping instanceof TemporalMapping)
                {
                    subExpr = new TemporalSubqueryExpression(stmt, subStmt);
                }
                else if (subMapping instanceof StringMapping)
                {
                    subExpr = new StringSubqueryExpression(stmt, subStmt);
                }
                else
                {
                    subExpr = new NumericSubqueryExpression(stmt, subStmt);
                }
                if (subExpr.getJavaTypeMapping() == null)
                {
                    subExpr.setJavaTypeMapping(subMapping);
                }
            }
            stack.push(subExpr);
            return subExpr;
        }
        else if (stmt.getParentStatement() != null && parentMapper != null && 
                parentMapper.candidateAlias != null && parentMapper.candidateAlias.equals(varName))
        {
            // Variable in subquery linking back to parent query
            SQLExpression varExpr = exprFactory.newExpression(stmt.getParentStatement(), stmt.getParentStatement().getPrimaryTable(),
                stmt.getParentStatement().getPrimaryTable().getTable().getIdMapping());
            stack.push(varExpr);
            return varExpr;
        }
        else
        {
            // Variable never met before, so return as UnboundExpression - process later if needing binding
            NucleusLogger.QUERY.debug("QueryToSQL.processVariable (unbound) variable=" + varName + " is not yet bound so returning UnboundExpression");
            UnboundExpression unbExpr = new UnboundExpression(stmt, varName);
            stack.push(unbExpr);
            return unbExpr;
        }
    }

    protected SQLExpression processUnboundExpression(UnboundExpression expr)
    {
        String varName = expr.getVariableName();
        Symbol varSym = compilation.getSymbolTable().getSymbol(varName);
        SQLExpression sqlExpr = bindVariable(expr, varSym.getValueType());
        if (sqlExpr != null)
        {
            stack.push(sqlExpr);
            return sqlExpr;
        }
        throw new NucleusUserException("Variable '" + varName + "' is unbound and cannot be determined (is it a misspelled field name? or is not intended to be a variable?)");
    }

    /**
     * Convenience method to return a parameter-based literal using the supplied mapping to replace
     * the provided ParameterLiteral (generated before its type was known).
     * @param paramLit The parameter literal
     * @param mapping Mapping to use
     * @return The replacement expression
     */
    protected SQLExpression replaceParameterLiteral(ParameterLiteral paramLit, JavaTypeMapping mapping)
    {
        return exprFactory.newLiteralParameter(stmt, mapping, paramLit.getValue(), paramLit.getParameterName());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.query.QueryGenerator#useParameterExpressionAsLiteral(org.datanucleus.store.rdbms.sql.expression.SQLLiteral)
     */
    public void useParameterExpressionAsLiteral(SQLLiteral paramLiteral)
    {
        paramLiteral.setNotParameter();
        setNotPrecompilable();
    }

    public boolean hasExtension(String key)
    {
        return (extensionsByName == null ? false : extensionsByName.containsKey(key));
    }

    public Object getValueForExtension(String key)
    {
        return (extensionsByName == null ? null : extensionsByName.get(key));
    }

    /**
     * Convenience method to return the required join type for the specified alias.
     * If the table has a join type defined as an extension "datanucleus.query.jdoql.{alias}.join"
     * then this returns the type. Otherwise returns null.
     * @param alias The alias
     * @return The join type required (if any)
     */
    public org.datanucleus.store.rdbms.sql.SQLJoin.JoinType getRequiredJoinTypeForAlias(String alias)
    {
        if (alias == null)
        {
            return null;
        }
        else if (alias.equals(candidateAlias))
        {
            return null;
        }

        String extensionName = "datanucleus.query.jdoql." + alias + ".join";
        org.datanucleus.store.rdbms.sql.SQLJoin.JoinType joinType = null;
        if (hasExtension(extensionName))
        {
            String joinValue = (String)getValueForExtension(extensionName);
            if (joinValue.equalsIgnoreCase("INNERJOIN"))
            {
                joinType = org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.INNER_JOIN;
            }
            else if (joinValue.equalsIgnoreCase("LEFTOUTERJOIN"))
            {
                joinType = org.datanucleus.store.rdbms.sql.SQLJoin.JoinType.LEFT_OUTER_JOIN;
            }
        }
        return joinType;
    }

    /**
     * Convenience method to return the value of a field of the supplied object.
     * If the object is null then returns null for the field.
     * @param obj The object
     * @param fieldName The field name
     * @return The field value
     */
    protected Object getValueForObjectField(Object obj, String fieldName)
    {
        if (obj != null)
        {
            Object paramFieldValue = null;
            if (ec.getApiAdapter().isPersistable(obj))
            {
                ObjectProvider paramSM = ec.findObjectProvider(obj);
                AbstractClassMetaData paramCmd = ec.getMetaDataManager().getMetaDataForClass(obj.getClass(), clr);
                AbstractMemberMetaData paramFieldMmd = paramCmd.getMetaDataForMember(fieldName);
                if (paramSM != null)
                {
                    paramSM.isLoaded(paramFieldMmd.getAbsoluteFieldNumber());
                    paramFieldValue = paramSM.provideField(paramFieldMmd.getAbsoluteFieldNumber());
                }
                else
                {
                    paramFieldValue = ClassUtils.getValueOfFieldByReflection(obj, fieldName);
                }
            }
            else
            {
                paramFieldValue = ClassUtils.getValueOfFieldByReflection(obj, fieldName);
            }

            return paramFieldValue;
        }
        return null;
    }

    protected SQLTableMapping getSQLTableMappingForAlias(String alias)
    {
        if (alias == null)
        {
            return null;
        }
        if (options.contains(OPTION_CASE_INSENSITIVE))
        {
            return sqlTableByPrimary.get(alias.toUpperCase());
        }
        return sqlTableByPrimary.get(alias);
    }

    public String getAliasForSQLTableMapping(SQLTableMapping tblMapping)
    {
        Iterator<Map.Entry<String, SQLTableMapping>> iter = sqlTableByPrimary.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, SQLTableMapping> entry = iter.next();
            if (entry.getValue() == tblMapping)
            {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Returns the alias used for the specified SQLTable.
     * If we have a variable "var1" and there is an access to a field also, then we may have entries
     * for "var1" and "var1.field". In this case we return the shortest.
     * @param tbl The table
     * @return The query name alias for this table
     */
    public String getAliasForSQLTable(SQLTable tbl)
    {
        Iterator<Map.Entry<String, SQLTableMapping>> iter = sqlTableByPrimary.entrySet().iterator();
        String alias = null;
        while (iter.hasNext())
        {
            Map.Entry<String, SQLTableMapping> entry = iter.next();
            if (entry.getValue().table == tbl)
            {
                // Take the shortest alias (since we could have "var", "var.field1" etc)
                if (alias == null)
                {
                    alias = entry.getKey();
                }
                else
                {
                    if (entry.getKey().length() < alias.length())
                    {
                        alias = entry.getKey();
                    }
                }
            }
        }
        return alias;
    }

    protected void setSQLTableMappingForAlias(String alias, SQLTableMapping mapping)
    {
        if (alias == null)
        {
            return;
        }
        if (options.contains(OPTION_CASE_INSENSITIVE))
        {
            sqlTableByPrimary.put(alias.toUpperCase(), mapping);
        }
        else
        {
            sqlTableByPrimary.put(alias, mapping);
        }
    }

    protected boolean hasSQLTableMappingForAlias(String alias)
    {
        if (options.contains(OPTION_CASE_INSENSITIVE))
        {
            return sqlTableByPrimary.containsKey(alias.toUpperCase());
        }
        return sqlTableByPrimary.containsKey(alias);
    }

    /**
     * Method to bind the specified variable to the table and mapping.
     * @param varName Variable name
     * @param cmd Metadata for this variable type
     * @param sqlTbl Table for this variable
     * @param mapping The mapping of this variable in the table
     */
    public void bindVariable(String varName, AbstractClassMetaData cmd, SQLTable sqlTbl, JavaTypeMapping mapping)
    {
        SQLTableMapping m = getSQLTableMappingForAlias(varName);
        if (m != null)
        {
            throw new NucleusException("Variable " + varName + " is already bound to " + m.table + " yet attempting to bind to " + sqlTbl);
        }

        NucleusLogger.QUERY.debug("QueryToSQL.bindVariable variable " + varName + " being bound to table=" + sqlTbl + " mapping=" + mapping);
        m = new SQLTableMapping(sqlTbl, cmd, mapping);
        setSQLTableMappingForAlias(varName, m);
    }

    /**
     * Method to bind the specified unbound variable (as cross join) on the assumption that the type
     * is a persistable class.
     * @param expr Unbound expression
     * @param type The type to bind as
     */
    public SQLExpression bindVariable(UnboundExpression expr, Class type)
    {
        String varName = expr.getVariableName();
        Symbol varSym = compilation.getSymbolTable().getSymbol(varName);
        if (varSym.getValueType() == null)
        {
            varSym.setValueType(type);
        }
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(type, clr);
        if (cmd != null)
        {
            // Variable is persistent type, so add cross join (may need changing later on in compilation)
            DatastoreClass varTable = storeMgr.getDatastoreClass(varSym.getValueType().getName(), clr);
            SQLTable varSqlTbl = stmt.crossJoin(varTable, "VAR_" + varName, null);
            SQLTableMapping varSqlTblMapping = new SQLTableMapping(varSqlTbl, cmd, varTable.getIdMapping());
            setSQLTableMappingForAlias(varName, varSqlTblMapping);
            return exprFactory.newExpression(stmt, varSqlTbl, varTable.getIdMapping());
        }
        return null;
    }

    /**
     * Method to bind the specified parameter to the defined type.
     * If the parameter is already bound (declared in the query perhaps, or bound via an earlier usage) then 
     * does nothing.
     * @param paramName Name of the parameter
     * @param type The type (or subclass)
     */
    public void bindParameter(String paramName, Class type)
    {
        Symbol paramSym = compilation.getSymbolTable().getSymbol(paramName);
        if (paramSym != null && paramSym.getValueType() == null)
        {
            paramSym.setValueType(type);
        }
    }

    /**
     * Accessor for the type of a variable if already known (declared?).
     * @param varName Name of the variable
     * @return The type if it is known
     */
    public Class getTypeOfVariable(String varName)
    {
        Symbol sym = compilation.getSymbolTable().getSymbol(varName);
        if (sym != null && sym.getValueType() != null)
        {
            return sym.getValueType();
        }
        return null;
    }

    /**
     * Accessor for whether the query has explicit variables.
     * If not then has implicit variables, meaning that they could potentially be rebound later
     * if prematurely bound in a particular way.
     * @return Whether the query has explicit variables
     */
    public boolean hasExplicitJoins()
    {
        return options.contains(OPTION_EXPLICIT_JOINS);
    }

    /**
     * Convenience method to return a boolean expression suitable for using in a filter.
     * Will return the input expression unless it is simply a reference to a field of a class
     * such as in a filter clause like <pre>myBoolField</pre>. In that case we return a boolean
     * like <pre>myBoolField == TRUE</pre>.
     * @param expr The expression to check
     * @return The expression valid for use in the filter
     */
    protected BooleanExpression getBooleanExpressionForUseInFilter(BooleanExpression expr)
    {
        if (compileComponent != CompilationComponent.FILTER)
        {
            return expr;
        }
        if (!expr.hasClosure())
        {
            // Add closure to the boolean expression
            return new BooleanExpression(expr, Expression.OP_EQ, new BooleanLiteral(stmt, expr.getJavaTypeMapping(), Boolean.TRUE, null));
        }
        return expr;
    }

    /**
     * Convenience method to resolve a class name.
     * @param className The class name
     * @return The class it relates to (if found)
     */
    public Class resolveClass(String className)
    {
        Class cls = null;
        try
        {
            cls = clr.classForName(className);
        }
        catch (ClassNotResolvedException cnre)
        {
            if (importsDefinition != null)
            {
                cls = importsDefinition.resolveClassDeclaration(className, clr, null);
            }
        }
        if (cls == null && compilation.getQueryLanguage().equalsIgnoreCase("JPQL"))
        {
            // JPQL also allows use of EntityName in queries
            AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForEntityName(className);
            if (cmd != null)
            {
                return clr.classForName(cmd.getFullClassName());
            }
        }
        return cls;
    }
}