// Generated from parser/SQLBase.g4 by ANTLR 4.6.

package parser // SQLBase

import "github.com/antlr/antlr4/runtime/Go/antlr"

// BaseSQLBaseListener is a complete listener for a parse tree produced by SQLBaseParser.
type BaseSQLBaseListener struct{}

var _ SQLBaseListener = &BaseSQLBaseListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseSQLBaseListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseSQLBaseListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseSQLBaseListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseSQLBaseListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterStatements is called when production statements is entered.
func (s *BaseSQLBaseListener) EnterStatements(ctx *StatementsContext) {}

// ExitStatements is called when production statements is exited.
func (s *BaseSQLBaseListener) ExitStatements(ctx *StatementsContext) {}

// EnterStatementDefault is called when production statementDefault is entered.
func (s *BaseSQLBaseListener) EnterStatementDefault(ctx *StatementDefaultContext) {}

// ExitStatementDefault is called when production statementDefault is exited.
func (s *BaseSQLBaseListener) ExitStatementDefault(ctx *StatementDefaultContext) {}

// EnterCreateTable is called when production createTable is entered.
func (s *BaseSQLBaseListener) EnterCreateTable(ctx *CreateTableContext) {}

// ExitCreateTable is called when production createTable is exited.
func (s *BaseSQLBaseListener) ExitCreateTable(ctx *CreateTableContext) {}

// EnterCreateTableAsSelect is called when production createTableAsSelect is entered.
func (s *BaseSQLBaseListener) EnterCreateTableAsSelect(ctx *CreateTableAsSelectContext) {}

// ExitCreateTableAsSelect is called when production createTableAsSelect is exited.
func (s *BaseSQLBaseListener) ExitCreateTableAsSelect(ctx *CreateTableAsSelectContext) {}

// EnterDropTable is called when production dropTable is entered.
func (s *BaseSQLBaseListener) EnterDropTable(ctx *DropTableContext) {}

// ExitDropTable is called when production dropTable is exited.
func (s *BaseSQLBaseListener) ExitDropTable(ctx *DropTableContext) {}

// EnterInsertInto is called when production insertInto is entered.
func (s *BaseSQLBaseListener) EnterInsertInto(ctx *InsertIntoContext) {}

// ExitInsertInto is called when production insertInto is exited.
func (s *BaseSQLBaseListener) ExitInsertInto(ctx *InsertIntoContext) {}

// EnterDelete is called when production delete is entered.
func (s *BaseSQLBaseListener) EnterDelete(ctx *DeleteContext) {}

// ExitDelete is called when production delete is exited.
func (s *BaseSQLBaseListener) ExitDelete(ctx *DeleteContext) {}

// EnterRenameTable is called when production renameTable is entered.
func (s *BaseSQLBaseListener) EnterRenameTable(ctx *RenameTableContext) {}

// ExitRenameTable is called when production renameTable is exited.
func (s *BaseSQLBaseListener) ExitRenameTable(ctx *RenameTableContext) {}

// EnterRenameColumn is called when production renameColumn is entered.
func (s *BaseSQLBaseListener) EnterRenameColumn(ctx *RenameColumnContext) {}

// ExitRenameColumn is called when production renameColumn is exited.
func (s *BaseSQLBaseListener) ExitRenameColumn(ctx *RenameColumnContext) {}

// EnterAddColumn is called when production addColumn is entered.
func (s *BaseSQLBaseListener) EnterAddColumn(ctx *AddColumnContext) {}

// ExitAddColumn is called when production addColumn is exited.
func (s *BaseSQLBaseListener) ExitAddColumn(ctx *AddColumnContext) {}

// EnterCreateView is called when production createView is entered.
func (s *BaseSQLBaseListener) EnterCreateView(ctx *CreateViewContext) {}

// ExitCreateView is called when production createView is exited.
func (s *BaseSQLBaseListener) ExitCreateView(ctx *CreateViewContext) {}

// EnterDropView is called when production dropView is entered.
func (s *BaseSQLBaseListener) EnterDropView(ctx *DropViewContext) {}

// ExitDropView is called when production dropView is exited.
func (s *BaseSQLBaseListener) ExitDropView(ctx *DropViewContext) {}

// EnterCall is called when production call is entered.
func (s *BaseSQLBaseListener) EnterCall(ctx *CallContext) {}

// ExitCall is called when production call is exited.
func (s *BaseSQLBaseListener) ExitCall(ctx *CallContext) {}

// EnterGrant is called when production grant is entered.
func (s *BaseSQLBaseListener) EnterGrant(ctx *GrantContext) {}

// ExitGrant is called when production grant is exited.
func (s *BaseSQLBaseListener) ExitGrant(ctx *GrantContext) {}

// EnterRevoke is called when production revoke is entered.
func (s *BaseSQLBaseListener) EnterRevoke(ctx *RevokeContext) {}

// ExitRevoke is called when production revoke is exited.
func (s *BaseSQLBaseListener) ExitRevoke(ctx *RevokeContext) {}

// EnterExplain is called when production explain is entered.
func (s *BaseSQLBaseListener) EnterExplain(ctx *ExplainContext) {}

// ExitExplain is called when production explain is exited.
func (s *BaseSQLBaseListener) ExitExplain(ctx *ExplainContext) {}

// EnterShowCreateTable is called when production showCreateTable is entered.
func (s *BaseSQLBaseListener) EnterShowCreateTable(ctx *ShowCreateTableContext) {}

// ExitShowCreateTable is called when production showCreateTable is exited.
func (s *BaseSQLBaseListener) ExitShowCreateTable(ctx *ShowCreateTableContext) {}

// EnterShowCreateView is called when production showCreateView is entered.
func (s *BaseSQLBaseListener) EnterShowCreateView(ctx *ShowCreateViewContext) {}

// ExitShowCreateView is called when production showCreateView is exited.
func (s *BaseSQLBaseListener) ExitShowCreateView(ctx *ShowCreateViewContext) {}

// EnterShowTables is called when production showTables is entered.
func (s *BaseSQLBaseListener) EnterShowTables(ctx *ShowTablesContext) {}

// ExitShowTables is called when production showTables is exited.
func (s *BaseSQLBaseListener) ExitShowTables(ctx *ShowTablesContext) {}

// EnterShowSchemas is called when production showSchemas is entered.
func (s *BaseSQLBaseListener) EnterShowSchemas(ctx *ShowSchemasContext) {}

// ExitShowSchemas is called when production showSchemas is exited.
func (s *BaseSQLBaseListener) ExitShowSchemas(ctx *ShowSchemasContext) {}

// EnterShowCatalogs is called when production showCatalogs is entered.
func (s *BaseSQLBaseListener) EnterShowCatalogs(ctx *ShowCatalogsContext) {}

// ExitShowCatalogs is called when production showCatalogs is exited.
func (s *BaseSQLBaseListener) ExitShowCatalogs(ctx *ShowCatalogsContext) {}

// EnterShowColumns is called when production showColumns is entered.
func (s *BaseSQLBaseListener) EnterShowColumns(ctx *ShowColumnsContext) {}

// ExitShowColumns is called when production showColumns is exited.
func (s *BaseSQLBaseListener) ExitShowColumns(ctx *ShowColumnsContext) {}

// EnterShowFunctions is called when production showFunctions is entered.
func (s *BaseSQLBaseListener) EnterShowFunctions(ctx *ShowFunctionsContext) {}

// ExitShowFunctions is called when production showFunctions is exited.
func (s *BaseSQLBaseListener) ExitShowFunctions(ctx *ShowFunctionsContext) {}

// EnterShowSession is called when production showSession is entered.
func (s *BaseSQLBaseListener) EnterShowSession(ctx *ShowSessionContext) {}

// ExitShowSession is called when production showSession is exited.
func (s *BaseSQLBaseListener) ExitShowSession(ctx *ShowSessionContext) {}

// EnterSetSession is called when production setSession is entered.
func (s *BaseSQLBaseListener) EnterSetSession(ctx *SetSessionContext) {}

// ExitSetSession is called when production setSession is exited.
func (s *BaseSQLBaseListener) ExitSetSession(ctx *SetSessionContext) {}

// EnterResetSession is called when production resetSession is entered.
func (s *BaseSQLBaseListener) EnterResetSession(ctx *ResetSessionContext) {}

// ExitResetSession is called when production resetSession is exited.
func (s *BaseSQLBaseListener) ExitResetSession(ctx *ResetSessionContext) {}

// EnterStartTransaction is called when production startTransaction is entered.
func (s *BaseSQLBaseListener) EnterStartTransaction(ctx *StartTransactionContext) {}

// ExitStartTransaction is called when production startTransaction is exited.
func (s *BaseSQLBaseListener) ExitStartTransaction(ctx *StartTransactionContext) {}

// EnterCommit is called when production commit is entered.
func (s *BaseSQLBaseListener) EnterCommit(ctx *CommitContext) {}

// ExitCommit is called when production commit is exited.
func (s *BaseSQLBaseListener) ExitCommit(ctx *CommitContext) {}

// EnterRollback is called when production rollback is entered.
func (s *BaseSQLBaseListener) EnterRollback(ctx *RollbackContext) {}

// ExitRollback is called when production rollback is exited.
func (s *BaseSQLBaseListener) ExitRollback(ctx *RollbackContext) {}

// EnterShowPartitions is called when production showPartitions is entered.
func (s *BaseSQLBaseListener) EnterShowPartitions(ctx *ShowPartitionsContext) {}

// ExitShowPartitions is called when production showPartitions is exited.
func (s *BaseSQLBaseListener) ExitShowPartitions(ctx *ShowPartitionsContext) {}

// EnterPrepare is called when production prepare is entered.
func (s *BaseSQLBaseListener) EnterPrepare(ctx *PrepareContext) {}

// ExitPrepare is called when production prepare is exited.
func (s *BaseSQLBaseListener) ExitPrepare(ctx *PrepareContext) {}

// EnterDeallocate is called when production deallocate is entered.
func (s *BaseSQLBaseListener) EnterDeallocate(ctx *DeallocateContext) {}

// ExitDeallocate is called when production deallocate is exited.
func (s *BaseSQLBaseListener) ExitDeallocate(ctx *DeallocateContext) {}

// EnterExecute is called when production execute is entered.
func (s *BaseSQLBaseListener) EnterExecute(ctx *ExecuteContext) {}

// ExitExecute is called when production execute is exited.
func (s *BaseSQLBaseListener) ExitExecute(ctx *ExecuteContext) {}

// EnterDescribeInput is called when production describeInput is entered.
func (s *BaseSQLBaseListener) EnterDescribeInput(ctx *DescribeInputContext) {}

// ExitDescribeInput is called when production describeInput is exited.
func (s *BaseSQLBaseListener) ExitDescribeInput(ctx *DescribeInputContext) {}

// EnterDescribeOutput is called when production describeOutput is entered.
func (s *BaseSQLBaseListener) EnterDescribeOutput(ctx *DescribeOutputContext) {}

// ExitDescribeOutput is called when production describeOutput is exited.
func (s *BaseSQLBaseListener) ExitDescribeOutput(ctx *DescribeOutputContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseSQLBaseListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseSQLBaseListener) ExitQuery(ctx *QueryContext) {}

// EnterWith is called when production with is entered.
func (s *BaseSQLBaseListener) EnterWith(ctx *WithContext) {}

// ExitWith is called when production with is exited.
func (s *BaseSQLBaseListener) ExitWith(ctx *WithContext) {}

// EnterTableElement is called when production tableElement is entered.
func (s *BaseSQLBaseListener) EnterTableElement(ctx *TableElementContext) {}

// ExitTableElement is called when production tableElement is exited.
func (s *BaseSQLBaseListener) ExitTableElement(ctx *TableElementContext) {}

// EnterColumnDefinition is called when production columnDefinition is entered.
func (s *BaseSQLBaseListener) EnterColumnDefinition(ctx *ColumnDefinitionContext) {}

// ExitColumnDefinition is called when production columnDefinition is exited.
func (s *BaseSQLBaseListener) ExitColumnDefinition(ctx *ColumnDefinitionContext) {}

// EnterLikeClause is called when production likeClause is entered.
func (s *BaseSQLBaseListener) EnterLikeClause(ctx *LikeClauseContext) {}

// ExitLikeClause is called when production likeClause is exited.
func (s *BaseSQLBaseListener) ExitLikeClause(ctx *LikeClauseContext) {}

// EnterTableProperties is called when production tableProperties is entered.
func (s *BaseSQLBaseListener) EnterTableProperties(ctx *TablePropertiesContext) {}

// ExitTableProperties is called when production tableProperties is exited.
func (s *BaseSQLBaseListener) ExitTableProperties(ctx *TablePropertiesContext) {}

// EnterTableProperty is called when production tableProperty is entered.
func (s *BaseSQLBaseListener) EnterTableProperty(ctx *TablePropertyContext) {}

// ExitTableProperty is called when production tableProperty is exited.
func (s *BaseSQLBaseListener) ExitTableProperty(ctx *TablePropertyContext) {}

// EnterQueryNoWith is called when production queryNoWith is entered.
func (s *BaseSQLBaseListener) EnterQueryNoWith(ctx *QueryNoWithContext) {}

// ExitQueryNoWith is called when production queryNoWith is exited.
func (s *BaseSQLBaseListener) ExitQueryNoWith(ctx *QueryNoWithContext) {}

// EnterQueryTermDefault is called when production queryTermDefault is entered.
func (s *BaseSQLBaseListener) EnterQueryTermDefault(ctx *QueryTermDefaultContext) {}

// ExitQueryTermDefault is called when production queryTermDefault is exited.
func (s *BaseSQLBaseListener) ExitQueryTermDefault(ctx *QueryTermDefaultContext) {}

// EnterSetOperation is called when production setOperation is entered.
func (s *BaseSQLBaseListener) EnterSetOperation(ctx *SetOperationContext) {}

// ExitSetOperation is called when production setOperation is exited.
func (s *BaseSQLBaseListener) ExitSetOperation(ctx *SetOperationContext) {}

// EnterQueryPrimaryDefault is called when production queryPrimaryDefault is entered.
func (s *BaseSQLBaseListener) EnterQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) {}

// ExitQueryPrimaryDefault is called when production queryPrimaryDefault is exited.
func (s *BaseSQLBaseListener) ExitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) {}

// EnterTable is called when production table is entered.
func (s *BaseSQLBaseListener) EnterTable(ctx *TableContext) {}

// ExitTable is called when production table is exited.
func (s *BaseSQLBaseListener) ExitTable(ctx *TableContext) {}

// EnterInlineTable is called when production inlineTable is entered.
func (s *BaseSQLBaseListener) EnterInlineTable(ctx *InlineTableContext) {}

// ExitInlineTable is called when production inlineTable is exited.
func (s *BaseSQLBaseListener) ExitInlineTable(ctx *InlineTableContext) {}

// EnterSubquery is called when production subquery is entered.
func (s *BaseSQLBaseListener) EnterSubquery(ctx *SubqueryContext) {}

// ExitSubquery is called when production subquery is exited.
func (s *BaseSQLBaseListener) ExitSubquery(ctx *SubqueryContext) {}

// EnterSortItem is called when production sortItem is entered.
func (s *BaseSQLBaseListener) EnterSortItem(ctx *SortItemContext) {}

// ExitSortItem is called when production sortItem is exited.
func (s *BaseSQLBaseListener) ExitSortItem(ctx *SortItemContext) {}

// EnterQuerySpecification is called when production querySpecification is entered.
func (s *BaseSQLBaseListener) EnterQuerySpecification(ctx *QuerySpecificationContext) {}

// ExitQuerySpecification is called when production querySpecification is exited.
func (s *BaseSQLBaseListener) ExitQuerySpecification(ctx *QuerySpecificationContext) {}

// EnterGroupBy is called when production groupBy is entered.
func (s *BaseSQLBaseListener) EnterGroupBy(ctx *GroupByContext) {}

// ExitGroupBy is called when production groupBy is exited.
func (s *BaseSQLBaseListener) ExitGroupBy(ctx *GroupByContext) {}

// EnterSingleGroupingSet is called when production singleGroupingSet is entered.
func (s *BaseSQLBaseListener) EnterSingleGroupingSet(ctx *SingleGroupingSetContext) {}

// ExitSingleGroupingSet is called when production singleGroupingSet is exited.
func (s *BaseSQLBaseListener) ExitSingleGroupingSet(ctx *SingleGroupingSetContext) {}

// EnterRollup is called when production rollup is entered.
func (s *BaseSQLBaseListener) EnterRollup(ctx *RollupContext) {}

// ExitRollup is called when production rollup is exited.
func (s *BaseSQLBaseListener) ExitRollup(ctx *RollupContext) {}

// EnterCube is called when production cube is entered.
func (s *BaseSQLBaseListener) EnterCube(ctx *CubeContext) {}

// ExitCube is called when production cube is exited.
func (s *BaseSQLBaseListener) ExitCube(ctx *CubeContext) {}

// EnterMultipleGroupingSets is called when production multipleGroupingSets is entered.
func (s *BaseSQLBaseListener) EnterMultipleGroupingSets(ctx *MultipleGroupingSetsContext) {}

// ExitMultipleGroupingSets is called when production multipleGroupingSets is exited.
func (s *BaseSQLBaseListener) ExitMultipleGroupingSets(ctx *MultipleGroupingSetsContext) {}

// EnterGroupingExpressions is called when production groupingExpressions is entered.
func (s *BaseSQLBaseListener) EnterGroupingExpressions(ctx *GroupingExpressionsContext) {}

// ExitGroupingExpressions is called when production groupingExpressions is exited.
func (s *BaseSQLBaseListener) ExitGroupingExpressions(ctx *GroupingExpressionsContext) {}

// EnterGroupingSet is called when production groupingSet is entered.
func (s *BaseSQLBaseListener) EnterGroupingSet(ctx *GroupingSetContext) {}

// ExitGroupingSet is called when production groupingSet is exited.
func (s *BaseSQLBaseListener) ExitGroupingSet(ctx *GroupingSetContext) {}

// EnterNamedQuery is called when production namedQuery is entered.
func (s *BaseSQLBaseListener) EnterNamedQuery(ctx *NamedQueryContext) {}

// ExitNamedQuery is called when production namedQuery is exited.
func (s *BaseSQLBaseListener) ExitNamedQuery(ctx *NamedQueryContext) {}

// EnterSetQuantifier is called when production setQuantifier is entered.
func (s *BaseSQLBaseListener) EnterSetQuantifier(ctx *SetQuantifierContext) {}

// ExitSetQuantifier is called when production setQuantifier is exited.
func (s *BaseSQLBaseListener) ExitSetQuantifier(ctx *SetQuantifierContext) {}

// EnterSelectSingle is called when production selectSingle is entered.
func (s *BaseSQLBaseListener) EnterSelectSingle(ctx *SelectSingleContext) {}

// ExitSelectSingle is called when production selectSingle is exited.
func (s *BaseSQLBaseListener) ExitSelectSingle(ctx *SelectSingleContext) {}

// EnterSelectAll is called when production selectAll is entered.
func (s *BaseSQLBaseListener) EnterSelectAll(ctx *SelectAllContext) {}

// ExitSelectAll is called when production selectAll is exited.
func (s *BaseSQLBaseListener) ExitSelectAll(ctx *SelectAllContext) {}

// EnterRelationDefault is called when production relationDefault is entered.
func (s *BaseSQLBaseListener) EnterRelationDefault(ctx *RelationDefaultContext) {}

// ExitRelationDefault is called when production relationDefault is exited.
func (s *BaseSQLBaseListener) ExitRelationDefault(ctx *RelationDefaultContext) {}

// EnterJoinRelation is called when production joinRelation is entered.
func (s *BaseSQLBaseListener) EnterJoinRelation(ctx *JoinRelationContext) {}

// ExitJoinRelation is called when production joinRelation is exited.
func (s *BaseSQLBaseListener) ExitJoinRelation(ctx *JoinRelationContext) {}

// EnterJoinType is called when production joinType is entered.
func (s *BaseSQLBaseListener) EnterJoinType(ctx *JoinTypeContext) {}

// ExitJoinType is called when production joinType is exited.
func (s *BaseSQLBaseListener) ExitJoinType(ctx *JoinTypeContext) {}

// EnterJoinCriteria is called when production joinCriteria is entered.
func (s *BaseSQLBaseListener) EnterJoinCriteria(ctx *JoinCriteriaContext) {}

// ExitJoinCriteria is called when production joinCriteria is exited.
func (s *BaseSQLBaseListener) ExitJoinCriteria(ctx *JoinCriteriaContext) {}

// EnterSampledRelation is called when production sampledRelation is entered.
func (s *BaseSQLBaseListener) EnterSampledRelation(ctx *SampledRelationContext) {}

// ExitSampledRelation is called when production sampledRelation is exited.
func (s *BaseSQLBaseListener) ExitSampledRelation(ctx *SampledRelationContext) {}

// EnterAliasedRelation is called when production aliasedRelation is entered.
func (s *BaseSQLBaseListener) EnterAliasedRelation(ctx *AliasedRelationContext) {}

// ExitAliasedRelation is called when production aliasedRelation is exited.
func (s *BaseSQLBaseListener) ExitAliasedRelation(ctx *AliasedRelationContext) {}

// EnterColumnAliases is called when production columnAliases is entered.
func (s *BaseSQLBaseListener) EnterColumnAliases(ctx *ColumnAliasesContext) {}

// ExitColumnAliases is called when production columnAliases is exited.
func (s *BaseSQLBaseListener) ExitColumnAliases(ctx *ColumnAliasesContext) {}

// EnterTableName is called when production tableName is entered.
func (s *BaseSQLBaseListener) EnterTableName(ctx *TableNameContext) {}

// ExitTableName is called when production tableName is exited.
func (s *BaseSQLBaseListener) ExitTableName(ctx *TableNameContext) {}

// EnterSubqueryRelation is called when production subqueryRelation is entered.
func (s *BaseSQLBaseListener) EnterSubqueryRelation(ctx *SubqueryRelationContext) {}

// ExitSubqueryRelation is called when production subqueryRelation is exited.
func (s *BaseSQLBaseListener) ExitSubqueryRelation(ctx *SubqueryRelationContext) {}

// EnterUnnest is called when production unnest is entered.
func (s *BaseSQLBaseListener) EnterUnnest(ctx *UnnestContext) {}

// ExitUnnest is called when production unnest is exited.
func (s *BaseSQLBaseListener) ExitUnnest(ctx *UnnestContext) {}

// EnterParenthesizedRelation is called when production parenthesizedRelation is entered.
func (s *BaseSQLBaseListener) EnterParenthesizedRelation(ctx *ParenthesizedRelationContext) {}

// ExitParenthesizedRelation is called when production parenthesizedRelation is exited.
func (s *BaseSQLBaseListener) ExitParenthesizedRelation(ctx *ParenthesizedRelationContext) {}

// EnterExpression is called when production expression is entered.
func (s *BaseSQLBaseListener) EnterExpression(ctx *ExpressionContext) {}

// ExitExpression is called when production expression is exited.
func (s *BaseSQLBaseListener) ExitExpression(ctx *ExpressionContext) {}

// EnterLogicalNot is called when production logicalNot is entered.
func (s *BaseSQLBaseListener) EnterLogicalNot(ctx *LogicalNotContext) {}

// ExitLogicalNot is called when production logicalNot is exited.
func (s *BaseSQLBaseListener) ExitLogicalNot(ctx *LogicalNotContext) {}

// EnterBooleanDefault is called when production booleanDefault is entered.
func (s *BaseSQLBaseListener) EnterBooleanDefault(ctx *BooleanDefaultContext) {}

// ExitBooleanDefault is called when production booleanDefault is exited.
func (s *BaseSQLBaseListener) ExitBooleanDefault(ctx *BooleanDefaultContext) {}

// EnterBoolLiteralToo is called when production boolLiteralToo is entered.
func (s *BaseSQLBaseListener) EnterBoolLiteralToo(ctx *BoolLiteralTooContext) {}

// ExitBoolLiteralToo is called when production boolLiteralToo is exited.
func (s *BaseSQLBaseListener) ExitBoolLiteralToo(ctx *BoolLiteralTooContext) {}

// EnterLogicalBinary is called when production logicalBinary is entered.
func (s *BaseSQLBaseListener) EnterLogicalBinary(ctx *LogicalBinaryContext) {}

// ExitLogicalBinary is called when production logicalBinary is exited.
func (s *BaseSQLBaseListener) ExitLogicalBinary(ctx *LogicalBinaryContext) {}

// EnterBooleanliteral is called when production booleanliteral is entered.
func (s *BaseSQLBaseListener) EnterBooleanliteral(ctx *BooleanliteralContext) {}

// ExitBooleanliteral is called when production booleanliteral is exited.
func (s *BaseSQLBaseListener) ExitBooleanliteral(ctx *BooleanliteralContext) {}

// EnterComparison is called when production comparison is entered.
func (s *BaseSQLBaseListener) EnterComparison(ctx *ComparisonContext) {}

// ExitComparison is called when production comparison is exited.
func (s *BaseSQLBaseListener) ExitComparison(ctx *ComparisonContext) {}

// EnterQuantifiedComparison is called when production quantifiedComparison is entered.
func (s *BaseSQLBaseListener) EnterQuantifiedComparison(ctx *QuantifiedComparisonContext) {}

// ExitQuantifiedComparison is called when production quantifiedComparison is exited.
func (s *BaseSQLBaseListener) ExitQuantifiedComparison(ctx *QuantifiedComparisonContext) {}

// EnterBetween is called when production between is entered.
func (s *BaseSQLBaseListener) EnterBetween(ctx *BetweenContext) {}

// ExitBetween is called when production between is exited.
func (s *BaseSQLBaseListener) ExitBetween(ctx *BetweenContext) {}

// EnterInList is called when production inList is entered.
func (s *BaseSQLBaseListener) EnterInList(ctx *InListContext) {}

// ExitInList is called when production inList is exited.
func (s *BaseSQLBaseListener) ExitInList(ctx *InListContext) {}

// EnterInSubquery is called when production inSubquery is entered.
func (s *BaseSQLBaseListener) EnterInSubquery(ctx *InSubqueryContext) {}

// ExitInSubquery is called when production inSubquery is exited.
func (s *BaseSQLBaseListener) ExitInSubquery(ctx *InSubqueryContext) {}

// EnterLike is called when production like is entered.
func (s *BaseSQLBaseListener) EnterLike(ctx *LikeContext) {}

// ExitLike is called when production like is exited.
func (s *BaseSQLBaseListener) ExitLike(ctx *LikeContext) {}

// EnterNullPredicate is called when production nullPredicate is entered.
func (s *BaseSQLBaseListener) EnterNullPredicate(ctx *NullPredicateContext) {}

// ExitNullPredicate is called when production nullPredicate is exited.
func (s *BaseSQLBaseListener) ExitNullPredicate(ctx *NullPredicateContext) {}

// EnterDistinctFrom is called when production distinctFrom is entered.
func (s *BaseSQLBaseListener) EnterDistinctFrom(ctx *DistinctFromContext) {}

// ExitDistinctFrom is called when production distinctFrom is exited.
func (s *BaseSQLBaseListener) ExitDistinctFrom(ctx *DistinctFromContext) {}

// EnterValueExpressionDefault is called when production valueExpressionDefault is entered.
func (s *BaseSQLBaseListener) EnterValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// ExitValueExpressionDefault is called when production valueExpressionDefault is exited.
func (s *BaseSQLBaseListener) ExitValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// EnterConcatenation is called when production concatenation is entered.
func (s *BaseSQLBaseListener) EnterConcatenation(ctx *ConcatenationContext) {}

// ExitConcatenation is called when production concatenation is exited.
func (s *BaseSQLBaseListener) ExitConcatenation(ctx *ConcatenationContext) {}

// EnterArithmeticBinary is called when production arithmeticBinary is entered.
func (s *BaseSQLBaseListener) EnterArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// ExitArithmeticBinary is called when production arithmeticBinary is exited.
func (s *BaseSQLBaseListener) ExitArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// EnterArithmeticUnary is called when production arithmeticUnary is entered.
func (s *BaseSQLBaseListener) EnterArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// ExitArithmeticUnary is called when production arithmeticUnary is exited.
func (s *BaseSQLBaseListener) ExitArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// EnterAtTimeZone is called when production atTimeZone is entered.
func (s *BaseSQLBaseListener) EnterAtTimeZone(ctx *AtTimeZoneContext) {}

// ExitAtTimeZone is called when production atTimeZone is exited.
func (s *BaseSQLBaseListener) ExitAtTimeZone(ctx *AtTimeZoneContext) {}

// EnterDereference is called when production dereference is entered.
func (s *BaseSQLBaseListener) EnterDereference(ctx *DereferenceContext) {}

// ExitDereference is called when production dereference is exited.
func (s *BaseSQLBaseListener) ExitDereference(ctx *DereferenceContext) {}

// EnterDecimalLiteral is called when production decimalLiteral is entered.
func (s *BaseSQLBaseListener) EnterDecimalLiteral(ctx *DecimalLiteralContext) {}

// ExitDecimalLiteral is called when production decimalLiteral is exited.
func (s *BaseSQLBaseListener) ExitDecimalLiteral(ctx *DecimalLiteralContext) {}

// EnterTypeConstructor is called when production typeConstructor is entered.
func (s *BaseSQLBaseListener) EnterTypeConstructor(ctx *TypeConstructorContext) {}

// ExitTypeConstructor is called when production typeConstructor is exited.
func (s *BaseSQLBaseListener) ExitTypeConstructor(ctx *TypeConstructorContext) {}

// EnterSpecialDateTimeFunction is called when production specialDateTimeFunction is entered.
func (s *BaseSQLBaseListener) EnterSpecialDateTimeFunction(ctx *SpecialDateTimeFunctionContext) {}

// ExitSpecialDateTimeFunction is called when production specialDateTimeFunction is exited.
func (s *BaseSQLBaseListener) ExitSpecialDateTimeFunction(ctx *SpecialDateTimeFunctionContext) {}

// EnterBoolLiteral is called when production boolLiteral is entered.
func (s *BaseSQLBaseListener) EnterBoolLiteral(ctx *BoolLiteralContext) {}

// ExitBoolLiteral is called when production boolLiteral is exited.
func (s *BaseSQLBaseListener) ExitBoolLiteral(ctx *BoolLiteralContext) {}

// EnterSubstring is called when production substring is entered.
func (s *BaseSQLBaseListener) EnterSubstring(ctx *SubstringContext) {}

// ExitSubstring is called when production substring is exited.
func (s *BaseSQLBaseListener) ExitSubstring(ctx *SubstringContext) {}

// EnterCast is called when production cast is entered.
func (s *BaseSQLBaseListener) EnterCast(ctx *CastContext) {}

// ExitCast is called when production cast is exited.
func (s *BaseSQLBaseListener) ExitCast(ctx *CastContext) {}

// EnterLambda is called when production lambda is entered.
func (s *BaseSQLBaseListener) EnterLambda(ctx *LambdaContext) {}

// ExitLambda is called when production lambda is exited.
func (s *BaseSQLBaseListener) ExitLambda(ctx *LambdaContext) {}

// EnterParenthesizedExpression is called when production parenthesizedExpression is entered.
func (s *BaseSQLBaseListener) EnterParenthesizedExpression(ctx *ParenthesizedExpressionContext) {}

// ExitParenthesizedExpression is called when production parenthesizedExpression is exited.
func (s *BaseSQLBaseListener) ExitParenthesizedExpression(ctx *ParenthesizedExpressionContext) {}

// EnterParameter is called when production parameter is entered.
func (s *BaseSQLBaseListener) EnterParameter(ctx *ParameterContext) {}

// ExitParameter is called when production parameter is exited.
func (s *BaseSQLBaseListener) ExitParameter(ctx *ParameterContext) {}

// EnterNormalize is called when production normalize is entered.
func (s *BaseSQLBaseListener) EnterNormalize(ctx *NormalizeContext) {}

// ExitNormalize is called when production normalize is exited.
func (s *BaseSQLBaseListener) ExitNormalize(ctx *NormalizeContext) {}

// EnterIntervalLiteral is called when production intervalLiteral is entered.
func (s *BaseSQLBaseListener) EnterIntervalLiteral(ctx *IntervalLiteralContext) {}

// ExitIntervalLiteral is called when production intervalLiteral is exited.
func (s *BaseSQLBaseListener) ExitIntervalLiteral(ctx *IntervalLiteralContext) {}

// EnterSimpleCase is called when production simpleCase is entered.
func (s *BaseSQLBaseListener) EnterSimpleCase(ctx *SimpleCaseContext) {}

// ExitSimpleCase is called when production simpleCase is exited.
func (s *BaseSQLBaseListener) ExitSimpleCase(ctx *SimpleCaseContext) {}

// EnterColumnReference is called when production columnReference is entered.
func (s *BaseSQLBaseListener) EnterColumnReference(ctx *ColumnReferenceContext) {}

// ExitColumnReference is called when production columnReference is exited.
func (s *BaseSQLBaseListener) ExitColumnReference(ctx *ColumnReferenceContext) {}

// EnterNullLiteral is called when production nullLiteral is entered.
func (s *BaseSQLBaseListener) EnterNullLiteral(ctx *NullLiteralContext) {}

// ExitNullLiteral is called when production nullLiteral is exited.
func (s *BaseSQLBaseListener) ExitNullLiteral(ctx *NullLiteralContext) {}

// EnterTypedLiteral is called when production typedLiteral is entered.
func (s *BaseSQLBaseListener) EnterTypedLiteral(ctx *TypedLiteralContext) {}

// ExitTypedLiteral is called when production typedLiteral is exited.
func (s *BaseSQLBaseListener) ExitTypedLiteral(ctx *TypedLiteralContext) {}

// EnterRowConstructor is called when production rowConstructor is entered.
func (s *BaseSQLBaseListener) EnterRowConstructor(ctx *RowConstructorContext) {}

// ExitRowConstructor is called when production rowConstructor is exited.
func (s *BaseSQLBaseListener) ExitRowConstructor(ctx *RowConstructorContext) {}

// EnterSubscript is called when production subscript is entered.
func (s *BaseSQLBaseListener) EnterSubscript(ctx *SubscriptContext) {}

// ExitSubscript is called when production subscript is exited.
func (s *BaseSQLBaseListener) ExitSubscript(ctx *SubscriptContext) {}

// EnterSubqueryExpression is called when production subqueryExpression is entered.
func (s *BaseSQLBaseListener) EnterSubqueryExpression(ctx *SubqueryExpressionContext) {}

// ExitSubqueryExpression is called when production subqueryExpression is exited.
func (s *BaseSQLBaseListener) ExitSubqueryExpression(ctx *SubqueryExpressionContext) {}

// EnterBinaryLiteral is called when production binaryLiteral is entered.
func (s *BaseSQLBaseListener) EnterBinaryLiteral(ctx *BinaryLiteralContext) {}

// ExitBinaryLiteral is called when production binaryLiteral is exited.
func (s *BaseSQLBaseListener) ExitBinaryLiteral(ctx *BinaryLiteralContext) {}

// EnterExtract is called when production extract is entered.
func (s *BaseSQLBaseListener) EnterExtract(ctx *ExtractContext) {}

// ExitExtract is called when production extract is exited.
func (s *BaseSQLBaseListener) ExitExtract(ctx *ExtractContext) {}

// EnterStringLiteral is called when production stringLiteral is entered.
func (s *BaseSQLBaseListener) EnterStringLiteral(ctx *StringLiteralContext) {}

// ExitStringLiteral is called when production stringLiteral is exited.
func (s *BaseSQLBaseListener) ExitStringLiteral(ctx *StringLiteralContext) {}

// EnterArrayConstructor is called when production arrayConstructor is entered.
func (s *BaseSQLBaseListener) EnterArrayConstructor(ctx *ArrayConstructorContext) {}

// ExitArrayConstructor is called when production arrayConstructor is exited.
func (s *BaseSQLBaseListener) ExitArrayConstructor(ctx *ArrayConstructorContext) {}

// EnterFunctionCall is called when production functionCall is entered.
func (s *BaseSQLBaseListener) EnterFunctionCall(ctx *FunctionCallContext) {}

// ExitFunctionCall is called when production functionCall is exited.
func (s *BaseSQLBaseListener) ExitFunctionCall(ctx *FunctionCallContext) {}

// EnterIntegerLiteral is called when production integerLiteral is entered.
func (s *BaseSQLBaseListener) EnterIntegerLiteral(ctx *IntegerLiteralContext) {}

// ExitIntegerLiteral is called when production integerLiteral is exited.
func (s *BaseSQLBaseListener) ExitIntegerLiteral(ctx *IntegerLiteralContext) {}

// EnterExists is called when production exists is entered.
func (s *BaseSQLBaseListener) EnterExists(ctx *ExistsContext) {}

// ExitExists is called when production exists is exited.
func (s *BaseSQLBaseListener) ExitExists(ctx *ExistsContext) {}

// EnterPosition is called when production position is entered.
func (s *BaseSQLBaseListener) EnterPosition(ctx *PositionContext) {}

// ExitPosition is called when production position is exited.
func (s *BaseSQLBaseListener) ExitPosition(ctx *PositionContext) {}

// EnterSearchedCase is called when production searchedCase is entered.
func (s *BaseSQLBaseListener) EnterSearchedCase(ctx *SearchedCaseContext) {}

// ExitSearchedCase is called when production searchedCase is exited.
func (s *BaseSQLBaseListener) ExitSearchedCase(ctx *SearchedCaseContext) {}

// EnterTimeZoneInterval is called when production timeZoneInterval is entered.
func (s *BaseSQLBaseListener) EnterTimeZoneInterval(ctx *TimeZoneIntervalContext) {}

// ExitTimeZoneInterval is called when production timeZoneInterval is exited.
func (s *BaseSQLBaseListener) ExitTimeZoneInterval(ctx *TimeZoneIntervalContext) {}

// EnterTimeZoneString is called when production timeZoneString is entered.
func (s *BaseSQLBaseListener) EnterTimeZoneString(ctx *TimeZoneStringContext) {}

// ExitTimeZoneString is called when production timeZoneString is exited.
func (s *BaseSQLBaseListener) ExitTimeZoneString(ctx *TimeZoneStringContext) {}

// EnterComparisonOperator is called when production comparisonOperator is entered.
func (s *BaseSQLBaseListener) EnterComparisonOperator(ctx *ComparisonOperatorContext) {}

// ExitComparisonOperator is called when production comparisonOperator is exited.
func (s *BaseSQLBaseListener) ExitComparisonOperator(ctx *ComparisonOperatorContext) {}

// EnterComparisonQuantifier is called when production comparisonQuantifier is entered.
func (s *BaseSQLBaseListener) EnterComparisonQuantifier(ctx *ComparisonQuantifierContext) {}

// ExitComparisonQuantifier is called when production comparisonQuantifier is exited.
func (s *BaseSQLBaseListener) ExitComparisonQuantifier(ctx *ComparisonQuantifierContext) {}

// EnterInterval is called when production interval is entered.
func (s *BaseSQLBaseListener) EnterInterval(ctx *IntervalContext) {}

// ExitInterval is called when production interval is exited.
func (s *BaseSQLBaseListener) ExitInterval(ctx *IntervalContext) {}

// EnterIntervalField is called when production intervalField is entered.
func (s *BaseSQLBaseListener) EnterIntervalField(ctx *IntervalFieldContext) {}

// ExitIntervalField is called when production intervalField is exited.
func (s *BaseSQLBaseListener) ExitIntervalField(ctx *IntervalFieldContext) {}

// EnterType_t is called when production type_t is entered.
func (s *BaseSQLBaseListener) EnterType_t(ctx *Type_tContext) {}

// ExitType_t is called when production type_t is exited.
func (s *BaseSQLBaseListener) ExitType_t(ctx *Type_tContext) {}

// EnterTypeParameter is called when production typeParameter is entered.
func (s *BaseSQLBaseListener) EnterTypeParameter(ctx *TypeParameterContext) {}

// ExitTypeParameter is called when production typeParameter is exited.
func (s *BaseSQLBaseListener) ExitTypeParameter(ctx *TypeParameterContext) {}

// EnterBaseType is called when production baseType is entered.
func (s *BaseSQLBaseListener) EnterBaseType(ctx *BaseTypeContext) {}

// ExitBaseType is called when production baseType is exited.
func (s *BaseSQLBaseListener) ExitBaseType(ctx *BaseTypeContext) {}

// EnterWhenClause is called when production whenClause is entered.
func (s *BaseSQLBaseListener) EnterWhenClause(ctx *WhenClauseContext) {}

// ExitWhenClause is called when production whenClause is exited.
func (s *BaseSQLBaseListener) ExitWhenClause(ctx *WhenClauseContext) {}

// EnterFilter is called when production filter is entered.
func (s *BaseSQLBaseListener) EnterFilter(ctx *FilterContext) {}

// ExitFilter is called when production filter is exited.
func (s *BaseSQLBaseListener) ExitFilter(ctx *FilterContext) {}

// EnterOver is called when production over is entered.
func (s *BaseSQLBaseListener) EnterOver(ctx *OverContext) {}

// ExitOver is called when production over is exited.
func (s *BaseSQLBaseListener) ExitOver(ctx *OverContext) {}

// EnterWindowFrame is called when production windowFrame is entered.
func (s *BaseSQLBaseListener) EnterWindowFrame(ctx *WindowFrameContext) {}

// ExitWindowFrame is called when production windowFrame is exited.
func (s *BaseSQLBaseListener) ExitWindowFrame(ctx *WindowFrameContext) {}

// EnterUnboundedFrame is called when production unboundedFrame is entered.
func (s *BaseSQLBaseListener) EnterUnboundedFrame(ctx *UnboundedFrameContext) {}

// ExitUnboundedFrame is called when production unboundedFrame is exited.
func (s *BaseSQLBaseListener) ExitUnboundedFrame(ctx *UnboundedFrameContext) {}

// EnterCurrentRowBound is called when production currentRowBound is entered.
func (s *BaseSQLBaseListener) EnterCurrentRowBound(ctx *CurrentRowBoundContext) {}

// ExitCurrentRowBound is called when production currentRowBound is exited.
func (s *BaseSQLBaseListener) ExitCurrentRowBound(ctx *CurrentRowBoundContext) {}

// EnterBoundedFrame is called when production boundedFrame is entered.
func (s *BaseSQLBaseListener) EnterBoundedFrame(ctx *BoundedFrameContext) {}

// ExitBoundedFrame is called when production boundedFrame is exited.
func (s *BaseSQLBaseListener) ExitBoundedFrame(ctx *BoundedFrameContext) {}

// EnterExplainFormat is called when production explainFormat is entered.
func (s *BaseSQLBaseListener) EnterExplainFormat(ctx *ExplainFormatContext) {}

// ExitExplainFormat is called when production explainFormat is exited.
func (s *BaseSQLBaseListener) ExitExplainFormat(ctx *ExplainFormatContext) {}

// EnterExplainType is called when production explainType is entered.
func (s *BaseSQLBaseListener) EnterExplainType(ctx *ExplainTypeContext) {}

// ExitExplainType is called when production explainType is exited.
func (s *BaseSQLBaseListener) ExitExplainType(ctx *ExplainTypeContext) {}

// EnterIsolationLevel is called when production isolationLevel is entered.
func (s *BaseSQLBaseListener) EnterIsolationLevel(ctx *IsolationLevelContext) {}

// ExitIsolationLevel is called when production isolationLevel is exited.
func (s *BaseSQLBaseListener) ExitIsolationLevel(ctx *IsolationLevelContext) {}

// EnterTransactionAccessMode is called when production transactionAccessMode is entered.
func (s *BaseSQLBaseListener) EnterTransactionAccessMode(ctx *TransactionAccessModeContext) {}

// ExitTransactionAccessMode is called when production transactionAccessMode is exited.
func (s *BaseSQLBaseListener) ExitTransactionAccessMode(ctx *TransactionAccessModeContext) {}

// EnterReadUncommitted is called when production readUncommitted is entered.
func (s *BaseSQLBaseListener) EnterReadUncommitted(ctx *ReadUncommittedContext) {}

// ExitReadUncommitted is called when production readUncommitted is exited.
func (s *BaseSQLBaseListener) ExitReadUncommitted(ctx *ReadUncommittedContext) {}

// EnterReadCommitted is called when production readCommitted is entered.
func (s *BaseSQLBaseListener) EnterReadCommitted(ctx *ReadCommittedContext) {}

// ExitReadCommitted is called when production readCommitted is exited.
func (s *BaseSQLBaseListener) ExitReadCommitted(ctx *ReadCommittedContext) {}

// EnterRepeatableRead is called when production repeatableRead is entered.
func (s *BaseSQLBaseListener) EnterRepeatableRead(ctx *RepeatableReadContext) {}

// ExitRepeatableRead is called when production repeatableRead is exited.
func (s *BaseSQLBaseListener) ExitRepeatableRead(ctx *RepeatableReadContext) {}

// EnterSerializable is called when production serializable is entered.
func (s *BaseSQLBaseListener) EnterSerializable(ctx *SerializableContext) {}

// ExitSerializable is called when production serializable is exited.
func (s *BaseSQLBaseListener) ExitSerializable(ctx *SerializableContext) {}

// EnterPositionalArgument is called when production positionalArgument is entered.
func (s *BaseSQLBaseListener) EnterPositionalArgument(ctx *PositionalArgumentContext) {}

// ExitPositionalArgument is called when production positionalArgument is exited.
func (s *BaseSQLBaseListener) ExitPositionalArgument(ctx *PositionalArgumentContext) {}

// EnterNamedArgument is called when production namedArgument is entered.
func (s *BaseSQLBaseListener) EnterNamedArgument(ctx *NamedArgumentContext) {}

// ExitNamedArgument is called when production namedArgument is exited.
func (s *BaseSQLBaseListener) ExitNamedArgument(ctx *NamedArgumentContext) {}

// EnterPrivilege is called when production privilege is entered.
func (s *BaseSQLBaseListener) EnterPrivilege(ctx *PrivilegeContext) {}

// ExitPrivilege is called when production privilege is exited.
func (s *BaseSQLBaseListener) ExitPrivilege(ctx *PrivilegeContext) {}

// EnterDotQualifiedName is called when production dotQualifiedName is entered.
func (s *BaseSQLBaseListener) EnterDotQualifiedName(ctx *DotQualifiedNameContext) {}

// ExitDotQualifiedName is called when production dotQualifiedName is exited.
func (s *BaseSQLBaseListener) ExitDotQualifiedName(ctx *DotQualifiedNameContext) {}

// EnterUnquotedIdentifier is called when production unquotedIdentifier is entered.
func (s *BaseSQLBaseListener) EnterUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// ExitUnquotedIdentifier is called when production unquotedIdentifier is exited.
func (s *BaseSQLBaseListener) ExitUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// EnterDigitIdentifier is called when production digitIdentifier is entered.
func (s *BaseSQLBaseListener) EnterDigitIdentifier(ctx *DigitIdentifierContext) {}

// ExitDigitIdentifier is called when production digitIdentifier is exited.
func (s *BaseSQLBaseListener) ExitDigitIdentifier(ctx *DigitIdentifierContext) {}

// EnterQuotedIdentifierAlternative is called when production quotedIdentifierAlternative is entered.
func (s *BaseSQLBaseListener) EnterQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) {
}

// ExitQuotedIdentifierAlternative is called when production quotedIdentifierAlternative is exited.
func (s *BaseSQLBaseListener) ExitQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) {
}

// EnterBackQuotedIdentifier is called when production backQuotedIdentifier is entered.
func (s *BaseSQLBaseListener) EnterBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// ExitBackQuotedIdentifier is called when production backQuotedIdentifier is exited.
func (s *BaseSQLBaseListener) ExitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// EnterNonReservedIdentifier is called when production nonReservedIdentifier is entered.
func (s *BaseSQLBaseListener) EnterNonReservedIdentifier(ctx *NonReservedIdentifierContext) {}

// ExitNonReservedIdentifier is called when production nonReservedIdentifier is exited.
func (s *BaseSQLBaseListener) ExitNonReservedIdentifier(ctx *NonReservedIdentifierContext) {}

// EnterNonReserved is called when production nonReserved is entered.
func (s *BaseSQLBaseListener) EnterNonReserved(ctx *NonReservedContext) {}

// ExitNonReserved is called when production nonReserved is exited.
func (s *BaseSQLBaseListener) ExitNonReserved(ctx *NonReservedContext) {}
