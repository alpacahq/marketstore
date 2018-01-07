// Generated from parser/SQLBase.g4 by ANTLR 4.6.

package parser // SQLBase

import "github.com/antlr/antlr4/runtime/Go/antlr"

// SQLBaseListener is a complete listener for a parse tree produced by SQLBaseParser.
type SQLBaseListener interface {
	antlr.ParseTreeListener

	// EnterStatements is called when entering the statements production.
	EnterStatements(c *StatementsContext)

	// EnterStatementDefault is called when entering the statementDefault production.
	EnterStatementDefault(c *StatementDefaultContext)

	// EnterCreateTable is called when entering the createTable production.
	EnterCreateTable(c *CreateTableContext)

	// EnterCreateTableAsSelect is called when entering the createTableAsSelect production.
	EnterCreateTableAsSelect(c *CreateTableAsSelectContext)

	// EnterDropTable is called when entering the dropTable production.
	EnterDropTable(c *DropTableContext)

	// EnterInsertInto is called when entering the insertInto production.
	EnterInsertInto(c *InsertIntoContext)

	// EnterDelete is called when entering the delete production.
	EnterDelete(c *DeleteContext)

	// EnterRenameTable is called when entering the renameTable production.
	EnterRenameTable(c *RenameTableContext)

	// EnterRenameColumn is called when entering the renameColumn production.
	EnterRenameColumn(c *RenameColumnContext)

	// EnterAddColumn is called when entering the addColumn production.
	EnterAddColumn(c *AddColumnContext)

	// EnterCreateView is called when entering the createView production.
	EnterCreateView(c *CreateViewContext)

	// EnterDropView is called when entering the dropView production.
	EnterDropView(c *DropViewContext)

	// EnterCall is called when entering the call production.
	EnterCall(c *CallContext)

	// EnterGrant is called when entering the grant production.
	EnterGrant(c *GrantContext)

	// EnterRevoke is called when entering the revoke production.
	EnterRevoke(c *RevokeContext)

	// EnterExplain is called when entering the explain production.
	EnterExplain(c *ExplainContext)

	// EnterShowCreateTable is called when entering the showCreateTable production.
	EnterShowCreateTable(c *ShowCreateTableContext)

	// EnterShowCreateView is called when entering the showCreateView production.
	EnterShowCreateView(c *ShowCreateViewContext)

	// EnterShowTables is called when entering the showTables production.
	EnterShowTables(c *ShowTablesContext)

	// EnterShowSchemas is called when entering the showSchemas production.
	EnterShowSchemas(c *ShowSchemasContext)

	// EnterShowCatalogs is called when entering the showCatalogs production.
	EnterShowCatalogs(c *ShowCatalogsContext)

	// EnterShowColumns is called when entering the showColumns production.
	EnterShowColumns(c *ShowColumnsContext)

	// EnterShowFunctions is called when entering the showFunctions production.
	EnterShowFunctions(c *ShowFunctionsContext)

	// EnterShowSession is called when entering the showSession production.
	EnterShowSession(c *ShowSessionContext)

	// EnterSetSession is called when entering the setSession production.
	EnterSetSession(c *SetSessionContext)

	// EnterResetSession is called when entering the resetSession production.
	EnterResetSession(c *ResetSessionContext)

	// EnterStartTransaction is called when entering the startTransaction production.
	EnterStartTransaction(c *StartTransactionContext)

	// EnterCommit is called when entering the commit production.
	EnterCommit(c *CommitContext)

	// EnterRollback is called when entering the rollback production.
	EnterRollback(c *RollbackContext)

	// EnterShowPartitions is called when entering the showPartitions production.
	EnterShowPartitions(c *ShowPartitionsContext)

	// EnterPrepare is called when entering the prepare production.
	EnterPrepare(c *PrepareContext)

	// EnterDeallocate is called when entering the deallocate production.
	EnterDeallocate(c *DeallocateContext)

	// EnterExecute is called when entering the execute production.
	EnterExecute(c *ExecuteContext)

	// EnterDescribeInput is called when entering the describeInput production.
	EnterDescribeInput(c *DescribeInputContext)

	// EnterDescribeOutput is called when entering the describeOutput production.
	EnterDescribeOutput(c *DescribeOutputContext)

	// EnterQuery is called when entering the query production.
	EnterQuery(c *QueryContext)

	// EnterWith is called when entering the with production.
	EnterWith(c *WithContext)

	// EnterTableElement is called when entering the tableElement production.
	EnterTableElement(c *TableElementContext)

	// EnterColumnDefinition is called when entering the columnDefinition production.
	EnterColumnDefinition(c *ColumnDefinitionContext)

	// EnterLikeClause is called when entering the likeClause production.
	EnterLikeClause(c *LikeClauseContext)

	// EnterTableProperties is called when entering the tableProperties production.
	EnterTableProperties(c *TablePropertiesContext)

	// EnterTableProperty is called when entering the tableProperty production.
	EnterTableProperty(c *TablePropertyContext)

	// EnterQueryNoWith is called when entering the queryNoWith production.
	EnterQueryNoWith(c *QueryNoWithContext)

	// EnterQueryTermDefault is called when entering the queryTermDefault production.
	EnterQueryTermDefault(c *QueryTermDefaultContext)

	// EnterSetOperation is called when entering the setOperation production.
	EnterSetOperation(c *SetOperationContext)

	// EnterQueryPrimaryDefault is called when entering the queryPrimaryDefault production.
	EnterQueryPrimaryDefault(c *QueryPrimaryDefaultContext)

	// EnterTable is called when entering the table production.
	EnterTable(c *TableContext)

	// EnterInlineTable is called when entering the inlineTable production.
	EnterInlineTable(c *InlineTableContext)

	// EnterSubquery is called when entering the subquery production.
	EnterSubquery(c *SubqueryContext)

	// EnterSortItem is called when entering the sortItem production.
	EnterSortItem(c *SortItemContext)

	// EnterQuerySpecification is called when entering the querySpecification production.
	EnterQuerySpecification(c *QuerySpecificationContext)

	// EnterGroupBy is called when entering the groupBy production.
	EnterGroupBy(c *GroupByContext)

	// EnterSingleGroupingSet is called when entering the singleGroupingSet production.
	EnterSingleGroupingSet(c *SingleGroupingSetContext)

	// EnterRollup is called when entering the rollup production.
	EnterRollup(c *RollupContext)

	// EnterCube is called when entering the cube production.
	EnterCube(c *CubeContext)

	// EnterMultipleGroupingSets is called when entering the multipleGroupingSets production.
	EnterMultipleGroupingSets(c *MultipleGroupingSetsContext)

	// EnterGroupingExpressions is called when entering the groupingExpressions production.
	EnterGroupingExpressions(c *GroupingExpressionsContext)

	// EnterGroupingSet is called when entering the groupingSet production.
	EnterGroupingSet(c *GroupingSetContext)

	// EnterNamedQuery is called when entering the namedQuery production.
	EnterNamedQuery(c *NamedQueryContext)

	// EnterSetQuantifier is called when entering the setQuantifier production.
	EnterSetQuantifier(c *SetQuantifierContext)

	// EnterSelectSingle is called when entering the selectSingle production.
	EnterSelectSingle(c *SelectSingleContext)

	// EnterSelectAll is called when entering the selectAll production.
	EnterSelectAll(c *SelectAllContext)

	// EnterRelationDefault is called when entering the relationDefault production.
	EnterRelationDefault(c *RelationDefaultContext)

	// EnterJoinRelation is called when entering the joinRelation production.
	EnterJoinRelation(c *JoinRelationContext)

	// EnterJoinType is called when entering the joinType production.
	EnterJoinType(c *JoinTypeContext)

	// EnterJoinCriteria is called when entering the joinCriteria production.
	EnterJoinCriteria(c *JoinCriteriaContext)

	// EnterSampledRelation is called when entering the sampledRelation production.
	EnterSampledRelation(c *SampledRelationContext)

	// EnterAliasedRelation is called when entering the aliasedRelation production.
	EnterAliasedRelation(c *AliasedRelationContext)

	// EnterColumnAliases is called when entering the columnAliases production.
	EnterColumnAliases(c *ColumnAliasesContext)

	// EnterTableName is called when entering the tableName production.
	EnterTableName(c *TableNameContext)

	// EnterSubqueryRelation is called when entering the subqueryRelation production.
	EnterSubqueryRelation(c *SubqueryRelationContext)

	// EnterUnnest is called when entering the unnest production.
	EnterUnnest(c *UnnestContext)

	// EnterParenthesizedRelation is called when entering the parenthesizedRelation production.
	EnterParenthesizedRelation(c *ParenthesizedRelationContext)

	// EnterExpression is called when entering the expression production.
	EnterExpression(c *ExpressionContext)

	// EnterLogicalNot is called when entering the logicalNot production.
	EnterLogicalNot(c *LogicalNotContext)

	// EnterBooleanDefault is called when entering the booleanDefault production.
	EnterBooleanDefault(c *BooleanDefaultContext)

	// EnterBoolLiteralToo is called when entering the boolLiteralToo production.
	EnterBoolLiteralToo(c *BoolLiteralTooContext)

	// EnterLogicalBinary is called when entering the logicalBinary production.
	EnterLogicalBinary(c *LogicalBinaryContext)

	// EnterBooleanliteral is called when entering the booleanliteral production.
	EnterBooleanliteral(c *BooleanliteralContext)

	// EnterComparison is called when entering the comparison production.
	EnterComparison(c *ComparisonContext)

	// EnterQuantifiedComparison is called when entering the quantifiedComparison production.
	EnterQuantifiedComparison(c *QuantifiedComparisonContext)

	// EnterBetween is called when entering the between production.
	EnterBetween(c *BetweenContext)

	// EnterInList is called when entering the inList production.
	EnterInList(c *InListContext)

	// EnterInSubquery is called when entering the inSubquery production.
	EnterInSubquery(c *InSubqueryContext)

	// EnterLike is called when entering the like production.
	EnterLike(c *LikeContext)

	// EnterNullPredicate is called when entering the nullPredicate production.
	EnterNullPredicate(c *NullPredicateContext)

	// EnterDistinctFrom is called when entering the distinctFrom production.
	EnterDistinctFrom(c *DistinctFromContext)

	// EnterValueExpressionDefault is called when entering the valueExpressionDefault production.
	EnterValueExpressionDefault(c *ValueExpressionDefaultContext)

	// EnterConcatenation is called when entering the concatenation production.
	EnterConcatenation(c *ConcatenationContext)

	// EnterArithmeticBinary is called when entering the arithmeticBinary production.
	EnterArithmeticBinary(c *ArithmeticBinaryContext)

	// EnterArithmeticUnary is called when entering the arithmeticUnary production.
	EnterArithmeticUnary(c *ArithmeticUnaryContext)

	// EnterAtTimeZone is called when entering the atTimeZone production.
	EnterAtTimeZone(c *AtTimeZoneContext)

	// EnterDereference is called when entering the dereference production.
	EnterDereference(c *DereferenceContext)

	// EnterDecimalLiteral is called when entering the decimalLiteral production.
	EnterDecimalLiteral(c *DecimalLiteralContext)

	// EnterTypeConstructor is called when entering the typeConstructor production.
	EnterTypeConstructor(c *TypeConstructorContext)

	// EnterSpecialDateTimeFunction is called when entering the specialDateTimeFunction production.
	EnterSpecialDateTimeFunction(c *SpecialDateTimeFunctionContext)

	// EnterBoolLiteral is called when entering the boolLiteral production.
	EnterBoolLiteral(c *BoolLiteralContext)

	// EnterSubstring is called when entering the substring production.
	EnterSubstring(c *SubstringContext)

	// EnterCast is called when entering the cast production.
	EnterCast(c *CastContext)

	// EnterLambda is called when entering the lambda production.
	EnterLambda(c *LambdaContext)

	// EnterParenthesizedExpression is called when entering the parenthesizedExpression production.
	EnterParenthesizedExpression(c *ParenthesizedExpressionContext)

	// EnterParameter is called when entering the parameter production.
	EnterParameter(c *ParameterContext)

	// EnterNormalize is called when entering the normalize production.
	EnterNormalize(c *NormalizeContext)

	// EnterIntervalLiteral is called when entering the intervalLiteral production.
	EnterIntervalLiteral(c *IntervalLiteralContext)

	// EnterSimpleCase is called when entering the simpleCase production.
	EnterSimpleCase(c *SimpleCaseContext)

	// EnterColumnReference is called when entering the columnReference production.
	EnterColumnReference(c *ColumnReferenceContext)

	// EnterNullLiteral is called when entering the nullLiteral production.
	EnterNullLiteral(c *NullLiteralContext)

	// EnterTypedLiteral is called when entering the typedLiteral production.
	EnterTypedLiteral(c *TypedLiteralContext)

	// EnterRowConstructor is called when entering the rowConstructor production.
	EnterRowConstructor(c *RowConstructorContext)

	// EnterSubscript is called when entering the subscript production.
	EnterSubscript(c *SubscriptContext)

	// EnterSubqueryExpression is called when entering the subqueryExpression production.
	EnterSubqueryExpression(c *SubqueryExpressionContext)

	// EnterBinaryLiteral is called when entering the binaryLiteral production.
	EnterBinaryLiteral(c *BinaryLiteralContext)

	// EnterExtract is called when entering the extract production.
	EnterExtract(c *ExtractContext)

	// EnterStringLiteral is called when entering the stringLiteral production.
	EnterStringLiteral(c *StringLiteralContext)

	// EnterArrayConstructor is called when entering the arrayConstructor production.
	EnterArrayConstructor(c *ArrayConstructorContext)

	// EnterFunctionCall is called when entering the functionCall production.
	EnterFunctionCall(c *FunctionCallContext)

	// EnterIntegerLiteral is called when entering the integerLiteral production.
	EnterIntegerLiteral(c *IntegerLiteralContext)

	// EnterExists is called when entering the exists production.
	EnterExists(c *ExistsContext)

	// EnterPosition is called when entering the position production.
	EnterPosition(c *PositionContext)

	// EnterSearchedCase is called when entering the searchedCase production.
	EnterSearchedCase(c *SearchedCaseContext)

	// EnterTimeZoneInterval is called when entering the timeZoneInterval production.
	EnterTimeZoneInterval(c *TimeZoneIntervalContext)

	// EnterTimeZoneString is called when entering the timeZoneString production.
	EnterTimeZoneString(c *TimeZoneStringContext)

	// EnterComparisonOperator is called when entering the comparisonOperator production.
	EnterComparisonOperator(c *ComparisonOperatorContext)

	// EnterComparisonQuantifier is called when entering the comparisonQuantifier production.
	EnterComparisonQuantifier(c *ComparisonQuantifierContext)

	// EnterInterval is called when entering the interval production.
	EnterInterval(c *IntervalContext)

	// EnterIntervalField is called when entering the intervalField production.
	EnterIntervalField(c *IntervalFieldContext)

	// EnterType_t is called when entering the type_t production.
	EnterType_t(c *Type_tContext)

	// EnterTypeParameter is called when entering the typeParameter production.
	EnterTypeParameter(c *TypeParameterContext)

	// EnterBaseType is called when entering the baseType production.
	EnterBaseType(c *BaseTypeContext)

	// EnterWhenClause is called when entering the whenClause production.
	EnterWhenClause(c *WhenClauseContext)

	// EnterFilter is called when entering the filter production.
	EnterFilter(c *FilterContext)

	// EnterOver is called when entering the over production.
	EnterOver(c *OverContext)

	// EnterWindowFrame is called when entering the windowFrame production.
	EnterWindowFrame(c *WindowFrameContext)

	// EnterUnboundedFrame is called when entering the unboundedFrame production.
	EnterUnboundedFrame(c *UnboundedFrameContext)

	// EnterCurrentRowBound is called when entering the currentRowBound production.
	EnterCurrentRowBound(c *CurrentRowBoundContext)

	// EnterBoundedFrame is called when entering the boundedFrame production.
	EnterBoundedFrame(c *BoundedFrameContext)

	// EnterExplainFormat is called when entering the explainFormat production.
	EnterExplainFormat(c *ExplainFormatContext)

	// EnterExplainType is called when entering the explainType production.
	EnterExplainType(c *ExplainTypeContext)

	// EnterIsolationLevel is called when entering the isolationLevel production.
	EnterIsolationLevel(c *IsolationLevelContext)

	// EnterTransactionAccessMode is called when entering the transactionAccessMode production.
	EnterTransactionAccessMode(c *TransactionAccessModeContext)

	// EnterReadUncommitted is called when entering the readUncommitted production.
	EnterReadUncommitted(c *ReadUncommittedContext)

	// EnterReadCommitted is called when entering the readCommitted production.
	EnterReadCommitted(c *ReadCommittedContext)

	// EnterRepeatableRead is called when entering the repeatableRead production.
	EnterRepeatableRead(c *RepeatableReadContext)

	// EnterSerializable is called when entering the serializable production.
	EnterSerializable(c *SerializableContext)

	// EnterPositionalArgument is called when entering the positionalArgument production.
	EnterPositionalArgument(c *PositionalArgumentContext)

	// EnterNamedArgument is called when entering the namedArgument production.
	EnterNamedArgument(c *NamedArgumentContext)

	// EnterPrivilege is called when entering the privilege production.
	EnterPrivilege(c *PrivilegeContext)

	// EnterDotQualifiedName is called when entering the dotQualifiedName production.
	EnterDotQualifiedName(c *DotQualifiedNameContext)

	// EnterUnquotedIdentifier is called when entering the unquotedIdentifier production.
	EnterUnquotedIdentifier(c *UnquotedIdentifierContext)

	// EnterDigitIdentifier is called when entering the digitIdentifier production.
	EnterDigitIdentifier(c *DigitIdentifierContext)

	// EnterQuotedIdentifierAlternative is called when entering the quotedIdentifierAlternative production.
	EnterQuotedIdentifierAlternative(c *QuotedIdentifierAlternativeContext)

	// EnterBackQuotedIdentifier is called when entering the backQuotedIdentifier production.
	EnterBackQuotedIdentifier(c *BackQuotedIdentifierContext)

	// EnterNonReservedIdentifier is called when entering the nonReservedIdentifier production.
	EnterNonReservedIdentifier(c *NonReservedIdentifierContext)

	// EnterNonReserved is called when entering the nonReserved production.
	EnterNonReserved(c *NonReservedContext)

	// ExitStatements is called when exiting the statements production.
	ExitStatements(c *StatementsContext)

	// ExitStatementDefault is called when exiting the statementDefault production.
	ExitStatementDefault(c *StatementDefaultContext)

	// ExitCreateTable is called when exiting the createTable production.
	ExitCreateTable(c *CreateTableContext)

	// ExitCreateTableAsSelect is called when exiting the createTableAsSelect production.
	ExitCreateTableAsSelect(c *CreateTableAsSelectContext)

	// ExitDropTable is called when exiting the dropTable production.
	ExitDropTable(c *DropTableContext)

	// ExitInsertInto is called when exiting the insertInto production.
	ExitInsertInto(c *InsertIntoContext)

	// ExitDelete is called when exiting the delete production.
	ExitDelete(c *DeleteContext)

	// ExitRenameTable is called when exiting the renameTable production.
	ExitRenameTable(c *RenameTableContext)

	// ExitRenameColumn is called when exiting the renameColumn production.
	ExitRenameColumn(c *RenameColumnContext)

	// ExitAddColumn is called when exiting the addColumn production.
	ExitAddColumn(c *AddColumnContext)

	// ExitCreateView is called when exiting the createView production.
	ExitCreateView(c *CreateViewContext)

	// ExitDropView is called when exiting the dropView production.
	ExitDropView(c *DropViewContext)

	// ExitCall is called when exiting the call production.
	ExitCall(c *CallContext)

	// ExitGrant is called when exiting the grant production.
	ExitGrant(c *GrantContext)

	// ExitRevoke is called when exiting the revoke production.
	ExitRevoke(c *RevokeContext)

	// ExitExplain is called when exiting the explain production.
	ExitExplain(c *ExplainContext)

	// ExitShowCreateTable is called when exiting the showCreateTable production.
	ExitShowCreateTable(c *ShowCreateTableContext)

	// ExitShowCreateView is called when exiting the showCreateView production.
	ExitShowCreateView(c *ShowCreateViewContext)

	// ExitShowTables is called when exiting the showTables production.
	ExitShowTables(c *ShowTablesContext)

	// ExitShowSchemas is called when exiting the showSchemas production.
	ExitShowSchemas(c *ShowSchemasContext)

	// ExitShowCatalogs is called when exiting the showCatalogs production.
	ExitShowCatalogs(c *ShowCatalogsContext)

	// ExitShowColumns is called when exiting the showColumns production.
	ExitShowColumns(c *ShowColumnsContext)

	// ExitShowFunctions is called when exiting the showFunctions production.
	ExitShowFunctions(c *ShowFunctionsContext)

	// ExitShowSession is called when exiting the showSession production.
	ExitShowSession(c *ShowSessionContext)

	// ExitSetSession is called when exiting the setSession production.
	ExitSetSession(c *SetSessionContext)

	// ExitResetSession is called when exiting the resetSession production.
	ExitResetSession(c *ResetSessionContext)

	// ExitStartTransaction is called when exiting the startTransaction production.
	ExitStartTransaction(c *StartTransactionContext)

	// ExitCommit is called when exiting the commit production.
	ExitCommit(c *CommitContext)

	// ExitRollback is called when exiting the rollback production.
	ExitRollback(c *RollbackContext)

	// ExitShowPartitions is called when exiting the showPartitions production.
	ExitShowPartitions(c *ShowPartitionsContext)

	// ExitPrepare is called when exiting the prepare production.
	ExitPrepare(c *PrepareContext)

	// ExitDeallocate is called when exiting the deallocate production.
	ExitDeallocate(c *DeallocateContext)

	// ExitExecute is called when exiting the execute production.
	ExitExecute(c *ExecuteContext)

	// ExitDescribeInput is called when exiting the describeInput production.
	ExitDescribeInput(c *DescribeInputContext)

	// ExitDescribeOutput is called when exiting the describeOutput production.
	ExitDescribeOutput(c *DescribeOutputContext)

	// ExitQuery is called when exiting the query production.
	ExitQuery(c *QueryContext)

	// ExitWith is called when exiting the with production.
	ExitWith(c *WithContext)

	// ExitTableElement is called when exiting the tableElement production.
	ExitTableElement(c *TableElementContext)

	// ExitColumnDefinition is called when exiting the columnDefinition production.
	ExitColumnDefinition(c *ColumnDefinitionContext)

	// ExitLikeClause is called when exiting the likeClause production.
	ExitLikeClause(c *LikeClauseContext)

	// ExitTableProperties is called when exiting the tableProperties production.
	ExitTableProperties(c *TablePropertiesContext)

	// ExitTableProperty is called when exiting the tableProperty production.
	ExitTableProperty(c *TablePropertyContext)

	// ExitQueryNoWith is called when exiting the queryNoWith production.
	ExitQueryNoWith(c *QueryNoWithContext)

	// ExitQueryTermDefault is called when exiting the queryTermDefault production.
	ExitQueryTermDefault(c *QueryTermDefaultContext)

	// ExitSetOperation is called when exiting the setOperation production.
	ExitSetOperation(c *SetOperationContext)

	// ExitQueryPrimaryDefault is called when exiting the queryPrimaryDefault production.
	ExitQueryPrimaryDefault(c *QueryPrimaryDefaultContext)

	// ExitTable is called when exiting the table production.
	ExitTable(c *TableContext)

	// ExitInlineTable is called when exiting the inlineTable production.
	ExitInlineTable(c *InlineTableContext)

	// ExitSubquery is called when exiting the subquery production.
	ExitSubquery(c *SubqueryContext)

	// ExitSortItem is called when exiting the sortItem production.
	ExitSortItem(c *SortItemContext)

	// ExitQuerySpecification is called when exiting the querySpecification production.
	ExitQuerySpecification(c *QuerySpecificationContext)

	// ExitGroupBy is called when exiting the groupBy production.
	ExitGroupBy(c *GroupByContext)

	// ExitSingleGroupingSet is called when exiting the singleGroupingSet production.
	ExitSingleGroupingSet(c *SingleGroupingSetContext)

	// ExitRollup is called when exiting the rollup production.
	ExitRollup(c *RollupContext)

	// ExitCube is called when exiting the cube production.
	ExitCube(c *CubeContext)

	// ExitMultipleGroupingSets is called when exiting the multipleGroupingSets production.
	ExitMultipleGroupingSets(c *MultipleGroupingSetsContext)

	// ExitGroupingExpressions is called when exiting the groupingExpressions production.
	ExitGroupingExpressions(c *GroupingExpressionsContext)

	// ExitGroupingSet is called when exiting the groupingSet production.
	ExitGroupingSet(c *GroupingSetContext)

	// ExitNamedQuery is called when exiting the namedQuery production.
	ExitNamedQuery(c *NamedQueryContext)

	// ExitSetQuantifier is called when exiting the setQuantifier production.
	ExitSetQuantifier(c *SetQuantifierContext)

	// ExitSelectSingle is called when exiting the selectSingle production.
	ExitSelectSingle(c *SelectSingleContext)

	// ExitSelectAll is called when exiting the selectAll production.
	ExitSelectAll(c *SelectAllContext)

	// ExitRelationDefault is called when exiting the relationDefault production.
	ExitRelationDefault(c *RelationDefaultContext)

	// ExitJoinRelation is called when exiting the joinRelation production.
	ExitJoinRelation(c *JoinRelationContext)

	// ExitJoinType is called when exiting the joinType production.
	ExitJoinType(c *JoinTypeContext)

	// ExitJoinCriteria is called when exiting the joinCriteria production.
	ExitJoinCriteria(c *JoinCriteriaContext)

	// ExitSampledRelation is called when exiting the sampledRelation production.
	ExitSampledRelation(c *SampledRelationContext)

	// ExitAliasedRelation is called when exiting the aliasedRelation production.
	ExitAliasedRelation(c *AliasedRelationContext)

	// ExitColumnAliases is called when exiting the columnAliases production.
	ExitColumnAliases(c *ColumnAliasesContext)

	// ExitTableName is called when exiting the tableName production.
	ExitTableName(c *TableNameContext)

	// ExitSubqueryRelation is called when exiting the subqueryRelation production.
	ExitSubqueryRelation(c *SubqueryRelationContext)

	// ExitUnnest is called when exiting the unnest production.
	ExitUnnest(c *UnnestContext)

	// ExitParenthesizedRelation is called when exiting the parenthesizedRelation production.
	ExitParenthesizedRelation(c *ParenthesizedRelationContext)

	// ExitExpression is called when exiting the expression production.
	ExitExpression(c *ExpressionContext)

	// ExitLogicalNot is called when exiting the logicalNot production.
	ExitLogicalNot(c *LogicalNotContext)

	// ExitBooleanDefault is called when exiting the booleanDefault production.
	ExitBooleanDefault(c *BooleanDefaultContext)

	// ExitBoolLiteralToo is called when exiting the boolLiteralToo production.
	ExitBoolLiteralToo(c *BoolLiteralTooContext)

	// ExitLogicalBinary is called when exiting the logicalBinary production.
	ExitLogicalBinary(c *LogicalBinaryContext)

	// ExitBooleanliteral is called when exiting the booleanliteral production.
	ExitBooleanliteral(c *BooleanliteralContext)

	// ExitComparison is called when exiting the comparison production.
	ExitComparison(c *ComparisonContext)

	// ExitQuantifiedComparison is called when exiting the quantifiedComparison production.
	ExitQuantifiedComparison(c *QuantifiedComparisonContext)

	// ExitBetween is called when exiting the between production.
	ExitBetween(c *BetweenContext)

	// ExitInList is called when exiting the inList production.
	ExitInList(c *InListContext)

	// ExitInSubquery is called when exiting the inSubquery production.
	ExitInSubquery(c *InSubqueryContext)

	// ExitLike is called when exiting the like production.
	ExitLike(c *LikeContext)

	// ExitNullPredicate is called when exiting the nullPredicate production.
	ExitNullPredicate(c *NullPredicateContext)

	// ExitDistinctFrom is called when exiting the distinctFrom production.
	ExitDistinctFrom(c *DistinctFromContext)

	// ExitValueExpressionDefault is called when exiting the valueExpressionDefault production.
	ExitValueExpressionDefault(c *ValueExpressionDefaultContext)

	// ExitConcatenation is called when exiting the concatenation production.
	ExitConcatenation(c *ConcatenationContext)

	// ExitArithmeticBinary is called when exiting the arithmeticBinary production.
	ExitArithmeticBinary(c *ArithmeticBinaryContext)

	// ExitArithmeticUnary is called when exiting the arithmeticUnary production.
	ExitArithmeticUnary(c *ArithmeticUnaryContext)

	// ExitAtTimeZone is called when exiting the atTimeZone production.
	ExitAtTimeZone(c *AtTimeZoneContext)

	// ExitDereference is called when exiting the dereference production.
	ExitDereference(c *DereferenceContext)

	// ExitDecimalLiteral is called when exiting the decimalLiteral production.
	ExitDecimalLiteral(c *DecimalLiteralContext)

	// ExitTypeConstructor is called when exiting the typeConstructor production.
	ExitTypeConstructor(c *TypeConstructorContext)

	// ExitSpecialDateTimeFunction is called when exiting the specialDateTimeFunction production.
	ExitSpecialDateTimeFunction(c *SpecialDateTimeFunctionContext)

	// ExitBoolLiteral is called when exiting the boolLiteral production.
	ExitBoolLiteral(c *BoolLiteralContext)

	// ExitSubstring is called when exiting the substring production.
	ExitSubstring(c *SubstringContext)

	// ExitCast is called when exiting the cast production.
	ExitCast(c *CastContext)

	// ExitLambda is called when exiting the lambda production.
	ExitLambda(c *LambdaContext)

	// ExitParenthesizedExpression is called when exiting the parenthesizedExpression production.
	ExitParenthesizedExpression(c *ParenthesizedExpressionContext)

	// ExitParameter is called when exiting the parameter production.
	ExitParameter(c *ParameterContext)

	// ExitNormalize is called when exiting the normalize production.
	ExitNormalize(c *NormalizeContext)

	// ExitIntervalLiteral is called when exiting the intervalLiteral production.
	ExitIntervalLiteral(c *IntervalLiteralContext)

	// ExitSimpleCase is called when exiting the simpleCase production.
	ExitSimpleCase(c *SimpleCaseContext)

	// ExitColumnReference is called when exiting the columnReference production.
	ExitColumnReference(c *ColumnReferenceContext)

	// ExitNullLiteral is called when exiting the nullLiteral production.
	ExitNullLiteral(c *NullLiteralContext)

	// ExitTypedLiteral is called when exiting the typedLiteral production.
	ExitTypedLiteral(c *TypedLiteralContext)

	// ExitRowConstructor is called when exiting the rowConstructor production.
	ExitRowConstructor(c *RowConstructorContext)

	// ExitSubscript is called when exiting the subscript production.
	ExitSubscript(c *SubscriptContext)

	// ExitSubqueryExpression is called when exiting the subqueryExpression production.
	ExitSubqueryExpression(c *SubqueryExpressionContext)

	// ExitBinaryLiteral is called when exiting the binaryLiteral production.
	ExitBinaryLiteral(c *BinaryLiteralContext)

	// ExitExtract is called when exiting the extract production.
	ExitExtract(c *ExtractContext)

	// ExitStringLiteral is called when exiting the stringLiteral production.
	ExitStringLiteral(c *StringLiteralContext)

	// ExitArrayConstructor is called when exiting the arrayConstructor production.
	ExitArrayConstructor(c *ArrayConstructorContext)

	// ExitFunctionCall is called when exiting the functionCall production.
	ExitFunctionCall(c *FunctionCallContext)

	// ExitIntegerLiteral is called when exiting the integerLiteral production.
	ExitIntegerLiteral(c *IntegerLiteralContext)

	// ExitExists is called when exiting the exists production.
	ExitExists(c *ExistsContext)

	// ExitPosition is called when exiting the position production.
	ExitPosition(c *PositionContext)

	// ExitSearchedCase is called when exiting the searchedCase production.
	ExitSearchedCase(c *SearchedCaseContext)

	// ExitTimeZoneInterval is called when exiting the timeZoneInterval production.
	ExitTimeZoneInterval(c *TimeZoneIntervalContext)

	// ExitTimeZoneString is called when exiting the timeZoneString production.
	ExitTimeZoneString(c *TimeZoneStringContext)

	// ExitComparisonOperator is called when exiting the comparisonOperator production.
	ExitComparisonOperator(c *ComparisonOperatorContext)

	// ExitComparisonQuantifier is called when exiting the comparisonQuantifier production.
	ExitComparisonQuantifier(c *ComparisonQuantifierContext)

	// ExitInterval is called when exiting the interval production.
	ExitInterval(c *IntervalContext)

	// ExitIntervalField is called when exiting the intervalField production.
	ExitIntervalField(c *IntervalFieldContext)

	// ExitType_t is called when exiting the type_t production.
	ExitType_t(c *Type_tContext)

	// ExitTypeParameter is called when exiting the typeParameter production.
	ExitTypeParameter(c *TypeParameterContext)

	// ExitBaseType is called when exiting the baseType production.
	ExitBaseType(c *BaseTypeContext)

	// ExitWhenClause is called when exiting the whenClause production.
	ExitWhenClause(c *WhenClauseContext)

	// ExitFilter is called when exiting the filter production.
	ExitFilter(c *FilterContext)

	// ExitOver is called when exiting the over production.
	ExitOver(c *OverContext)

	// ExitWindowFrame is called when exiting the windowFrame production.
	ExitWindowFrame(c *WindowFrameContext)

	// ExitUnboundedFrame is called when exiting the unboundedFrame production.
	ExitUnboundedFrame(c *UnboundedFrameContext)

	// ExitCurrentRowBound is called when exiting the currentRowBound production.
	ExitCurrentRowBound(c *CurrentRowBoundContext)

	// ExitBoundedFrame is called when exiting the boundedFrame production.
	ExitBoundedFrame(c *BoundedFrameContext)

	// ExitExplainFormat is called when exiting the explainFormat production.
	ExitExplainFormat(c *ExplainFormatContext)

	// ExitExplainType is called when exiting the explainType production.
	ExitExplainType(c *ExplainTypeContext)

	// ExitIsolationLevel is called when exiting the isolationLevel production.
	ExitIsolationLevel(c *IsolationLevelContext)

	// ExitTransactionAccessMode is called when exiting the transactionAccessMode production.
	ExitTransactionAccessMode(c *TransactionAccessModeContext)

	// ExitReadUncommitted is called when exiting the readUncommitted production.
	ExitReadUncommitted(c *ReadUncommittedContext)

	// ExitReadCommitted is called when exiting the readCommitted production.
	ExitReadCommitted(c *ReadCommittedContext)

	// ExitRepeatableRead is called when exiting the repeatableRead production.
	ExitRepeatableRead(c *RepeatableReadContext)

	// ExitSerializable is called when exiting the serializable production.
	ExitSerializable(c *SerializableContext)

	// ExitPositionalArgument is called when exiting the positionalArgument production.
	ExitPositionalArgument(c *PositionalArgumentContext)

	// ExitNamedArgument is called when exiting the namedArgument production.
	ExitNamedArgument(c *NamedArgumentContext)

	// ExitPrivilege is called when exiting the privilege production.
	ExitPrivilege(c *PrivilegeContext)

	// ExitDotQualifiedName is called when exiting the dotQualifiedName production.
	ExitDotQualifiedName(c *DotQualifiedNameContext)

	// ExitUnquotedIdentifier is called when exiting the unquotedIdentifier production.
	ExitUnquotedIdentifier(c *UnquotedIdentifierContext)

	// ExitDigitIdentifier is called when exiting the digitIdentifier production.
	ExitDigitIdentifier(c *DigitIdentifierContext)

	// ExitQuotedIdentifierAlternative is called when exiting the quotedIdentifierAlternative production.
	ExitQuotedIdentifierAlternative(c *QuotedIdentifierAlternativeContext)

	// ExitBackQuotedIdentifier is called when exiting the backQuotedIdentifier production.
	ExitBackQuotedIdentifier(c *BackQuotedIdentifierContext)

	// ExitNonReservedIdentifier is called when exiting the nonReservedIdentifier production.
	ExitNonReservedIdentifier(c *NonReservedIdentifierContext)

	// ExitNonReserved is called when exiting the nonReserved production.
	ExitNonReserved(c *NonReservedContext)
}
