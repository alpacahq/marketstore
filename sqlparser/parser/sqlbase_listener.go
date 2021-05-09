// Code generated from parser/SQLBase.g4 by ANTLR 4.9.2. DO NOT EDIT.

package parser // SQLBase

import "github.com/antlr/antlr4/runtime/Go/antlr"

// SQLBaseListener is a complete listener for a parse tree produced by SQLBaseParser.
type SQLBaseListener interface {
	antlr.ParseTreeListener

	// EnterStatements is called when entering the statements production.
	EnterStatements(c *StatementsContext)

	// EnterStatementDefault is called when entering the statementDefault production.
	EnterStatementDefault(c *StatementDefaultContext)

	// EnterInsertInto is called when entering the insertInto production.
	EnterInsertInto(c *InsertIntoContext)

	// EnterExplain is called when entering the explain production.
	EnterExplain(c *ExplainContext)

	// EnterQuery is called when entering the query production.
	EnterQuery(c *QueryContext)

	// EnterWith is called when entering the with production.
	EnterWith(c *WithContext)

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

	// ExitInsertInto is called when exiting the insertInto production.
	ExitInsertInto(c *InsertIntoContext)

	// ExitExplain is called when exiting the explain production.
	ExitExplain(c *ExplainContext)

	// ExitQuery is called when exiting the query production.
	ExitQuery(c *QueryContext)

	// ExitWith is called when exiting the with production.
	ExitWith(c *WithContext)

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
