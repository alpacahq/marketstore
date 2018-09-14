package sqlparser

func (this *StatementsParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitStatementsParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *StatementParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitStatementParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *QueryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *WithParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitWithParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *TableElementParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTableElementParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ColumnDefinitionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitColumnDefinitionParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *LikeClauseParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitLikeClauseParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *TablePropertiesParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTablePropertiesParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *TablePropertyParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTablePropertyParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *QueryNoWithParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryNoWithParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *QueryTermParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryTermParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *QueryPrimaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryPrimaryParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SortItemParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSortItemParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *QuerySpecificationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQuerySpecificationParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *GroupByParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupByParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *GroupingElementParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupingElementParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *GroupingExpressionsParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupingExpressionsParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *GroupingSetParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupingSetParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *NamedQueryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitNamedQueryParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SelectItemParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSelectItemParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *RelationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitRelationParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *JoinCriteriaParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitJoinCriteriaParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SampledRelationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSampledRelationParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *AliasedRelationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitAliasedRelationParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ColumnAliasesParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitColumnAliasesParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *RelationPrimaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitRelationPrimaryParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitExpressionParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *BooleanExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitBooleanExpressionParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *PredicateParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitPredicateParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ComparisonParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitComparisonParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *QuantifiedComparisonParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQuantifiedComparisonParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *BetweenParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitBetweenParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *InListParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitInListParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *InSubqueryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitInSubqueryParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *LikeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitLikeParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *NullPredicateParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitNullPredicateParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *DistinctFromParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitDistinctFromParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ValueExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitValueExpressionParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *AtTimeZoneParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitAtTimeZoneParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ArithmeticUnaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitArithmeticUnaryParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ArithmeticBinaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitArithmeticBinaryParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ConcatenationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitConcatenationParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *PrimaryExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitPrimaryExpressionParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *FunctionCallParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitFunctionCallParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *LambdaParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitLambdaParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SimpleCaseParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSimpleCaseParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SearchedCaseParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSearchedCaseParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *CastParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitCastParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SubscriptParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSubscriptParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *DereferenceParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitDereferenceParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SpecialDateTimeFunctionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSpecialDateTimeFunctionParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *SubstringParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSubstringParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *NormalizeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitNormalizeParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ExtractParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitExtractParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *TimeZoneSpecifierParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTimeZoneSpecifierParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ComparisonOperatorParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitComparisonOperatorParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ComparisonQuantifierParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitComparisonQuantifierParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *IntervalParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitIntervalParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *IntervalFieldParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitIntervalFieldParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *TypeTParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTypeTParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *TypeParameterParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTypeParameterParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *BaseTypeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitBaseTypeParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *WhenParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitWhenParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *FilterParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitFilterParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *OverParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitOverParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *WindowFrameParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitWindowFrameParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *FrameBoundParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitFrameBoundParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *ExplainOptionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitExplainOptionParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *TransactionModeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTransactionModeParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *LevelOfIsolationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitLevelOfIsolationParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *CallArgParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitCallArgParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *PrivilegeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitPrivilegeParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *QualifiedNameParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQualifiedNameParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *IDParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitIDParse(this)
	default:
		return t.VisitChildren(this)
	}
}
func (this *NonReservedParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitNonReservedParse(this)
	default:
		return t.VisitChildren(this)
	}
}

type ISQLQueryTreeVisitor interface {
	IMSTreeVisitor
	VisitStatementsParse(ctx *StatementsParse) interface{}
	VisitStatementParse(ctx *StatementParse) interface{}
	VisitQueryParse(ctx *QueryParse) interface{}
	VisitWithParse(ctx *WithParse) interface{}
	VisitTableElementParse(ctx *TableElementParse) interface{}
	VisitColumnDefinitionParse(ctx *ColumnDefinitionParse) interface{}
	VisitLikeClauseParse(ctx *LikeClauseParse) interface{}
	VisitTablePropertiesParse(ctx *TablePropertiesParse) interface{}
	VisitTablePropertyParse(ctx *TablePropertyParse) interface{}
	VisitQueryNoWithParse(ctx *QueryNoWithParse) interface{}
	VisitQueryTermParse(ctx *QueryTermParse) interface{}
	VisitQueryPrimaryParse(ctx *QueryPrimaryParse) interface{}
	VisitSortItemParse(ctx *SortItemParse) interface{}
	VisitQuerySpecificationParse(ctx *QuerySpecificationParse) interface{}
	VisitGroupByParse(ctx *GroupByParse) interface{}
	VisitGroupingElementParse(ctx *GroupingElementParse) interface{}
	VisitGroupingExpressionsParse(ctx *GroupingExpressionsParse) interface{}
	VisitGroupingSetParse(ctx *GroupingSetParse) interface{}
	VisitNamedQueryParse(ctx *NamedQueryParse) interface{}
	VisitSelectItemParse(ctx *SelectItemParse) interface{}
	VisitRelationParse(ctx *RelationParse) interface{}
	VisitJoinCriteriaParse(ctx *JoinCriteriaParse) interface{}
	VisitSampledRelationParse(ctx *SampledRelationParse) interface{}
	VisitAliasedRelationParse(ctx *AliasedRelationParse) interface{}
	VisitColumnAliasesParse(ctx *ColumnAliasesParse) interface{}
	VisitRelationPrimaryParse(ctx *RelationPrimaryParse) interface{}
	VisitExpressionParse(ctx *ExpressionParse) interface{}
	VisitBooleanExpressionParse(ctx *BooleanExpressionParse) interface{}
	VisitPredicateParse(ctx *PredicateParse) interface{}
	VisitComparisonParse(ctx *ComparisonParse) interface{}
	VisitQuantifiedComparisonParse(ctx *QuantifiedComparisonParse) interface{}
	VisitBetweenParse(ctx *BetweenParse) interface{}
	VisitInListParse(ctx *InListParse) interface{}
	VisitInSubqueryParse(ctx *InSubqueryParse) interface{}
	VisitLikeParse(ctx *LikeParse) interface{}
	VisitNullPredicateParse(ctx *NullPredicateParse) interface{}
	VisitDistinctFromParse(ctx *DistinctFromParse) interface{}
	VisitValueExpressionParse(ctx *ValueExpressionParse) interface{}
	VisitAtTimeZoneParse(ctx *AtTimeZoneParse) interface{}
	VisitArithmeticUnaryParse(ctx *ArithmeticUnaryParse) interface{}
	VisitArithmeticBinaryParse(ctx *ArithmeticBinaryParse) interface{}
	VisitConcatenationParse(ctx *ConcatenationParse) interface{}
	VisitPrimaryExpressionParse(ctx *PrimaryExpressionParse) interface{}
	VisitFunctionCallParse(ctx *FunctionCallParse) interface{}
	VisitLambdaParse(ctx *LambdaParse) interface{}
	VisitSimpleCaseParse(ctx *SimpleCaseParse) interface{}
	VisitSearchedCaseParse(ctx *SearchedCaseParse) interface{}
	VisitCastParse(ctx *CastParse) interface{}
	VisitSubscriptParse(ctx *SubscriptParse) interface{}
	VisitDereferenceParse(ctx *DereferenceParse) interface{}
	VisitSpecialDateTimeFunctionParse(ctx *SpecialDateTimeFunctionParse) interface{}
	VisitSubstringParse(ctx *SubstringParse) interface{}
	VisitNormalizeParse(ctx *NormalizeParse) interface{}
	VisitExtractParse(ctx *ExtractParse) interface{}
	VisitTimeZoneSpecifierParse(ctx *TimeZoneSpecifierParse) interface{}
	VisitComparisonOperatorParse(ctx *ComparisonOperatorParse) interface{}
	VisitComparisonQuantifierParse(ctx *ComparisonQuantifierParse) interface{}
	VisitIntervalParse(ctx *IntervalParse) interface{}
	VisitIntervalFieldParse(ctx *IntervalFieldParse) interface{}
	VisitTypeTParse(ctx *TypeTParse) interface{}
	VisitTypeParameterParse(ctx *TypeParameterParse) interface{}
	VisitBaseTypeParse(ctx *BaseTypeParse) interface{}
	VisitWhenParse(ctx *WhenParse) interface{}
	VisitFilterParse(ctx *FilterParse) interface{}
	VisitOverParse(ctx *OverParse) interface{}
	VisitWindowFrameParse(ctx *WindowFrameParse) interface{}
	VisitFrameBoundParse(ctx *FrameBoundParse) interface{}
	VisitExplainOptionParse(ctx *ExplainOptionParse) interface{}
	VisitTransactionModeParse(ctx *TransactionModeParse) interface{}
	VisitLevelOfIsolationParse(ctx *LevelOfIsolationParse) interface{}
	VisitCallArgParse(ctx *CallArgParse) interface{}
	VisitPrivilegeParse(ctx *PrivilegeParse) interface{}
	VisitQualifiedNameParse(ctx *QualifiedNameParse) interface{}
	VisitIDParse(ctx *IDParse) interface{}
	VisitNonReservedParse(ctx *NonReservedParse) interface{}
}

type BaseSQLQueryTreeVisitor struct {
	*BaseMSTreeVisitor
}

var _ ISQLQueryTreeVisitor = &BaseSQLQueryTreeVisitor{}

func (this *BaseSQLQueryTreeVisitor) Visit(tree IMSTree) interface{} {
	return tree.Accept(this)
}

func (this *BaseSQLQueryTreeVisitor) VisitChildren(tree IMSTree) interface{} {
	for _, child := range tree.GetChildren() {
		retval := child.Accept(this)
		if retval != nil {
			return retval
		}
	}
	return nil
}

func (this *BaseSQLQueryTreeVisitor) VisitStatementsParse(ctx *StatementsParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitStatementParse(ctx *StatementParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitQueryParse(ctx *QueryParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitWithParse(ctx *WithParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitTableElementParse(ctx *TableElementParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitColumnDefinitionParse(ctx *ColumnDefinitionParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitLikeClauseParse(ctx *LikeClauseParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitTablePropertiesParse(ctx *TablePropertiesParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitTablePropertyParse(ctx *TablePropertyParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitQueryNoWithParse(ctx *QueryNoWithParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitQueryTermParse(ctx *QueryTermParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitQueryPrimaryParse(ctx *QueryPrimaryParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSortItemParse(ctx *SortItemParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitQuerySpecificationParse(ctx *QuerySpecificationParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitGroupByParse(ctx *GroupByParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitGroupingElementParse(ctx *GroupingElementParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitGroupingExpressionsParse(ctx *GroupingExpressionsParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitGroupingSetParse(ctx *GroupingSetParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitNamedQueryParse(ctx *NamedQueryParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSelectItemParse(ctx *SelectItemParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitRelationParse(ctx *RelationParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitJoinCriteriaParse(ctx *JoinCriteriaParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSampledRelationParse(ctx *SampledRelationParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitAliasedRelationParse(ctx *AliasedRelationParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitColumnAliasesParse(ctx *ColumnAliasesParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitRelationPrimaryParse(ctx *RelationPrimaryParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitExpressionParse(ctx *ExpressionParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitBooleanExpressionParse(ctx *BooleanExpressionParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitPredicateParse(ctx *PredicateParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitComparisonParse(ctx *ComparisonParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitQuantifiedComparisonParse(ctx *QuantifiedComparisonParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitBetweenParse(ctx *BetweenParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitInListParse(ctx *InListParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitInSubqueryParse(ctx *InSubqueryParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitLikeParse(ctx *LikeParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitNullPredicateParse(ctx *NullPredicateParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitDistinctFromParse(ctx *DistinctFromParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitValueExpressionParse(ctx *ValueExpressionParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitAtTimeZoneParse(ctx *AtTimeZoneParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitArithmeticUnaryParse(ctx *ArithmeticUnaryParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitArithmeticBinaryParse(ctx *ArithmeticBinaryParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitConcatenationParse(ctx *ConcatenationParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitPrimaryExpressionParse(ctx *PrimaryExpressionParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitFunctionCallParse(ctx *FunctionCallParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitLambdaParse(ctx *LambdaParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSimpleCaseParse(ctx *SimpleCaseParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSearchedCaseParse(ctx *SearchedCaseParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitCastParse(ctx *CastParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSubscriptParse(ctx *SubscriptParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitDereferenceParse(ctx *DereferenceParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSpecialDateTimeFunctionParse(ctx *SpecialDateTimeFunctionParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitSubstringParse(ctx *SubstringParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitNormalizeParse(ctx *NormalizeParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitExtractParse(ctx *ExtractParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitTimeZoneSpecifierParse(ctx *TimeZoneSpecifierParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitComparisonOperatorParse(ctx *ComparisonOperatorParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitComparisonQuantifierParse(ctx *ComparisonQuantifierParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitIntervalParse(ctx *IntervalParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitIntervalFieldParse(ctx *IntervalFieldParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitTypeTParse(ctx *TypeTParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitTypeParameterParse(ctx *TypeParameterParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitBaseTypeParse(ctx *BaseTypeParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitWhenParse(ctx *WhenParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitFilterParse(ctx *FilterParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitOverParse(ctx *OverParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitWindowFrameParse(ctx *WindowFrameParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitFrameBoundParse(ctx *FrameBoundParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitExplainOptionParse(ctx *ExplainOptionParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitTransactionModeParse(ctx *TransactionModeParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitLevelOfIsolationParse(ctx *LevelOfIsolationParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitCallArgParse(ctx *CallArgParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitPrivilegeParse(ctx *PrivilegeParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitQualifiedNameParse(ctx *QualifiedNameParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitIDParse(ctx *IDParse) interface{} {
	return this.VisitChildren(ctx)
}
func (this *BaseSQLQueryTreeVisitor) VisitNonReservedParse(ctx *NonReservedParse) interface{} {
	return this.VisitChildren(ctx)
}
