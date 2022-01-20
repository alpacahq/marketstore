package sqlparser

func (v *StatementsParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitStatementsParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *StatementParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitStatementParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *QueryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *QueryNoWithParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryNoWithParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *QueryTermParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryTermParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *QueryPrimaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQueryPrimaryParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SortItemParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSortItemParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *QuerySpecificationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQuerySpecificationParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *GroupByParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupByParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *GroupingElementParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupingElementParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *GroupingExpressionsParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupingExpressionsParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *GroupingSetParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitGroupingSetParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SelectItemParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSelectItemParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *RelationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitRelationParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *JoinCriteriaParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitJoinCriteriaParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SampledRelationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSampledRelationParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *AliasedRelationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitAliasedRelationParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ColumnAliasesParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitColumnAliasesParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *RelationPrimaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitRelationPrimaryParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitExpressionParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *BooleanExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitBooleanExpressionParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *PredicateParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitPredicateParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ComparisonParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitComparisonParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *QuantifiedComparisonParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQuantifiedComparisonParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *BetweenParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitBetweenParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *InListParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitInListParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *InSubqueryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitInSubqueryParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *LikeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitLikeParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *NullPredicateParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitNullPredicateParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *DistinctFromParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitDistinctFromParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ValueExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitValueExpressionParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *AtTimeZoneParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitAtTimeZoneParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ArithmeticUnaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitArithmeticUnaryParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ArithmeticBinaryParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitArithmeticBinaryParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ConcatenationParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitConcatenationParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *PrimaryExpressionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitPrimaryExpressionParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *FunctionCallParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitFunctionCallParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *LambdaParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitLambdaParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SimpleCaseParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSimpleCaseParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SearchedCaseParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSearchedCaseParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *CastParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitCastParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SubscriptParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSubscriptParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *DereferenceParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitDereferenceParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SpecialDateTimeFunctionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSpecialDateTimeFunctionParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *SubstringParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitSubstringParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *NormalizeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitNormalizeParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ExtractParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitExtractParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *TimeZoneSpecifierParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTimeZoneSpecifierParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *IntervalParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitIntervalParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *IntervalFieldParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitIntervalFieldParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *TypeTParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTypeTParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *TypeParameterParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitTypeParameterParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *BaseTypeParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitBaseTypeParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *WhenParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitWhenParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *FilterParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitFilterParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *OverParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitOverParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *WindowFrameParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitWindowFrameParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *FrameBoundParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitFrameBoundParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *ExplainOptionParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitExplainOptionParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *QualifiedNameParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitQualifiedNameParse(v)
	default:
		return t.VisitChildren(v)
	}
}
func (v *IDParse) Accept(visitor IMSTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case ISQLQueryTreeVisitor:
		return t.VisitIDParse(v)
	default:
		return t.VisitChildren(v)
	}
}

type ISQLQueryTreeVisitor interface {
	IMSTreeVisitor
	VisitStatementsParse(ctx *StatementsParse) interface{}
	VisitStatementParse(ctx *StatementParse) interface{}
	VisitQueryParse(ctx *QueryParse) interface{}
	VisitQueryNoWithParse(ctx *QueryNoWithParse) interface{}
	VisitQueryTermParse(ctx *QueryTermParse) interface{}
	VisitQueryPrimaryParse(ctx *QueryPrimaryParse) interface{}
	VisitSortItemParse(ctx *SortItemParse) interface{}
	VisitQuerySpecificationParse(ctx *QuerySpecificationParse) interface{}
	VisitGroupByParse(ctx *GroupByParse) interface{}
	VisitGroupingElementParse(ctx *GroupingElementParse) interface{}
	VisitGroupingExpressionsParse(ctx *GroupingExpressionsParse) interface{}
	VisitGroupingSetParse(ctx *GroupingSetParse) interface{}
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
	VisitQualifiedNameParse(ctx *QualifiedNameParse) interface{}
	VisitIDParse(ctx *IDParse) interface{}
}

type BaseSQLQueryTreeVisitor struct {
	*BaseMSTreeVisitor
}

var _ ISQLQueryTreeVisitor = &BaseSQLQueryTreeVisitor{}

func (v *BaseSQLQueryTreeVisitor) Visit(tree IMSTree) interface{} {
	return tree.Accept(v)
}

func (v *BaseSQLQueryTreeVisitor) VisitChildren(tree IMSTree) interface{} {
	for _, child := range tree.GetChildren() {
		retval := child.Accept(v)
		if retval != nil {
			return retval
		}
	}
	return nil
}

func (v *BaseSQLQueryTreeVisitor) VisitStatementsParse(ctx *StatementsParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitStatementParse(ctx *StatementParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitQueryParse(ctx *QueryParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitQueryNoWithParse(ctx *QueryNoWithParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitQueryTermParse(ctx *QueryTermParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitQueryPrimaryParse(ctx *QueryPrimaryParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSortItemParse(ctx *SortItemParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitQuerySpecificationParse(ctx *QuerySpecificationParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitGroupByParse(ctx *GroupByParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitGroupingElementParse(ctx *GroupingElementParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitGroupingExpressionsParse(ctx *GroupingExpressionsParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitGroupingSetParse(ctx *GroupingSetParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSelectItemParse(ctx *SelectItemParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitRelationParse(ctx *RelationParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitJoinCriteriaParse(ctx *JoinCriteriaParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSampledRelationParse(ctx *SampledRelationParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitAliasedRelationParse(ctx *AliasedRelationParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitColumnAliasesParse(ctx *ColumnAliasesParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitRelationPrimaryParse(ctx *RelationPrimaryParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitExpressionParse(ctx *ExpressionParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitBooleanExpressionParse(ctx *BooleanExpressionParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitPredicateParse(ctx *PredicateParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitComparisonParse(ctx *ComparisonParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitQuantifiedComparisonParse(ctx *QuantifiedComparisonParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitBetweenParse(ctx *BetweenParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitInListParse(ctx *InListParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitInSubqueryParse(ctx *InSubqueryParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitLikeParse(ctx *LikeParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitNullPredicateParse(ctx *NullPredicateParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitDistinctFromParse(ctx *DistinctFromParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitValueExpressionParse(ctx *ValueExpressionParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitAtTimeZoneParse(ctx *AtTimeZoneParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitArithmeticUnaryParse(ctx *ArithmeticUnaryParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitArithmeticBinaryParse(ctx *ArithmeticBinaryParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitConcatenationParse(ctx *ConcatenationParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitPrimaryExpressionParse(ctx *PrimaryExpressionParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitFunctionCallParse(ctx *FunctionCallParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitLambdaParse(ctx *LambdaParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSimpleCaseParse(ctx *SimpleCaseParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSearchedCaseParse(ctx *SearchedCaseParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitCastParse(ctx *CastParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSubscriptParse(ctx *SubscriptParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitDereferenceParse(ctx *DereferenceParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSpecialDateTimeFunctionParse(ctx *SpecialDateTimeFunctionParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitSubstringParse(ctx *SubstringParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitNormalizeParse(ctx *NormalizeParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitExtractParse(ctx *ExtractParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitTimeZoneSpecifierParse(ctx *TimeZoneSpecifierParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitIntervalParse(ctx *IntervalParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitIntervalFieldParse(ctx *IntervalFieldParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitTypeTParse(ctx *TypeTParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitTypeParameterParse(ctx *TypeParameterParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitBaseTypeParse(ctx *BaseTypeParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitWhenParse(ctx *WhenParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitFilterParse(ctx *FilterParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitOverParse(ctx *OverParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitWindowFrameParse(ctx *WindowFrameParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitFrameBoundParse(ctx *FrameBoundParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitExplainOptionParse(ctx *ExplainOptionParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitQualifiedNameParse(ctx *QualifiedNameParse) interface{} {
	return v.VisitChildren(ctx)
}
func (v *BaseSQLQueryTreeVisitor) VisitIDParse(ctx *IDParse) interface{} { return v.VisitChildren(ctx) }
