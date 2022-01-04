package sqlparser

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"

	"github.com/alpacahq/marketstore/v4/sqlparser/parser"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type StatementsParse struct {
	MSTree
	QueryText string
}

func NewStatementsParse(node antlr.Tree, queryText string) (term *StatementsParse) {
	ctx := node.(*parser.StatementsContext)
	term = new(StatementsParse)
	term.QueryText = queryText
	term.AddChild(NewStatementParse(ctx.Statement(), queryText))
	return term
}

func (sp *StatementsParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type StatementParse struct {
	/*
		This one is long because it is a catch all for DDL type
		statements

		Many of these fields are unused depending on the value
		of statementType
	*/
	MSTree
	statementType                                                       StatementTypeEnum
	query, columnAliases, from, to, tableName, column                   IMSTree
	qualifiedNames, tableElements, booleanExpressions                   []IMSTree
	callArguments, privileges, explainOptions, identifiers, expressions []IMSTree
	sortItems, transactionModes                                         []IMSTree
	statement, tableProperties                                          IMSTree
	IsFrom, IsGrantOption, IsAll, IsExists                              bool
	QueryText                                                           string
}

func NewStatementParse(node antlr.Tree, queryText string) (term *StatementParse) {
	term = new(StatementParse)
	term.QueryText = queryText
	switch ctx := node.(type) {
	case *parser.StatementDefaultContext:
		term.statementType = QUERY_STMT
		term.query = NewQueryParse(ctx.Query())
	case *parser.InsertIntoContext:
		term.statementType = INSERT_INTO_STMT
		term.tableName = NewQualifiedNameParse(ctx.QualifiedName())
		term.query = NewQueryParse(ctx.Query())
		if ctx.ColumnAliases() != nil {
			term.columnAliases = NewColumnAliasesParse(ctx.ColumnAliases())
		}
	case *parser.ExplainContext:
		term.statementType = EXPLAIN_STMT
		term.statement = NewStatementParse(ctx.Statement(), queryText)
		for _, cctx := range ctx.AllExplainOption() {
			term.explainOptions = append(term.explainOptions,
				NewExplainOptionParse(cctx))
		}
	default:
		fmt.Println(reflect.TypeOf(ctx))
	}
	return term
}

func (sp *StatementParse) String(level int) (out []string) {
	out = append(out, Explain(sp.query, level+1)...)
	out = append(out, Explain(sp.columnAliases, level+1)...)
	out = append(out, Explain(sp.from, level+1)...)
	out = append(out, Explain(sp.to, level+1)...)
	out = append(out, Explain(sp.tableName, level+1)...)
	out = append(out, Explain(sp.column, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.qualifiedNames, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.tableElements, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.booleanExpressions, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.callArguments, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.privileges, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.explainOptions, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.identifiers, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.expressions, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.sortItems, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.transactionModes, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type QueryParse struct {
	MSTree
	queryNoWith IMSTree
}

func NewQueryParse(node antlr.Tree) (term *QueryParse) {
	ctx := node.(*parser.QueryContext)
	term = new(QueryParse)
	term.queryNoWith = NewQueryNoWithParse(ctx.QueryNoWith())
	return term
}

func (sp *QueryParse) String(level int) (out []string) {
	out = append(out, Explain(sp.queryNoWith, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type QueryNoWithParse struct {
	MSTree
	queryTerm IMSTree
	sortItems []IMSTree
	limit     int
}

func NewQueryNoWithParse(node antlr.Tree) (term *QueryNoWithParse) {
	ctx := node.(*parser.QueryNoWithContext)
	term = new(QueryNoWithParse)
	term.queryTerm = NewQueryTermParse(ctx.QueryTerm())
	for _, cctx := range ctx.AllSortItem() {
		term.sortItems = append(term.sortItems,
			NewSortItemParse(cctx))
	}
	if ctx.LIMIT() != nil {
		term.limit, _ = strconv.Atoi(ctx.INTEGER_VALUE().GetText())
	}
	return term
}

func (sp *QueryNoWithParse) String(level int) (out []string) {
	out = append(out, Explain(sp.queryTerm, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.sortItems, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type QueryTermParse struct {
	MSTree
	left, right, queryPrimary IMSTree
	operator                  SetOperatorEnum
	quantifier                SetQuantifierEnum
}

func NewQueryTermParse(node antlr.Tree) (term *QueryTermParse) {
	term = new(QueryTermParse)
	switch ctx := node.(type) {
	case *parser.QueryTermDefaultContext:
		term.queryPrimary = NewQueryPrimaryParse(ctx.QueryPrimary())
	case *parser.SetOperationContext:
		term.left = NewQueryTermParse(ctx.GetLeft())
		term.right = NewQueryTermParse(ctx.GetRight())
		switch {
		case ctx.INTERSECT() != nil:
			term.operator = INTERSECT
		case ctx.UNION() != nil:
			term.operator = UNION
		case ctx.EXCEPT() != nil:
			term.operator = EXCEPT
		}
		if ctx.SetQuantifier() != nil {
			cctx := ctx.SetQuantifier().(*parser.SetQuantifierContext)
			switch {
			case cctx.DISTINCT() != nil:
				term.quantifier = DISTINCT_SET
			case cctx.ALL() != nil:
				term.quantifier = ALL_SET
			}
		}
	}

	return term
}

func (sp *QueryTermParse) String(level int) (out []string) {
	out = append(out, Explain(sp.left, level+1)...)
	out = append(out, Explain(sp.right, level+1)...)
	out = append(out, Explain(sp.queryPrimary, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type QueryPrimaryParse struct {
	MSTree
	querySpec, tableName, subquery IMSTree
	expressions                    []IMSTree
}

func NewQueryPrimaryParse(node antlr.Tree) (term *QueryPrimaryParse) {
	term = new(QueryPrimaryParse)
	switch ctx := node.(type) {
	case *parser.QueryPrimaryDefaultContext:
		term.querySpec = NewQuerySpecificationParse(ctx.QuerySpecification())
	case *parser.TableContext:
		term.tableName = NewQualifiedNameParse(ctx.QualifiedName())
	case *parser.InlineTableContext:
		for _, cctx := range ctx.AllExpression() {
			term.expressions = append(term.expressions,
				NewExpressionParse(cctx))
		}
	case *parser.SubqueryContext:
		term.subquery = NewQueryNoWithParse(ctx.QueryNoWith())
	}

	return term
}

func (sp *QueryPrimaryParse) String(level int) (out []string) {
	out = append(out, Explain(sp.querySpec, level+1)...)
	out = append(out, Explain(sp.tableName, level+1)...)
	out = append(out, Explain(sp.subquery, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.expressions, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SortItemParse struct {
	MSTree
	sortOrdering SortOrderEnum
	nullOrdering NullOrderEnum
	expression   IMSTree
}

func NewSortItemParse(node antlr.Tree) (term *SortItemParse) {
	ctx := node.(*parser.SortItemContext)
	term = new(SortItemParse)
	term.expression = NewExpressionParse(ctx.Expression())
	if ctx.GetOrdering() != nil {
		switch {
		case ctx.ASC() != nil:
			term.sortOrdering = ASCENDING
		case ctx.DESC() != nil:
			term.sortOrdering = DESCENDING
		}
	}
	if ctx.GetNullOrdering() != nil {
		switch {
		case ctx.FIRST() != nil:
			term.nullOrdering = FIRST
		case ctx.LAST() != nil:
			term.nullOrdering = LAST
		}
	}
	return term
}

func (sp *SortItemParse) String(level int) (out []string) {
	out = append(out, Explain(sp.expression, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type QuerySpecificationParse struct {
	MSTree
	setQuantifier          SetQuantifierEnum
	where, groupBy, having IMSTree
	selectItems, relations []IMSTree
}

func NewQuerySpecificationParse(node antlr.Tree) (term *QuerySpecificationParse) {
	ctx := node.(*parser.QuerySpecificationContext)
	term = new(QuerySpecificationParse)
	if ctx.SetQuantifier() != nil {
		cctx := ctx.SetQuantifier().(*parser.SetQuantifierContext)
		switch {
		case cctx.DISTINCT() != nil:
			term.setQuantifier = DISTINCT_SET
		case cctx.ALL() != nil:
			term.setQuantifier = ALL_SET
		}
	}
	for _, cctx := range ctx.AllSelectItem() {
		term.selectItems = append(term.selectItems,
			NewSelectItemParse(cctx))
	}
	for _, cctx := range ctx.AllRelation() {
		term.relations = append(term.relations,
			NewRelationParse(cctx))
	}
	if ctx.WHERE() != nil {
		term.where = NewBooleanExpressionParse(ctx.GetWhere())
	}
	if ctx.GROUP() != nil {
		term.groupBy = NewGroupByParse(ctx.GroupBy())
	}
	if ctx.HAVING() != nil {
		term.having = NewBooleanExpressionParse(ctx.GetHaving())
	}
	return term
}

func (sp *QuerySpecificationParse) String(level int) (out []string) {
	out = append(out, Explain(sp.having, level+1)...)
	out = append(out, Explain(sp.groupBy, level+1)...)
	out = append(out, Explain(sp.where, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.selectItems, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.relations, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type GroupByParse struct {
	MSTree
	setQuantifier    SetQuantifierEnum
	groupingElements []IMSTree
}

func NewGroupByParse(node antlr.Tree) (term *GroupByParse) {
	ctx := node.(*parser.GroupByContext)
	term = new(GroupByParse)
	if ctx.SetQuantifier() != nil {
		cctx := ctx.SetQuantifier().(*parser.SetQuantifierContext)
		switch {
		case cctx.DISTINCT() != nil:
			term.setQuantifier = DISTINCT_SET
		case cctx.ALL() != nil:
			term.setQuantifier = ALL_SET
		}
	}
	for _, cctx := range ctx.AllGroupingElement() {
		term.groupingElements = append(term.groupingElements,
			NewGroupingElementParse(cctx))
	}
	return term
}

func (sp *GroupByParse) String(level int) (out []string) {
	out = append(out, ExplainAllItemsInList(sp.groupingElements, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type GroupingElementParse struct {
	MSTree
	groupingExp                          IMSTree
	rollupNames, cubeNames, groupingSets []IMSTree
}

func NewGroupingElementParse(node antlr.Tree) (term *GroupingElementParse) {
	ictx := node.GetChild(0)
	term = new(GroupingElementParse)
	switch ctx := ictx.(type) {
	case *parser.SingleGroupingSetContext:
		term.groupingExp = NewGroupingExpressionsParse(ctx.GroupingExpressions())
	case *parser.RollupContext:
		for _, cctx := range ctx.AllQualifiedName() {
			term.rollupNames = append(term.rollupNames,
				NewQualifiedNameParse(cctx))
		}
	case *parser.CubeContext:
		for _, cctx := range ctx.AllQualifiedName() {
			term.cubeNames = append(term.cubeNames,
				NewQualifiedNameParse(cctx))
		}
	case *parser.MultipleGroupingSetsContext:
		for _, cctx := range ctx.AllGroupingSet() {
			term.groupingSets = append(term.groupingSets,
				NewGroupingSetParse(cctx))
		}
	}
	return term
}

func (sp *GroupingElementParse) String(level int) (out []string) {
	out = append(out, Explain(sp.groupingExp, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.rollupNames, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.cubeNames, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.groupingSets, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type GroupingExpressionsParse struct {
	MSTree
	expressions []IMSTree
}

func NewGroupingExpressionsParse(node antlr.Tree) (term *GroupingExpressionsParse) {
	ctx := node.(*parser.GroupingExpressionsContext)
	term = new(GroupingExpressionsParse)
	for _, cctx := range ctx.AllExpression() {
		term.expressions = append(term.expressions,
			NewExpressionParse(cctx))
	}
	return term
}

func (sp *GroupingExpressionsParse) String(level int) (out []string) {
	out = append(out, ExplainAllItemsInList(sp.expressions, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type GroupingSetParse struct {
	MSTree
	qualifiedNames []IMSTree
}

func NewGroupingSetParse(node antlr.Tree) (term *GroupingSetParse) {
	ctx := node.(*parser.GroupingSetContext)
	term = new(GroupingSetParse)
	for _, cctx := range ctx.AllQualifiedName() {
		term.qualifiedNames = append(term.qualifiedNames,
			NewQualifiedNameParse(cctx))
	}
	return term
}

func (sp *GroupingSetParse) String(level int) (out []string) {
	out = append(out, ExplainAllItemsInList(sp.qualifiedNames, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SelectItemParse struct {
	MSTree
	expression, alias, qualifiedName IMSTree
	IsSelectAll                      bool
}

func NewSelectItemParse(node antlr.Tree) (term *SelectItemParse) {
	term = new(SelectItemParse)
	switch ctx := node.(type) {
	case *parser.SelectSingleContext:
		term.expression = NewExpressionParse(ctx.Expression())
		//		term.AddChild(NewExpressionParse(ctx.Expression()))
		if ctx.Identifier() != nil {
			term.alias = NewIDParse(ctx.Identifier())
		}
	case *parser.SelectAllContext:
		if ctx.QualifiedName() != nil {
			term.qualifiedName = NewQualifiedNameParse(ctx.QualifiedName())
		}
		term.IsSelectAll = true
	}
	return term
}

func (sp *SelectItemParse) String(level int) (out []string) {
	out = append(out, Explain(sp.expression, level+1)...)
	out = append(out, Explain(sp.alias, level+1)...)
	out = append(out, Explain(sp.qualifiedName, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type RelationParse struct {
	MSTree
	left, right, criteria, sampled IMSTree
	joinType                       JoinTypeEnum
}

func NewRelationParse(node antlr.Tree) (term *RelationParse) {
	term = new(RelationParse)
	switch ctx := node.(type) {
	case *parser.JoinRelationContext:
		term.left = NewRelationParse(ctx.GetLeft())
		term.right = NewRelationParse(ctx.GetRight())
		if ctx.JoinCriteria() != nil {
			term.criteria = NewJoinCriteriaParse(ctx.JoinCriteria())
		}
		if ctx.JoinType() != nil {
			cctx := ctx.JoinType().(*parser.JoinTypeContext)
			switch {
			case cctx.INNER() != nil:
				term.joinType = INNER
			case cctx.LEFT() != nil:
				term.joinType = LEFT_OUTER
			case cctx.RIGHT() != nil:
				term.joinType = RIGHT_OUTER
			case cctx.FULL() != nil:
				term.joinType = FULL_OUTER
			}
		}
	case *parser.RelationDefaultContext:
		term.sampled = NewSampledRelationParse(ctx.SampledRelation())
	}
	return term
}

func (sp *RelationParse) String(level int) (out []string) {
	out = append(out, Explain(sp.left, level+1)...)
	out = append(out, Explain(sp.right, level+1)...)
	out = append(out, Explain(sp.criteria, level+1)...)
	out = append(out, Explain(sp.sampled, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type JoinCriteriaParse struct {
	MSTree
	onExpression IMSTree
	identifiers  []IMSTree
}

func NewJoinCriteriaParse(node antlr.Tree) (term *JoinCriteriaParse) {
	ctx := node.(*parser.JoinCriteriaContext)
	term = new(JoinCriteriaParse)
	term.onExpression = NewBooleanExpressionParse(ctx.BooleanExpression())
	for _, cctx := range ctx.AllIdentifier() {
		term.identifiers = append(term.identifiers, NewIDParse(cctx))
	}
	return term
}

func (sp *JoinCriteriaParse) String(level int) (out []string) {
	out = append(out, Explain(sp.onExpression, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.identifiers, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SampledRelationParse struct {
	MSTree
	sampleType                  SampleTypeEnum
	aliasedRelation, percentage IMSTree
}

func NewSampledRelationParse(node antlr.Tree) (term *SampledRelationParse) {
	ctx := node.(*parser.SampledRelationContext)
	term = new(SampledRelationParse)
	term.aliasedRelation = NewAliasedRelationParse(ctx.AliasedRelation())
	if ctx.GetPercentage() != nil {
		term.percentage = NewExpressionParse(ctx.GetPercentage())
	}
	switch {
	case ctx.BERNOULLI() != nil:
		term.sampleType = BERNOULLI
	case ctx.SYSTEM() != nil:
		term.sampleType = SYSTEM
	case ctx.POISSONIZED() != nil:
		term.sampleType = POISSONIZED
	}
	return term
}

func (sp *SampledRelationParse) String(level int) (out []string) {
	out = append(out, Explain(sp.aliasedRelation, level+1)...)
	out = append(out, Explain(sp.percentage, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type AliasedRelationParse struct {
	MSTree
	hasID, hasAliases                    bool
	relationPrimary, identifier, aliases IMSTree
}

func NewAliasedRelationParse(node antlr.Tree) (term *AliasedRelationParse) {
	ctx := node.(*parser.AliasedRelationContext)
	term = new(AliasedRelationParse)
	term.relationPrimary = NewRelationPrimaryParse(ctx.RelationPrimary())
	if ctx.Identifier() != nil {
		term.hasID = true
		term.identifier = NewIDParse(ctx.Identifier())
		if ctx.ColumnAliases() != nil {
			term.hasAliases = true
			term.aliases = NewColumnAliasesParse(ctx.ColumnAliases())
		}
	}
	return term
}

func (sp *AliasedRelationParse) String(level int) (out []string) {
	out = append(out, Explain(sp.relationPrimary, level+1)...)
	out = append(out, Explain(sp.identifier, level+1)...)
	out = append(out, Explain(sp.aliases, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type ColumnAliasesParse struct{ MSTree }

func NewColumnAliasesParse(node antlr.Tree) (term *ColumnAliasesParse) {
	ctx := node.(*parser.ColumnAliasesContext)
	term = new(ColumnAliasesParse)
	for _, cctx := range ctx.AllIdentifier() {
		term.AddChild(NewIDParse(cctx))
	}
	return term
}

func (sp *ColumnAliasesParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
================================================
RelationPrimary block
================================================.
*/
type RelationPrimaryParse struct {
	MSTree
	IsTableName, IsSubquery, IsUnnest, IsRelation, WithOrdinality bool
}

func NewRelationPrimaryParse(node antlr.Tree) (term *RelationPrimaryParse) {
	term = new(RelationPrimaryParse)
	switch ctx := node.(type) {
	case *parser.TableNameContext:
		term.IsTableName = true
		term.AddChild(NewQualifiedNameParse(ctx.QualifiedName()))
	case *parser.SubqueryRelationContext:
		term.IsSubquery = true
		term.AddChild(NewQueryParse(ctx.Query()))
	case *parser.UnnestContext:
		term.IsUnnest = true
		for _, cctx := range ctx.AllExpression() {
			term.AddChild(NewExpressionParse(cctx))
		}
		if ctx.ORDINALITY() != nil {
			term.WithOrdinality = true
		}
	case *parser.ParenthesizedRelationContext:
		term.IsRelation = true
		term.AddChild(NewRelationParse(ctx.Relation()))
	}
	return term
}

func (sp *RelationPrimaryParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
================================================
*/

/*
================================================
Expression block
================================================.
*/
type ExpressionParse struct {
	MSTree
	IsBoolean bool
}

func NewExpressionParse(node antlr.Tree) (term *ExpressionParse) {
	ctx := node.(*parser.ExpressionContext)
	term = new(ExpressionParse)
	switch {
	case ctx.BooleanExpression() != nil:
		term.AddChild(NewBooleanExpressionParse(
			ctx.BooleanExpression()))
		term.IsBoolean = true
	case ctx.ValueExpression() != nil:
		term.AddChild(NewValueExpressionParse(
			ctx.ValueExpression()))
	}
	return term
}

func (sp *ExpressionParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
================================================
*/

/*
================================================
BooleanExpression block
================================================.
*/
type BooleanExpressionParse struct {
	MSTree
	predicate               IMSTree // Predicate node type
	left, right             IMSTree // Always Value Expression types
	operator                BinaryOperatorEnum
	IsLiteral, IsNot, value bool
}

func NewBooleanExpressionParse(node antlr.Tree) (term *BooleanExpressionParse) {
	term = new(BooleanExpressionParse)
	/*
		We handle the four cases in one node
		"left" stores valueExpression or booleanExpression
		for the default and logicalNot cases
	*/
	var done bool
	for {
		switch ctx := node.(type) {
		case *parser.LogicalNotContext:
			term.IsNot = true
			node = node.GetChild(0) // Iterate over child node
		case *parser.LogicalBinaryContext:
			term.right = NewExpressionParse(ctx.GetRight())
			switch ctx.GetOperator().GetText() {
			case "AND":
				term.operator = AND_OP
			case "OR":
				// TODO: Properly handle OR with quantitative predicates
				term.operator = OR_OP
			}
			term.left = NewBooleanExpressionParse(ctx.GetLeft())
			done = true
		case *parser.BooleanDefaultContext:
			term.left = NewValueExpressionParse(ctx.ValueExpression())
			term.predicate = NewPredicateParse(ctx.Predicate())
			done = true
		case *parser.BoolLiteralTooContext:
			term.IsLiteral = true
			cctx := ctx.Booleanliteral().(*parser.BooleanliteralContext)
			if cctx.TRUE() != nil {
				term.value = true
			}
			done = true
		}
		if done {
			break
		}
	}
	return term
}

func (sp *BooleanExpressionParse) String(level int) (out []string) {
	out = append(out, Explain(sp.predicate, level+1)...)
	out = append(out, Explain(sp.left, level+1)...)
	out = append(out, Explain(sp.right, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
================================================
*/

/*
================================================
Predicate block
================================================.
*/
type PredicateParse struct{ MSTree }

func NewPredicateParse(node antlr.Tree) (term *PredicateParse) {
	term = new(PredicateParse)
	switch ctx := node.(type) {
	case *parser.ComparisonContext:
		term.AddChild(NewComparisonParse(ctx))
	case *parser.QuantifiedComparisonContext:
		term.AddChild(NewQuantifiedComparisonParse(ctx))
	case *parser.BetweenContext:
		term.AddChild(NewBetweenParse(ctx))
	case *parser.InListContext:
		term.AddChild(NewInListParse(ctx))
	case *parser.InSubqueryContext:
		term.AddChild(NewInSubqueryParse(ctx))
	case *parser.LikeContext:
		term.AddChild(NewLikeParse(ctx))
	case *parser.NullPredicateContext:
		term.AddChild(NewNullPredicateParse(ctx))
	case *parser.DistinctFromContext:
		term.AddChild(NewDistinctFromParse(ctx))
	}

	return term
}

func (sp *PredicateParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type ComparisonParse struct {
	MSTree
	comparisonOperator io.ComparisonOperatorEnum
	right              IMSTree
}

func NewComparisonParse(node antlr.Tree) (term *ComparisonParse) {
	ctx := node.(*parser.ComparisonContext)
	term = new(ComparisonParse)
	term.comparisonOperator = io.StringToComparisonOperatorEnum(
		ctx.ComparisonOperator().GetText())
	term.right = NewValueExpressionParse(ctx.ValueExpression())
	return term
}

func (sp *ComparisonParse) String(level int) (out []string) {
	out = append(out, Explain(sp.right, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type QuantifiedComparisonParse struct {
	MSTree
	comparisonOperator   io.ComparisonOperatorEnum
	comparisonQuantifier ComparisonQuantifierEnum
	query                IMSTree
}

func NewQuantifiedComparisonParse(node antlr.Tree) (term *QuantifiedComparisonParse) {
	ctx := node.(*parser.QuantifiedComparisonContext)
	term = new(QuantifiedComparisonParse)
	term.comparisonOperator = io.StringToComparisonOperatorEnum(
		ctx.ComparisonOperator().GetText())
	term.comparisonQuantifier = StringToComparisonQuantifierEnum(
		ctx.ComparisonQuantifier().GetText())
	term.query = NewQueryParse(ctx.Query())
	return term
}

func (sp *QuantifiedComparisonParse) String(level int) (out []string) {
	out = append(out, Explain(sp.query, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type BetweenParse struct {
	MSTree
	IsNot        bool
	lower, upper IMSTree
}

func NewBetweenParse(node antlr.Tree) (term *BetweenParse) {
	ctx := node.(*parser.BetweenContext)
	term = new(BetweenParse)
	term.lower = NewValueExpressionParse(ctx.GetLower())
	term.upper = NewValueExpressionParse(ctx.GetUpper())
	if ctx.NOT() != nil {
		term.IsNot = true
	}
	return term
}

func (sp *BetweenParse) String(level int) (out []string) {
	out = append(out, Explain(sp.lower, level+1)...)
	out = append(out, Explain(sp.upper, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type InListParse struct {
	MSTree
	IsNot  bool
	inlist []IMSTree
}

func NewInListParse(node antlr.Tree) (term *InListParse) {
	ctx := node.(*parser.InListContext)
	term = new(InListParse)
	for _, cctx := range ctx.AllValueExpression() {
		term.inlist = append(term.inlist, NewValueExpressionParse(cctx))
	}
	if ctx.NOT() != nil {
		term.IsNot = true
	}
	return term
}

func (sp *InListParse) String(level int) (out []string) {
	out = append(out, ExplainAllItemsInList(sp.inlist, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type InSubqueryParse struct {
	MSTree
	IsNot bool
	query IMSTree
}

func NewInSubqueryParse(node antlr.Tree) (term *InSubqueryParse) {
	ctx := node.(*parser.InSubqueryContext)
	term = new(InSubqueryParse)
	term.query = NewQueryParse(ctx.Query())
	if ctx.NOT() != nil {
		term.IsNot = true
	}
	return term
}

func (sp *InSubqueryParse) String(level int) (out []string) {
	out = append(out, Explain(sp.query, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type LikeParse struct {
	MSTree
	IsNot           bool
	pattern, escape IMSTree
}

func NewLikeParse(node antlr.Tree) (term *LikeParse) {
	ctx := node.(*parser.LikeContext)
	term = new(LikeParse)
	term.pattern = NewValueExpressionParse(ctx.GetPattern())
	if ctx.GetEscape() != nil {
		term.escape = NewValueExpressionParse(ctx.GetEscape())
	}
	return term
}

func (sp *LikeParse) String(level int) (out []string) {
	out = append(out, Explain(sp.pattern, level+1)...)
	out = append(out, Explain(sp.escape, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type NullPredicateParse struct {
	MSTree
	IsNot bool
}

func NewNullPredicateParse(node antlr.Tree) (term *NullPredicateParse) {
	ctx := node.(*parser.NullPredicateContext)
	term = new(NullPredicateParse)
	if ctx.NOT() != nil {
		term.IsNot = true
	}
	return term
}

func (sp *NullPredicateParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type DistinctFromParse struct {
	MSTree
	IsNot bool
	right IMSTree
}

func NewDistinctFromParse(node antlr.Tree) (term *DistinctFromParse) {
	ctx := node.(*parser.DistinctFromContext)
	term = new(DistinctFromParse)
	term.right = NewValueExpressionParse(ctx.GetRight())
	if ctx.NOT() != nil {
		term.IsNot = true
	}
	return term
}

func (sp *DistinctFromParse) String(level int) (out []string) {
	out = append(out, Explain(sp.right, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
================================================
*/
/*
================================================
ValueExpression block
================================================
*/

type ValueExpressionParse struct {
	MSTree
}

func NewValueExpressionParse(node antlr.Tree) (term *ValueExpressionParse) {
	term = new(ValueExpressionParse)
	switch ctx := node.(type) {
	case *parser.ValueExpressionDefaultContext:
		term.AddChild(NewPrimaryExpressionParse(ctx.PrimaryExpression()))
	case *parser.AtTimeZoneContext:
		term.AddChild(NewAtTimeZoneParse(ctx))
	case *parser.ArithmeticUnaryContext:
		term.AddChild(NewArithmeticUnaryParse(ctx))
	case *parser.ArithmeticBinaryContext:
		term.AddChild(NewArithmeticBinaryParse(ctx))
	case *parser.ConcatenationContext:
		term.AddChild(NewConcatParse(ctx))
	}
	return term
}

func (sp *ValueExpressionParse) String(level int) (out []string) {
	//	out = append(out, Explain(sp.primaryExpression, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type AtTimeZoneParse struct {
	MSTree
	value, timezone IMSTree
}

func NewAtTimeZoneParse(node antlr.Tree) (term *AtTimeZoneParse) {
	ctx := node.(*parser.AtTimeZoneContext)
	term = new(AtTimeZoneParse)
	term.value = NewValueExpressionParse(ctx.ValueExpression())
	term.timezone = NewTimeZoneSpecifierParse(ctx.TimeZoneSpecifier())
	return term
}

func (sp *AtTimeZoneParse) String(level int) (out []string) {
	out = append(out, Explain(sp.value, level+1)...)
	out = append(out, Explain(sp.timezone, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type ArithmeticUnaryParse struct {
	MSTree
	value    IMSTree
	operator ArithmeticOperatorEnum
}

func NewArithmeticUnaryParse(node antlr.Tree) (term *ArithmeticUnaryParse) {
	ctx := node.(*parser.ArithmeticUnaryContext)
	term = new(ArithmeticUnaryParse)
	if ctx.MINUS() != nil {
		term.operator = MINUS
	} else {
		term.operator = PLUS
	}
	term.value = NewValueExpressionParse(ctx.ValueExpression())
	return term
}

func (sp *ArithmeticUnaryParse) String(level int) (out []string) {
	out = append(out, Explain(sp.value, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type ArithmeticBinaryParse struct {
	MSTree
	left, right IMSTree
	operator    ArithmeticOperatorEnum
}

func NewArithmeticBinaryParse(node antlr.Tree) (term *ArithmeticBinaryParse) {
	ctx := node.(*parser.ArithmeticBinaryContext)
	term = new(ArithmeticBinaryParse)
	switch {
	case ctx.ASTERISK() != nil:
		term.operator = MULTIPLY
	case ctx.SLASH() != nil:
		term.operator = DIVIDE
	case ctx.PERCENT() != nil:
		term.operator = PERCENT
	case ctx.PLUS() != nil:
		term.operator = PLUS
	case ctx.MINUS() != nil:
		term.operator = MINUS
	}
	term.left = NewValueExpressionParse(ctx.GetLeft())
	term.right = NewValueExpressionParse(ctx.GetRight())
	return term
}

func (sp *ArithmeticBinaryParse) String(level int) (out []string) {
	out = append(out, Explain(sp.left, level+1)...)
	out = append(out, Explain(sp.right, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type ConcatenationParse struct {
	MSTree
	left, right IMSTree
}

func NewConcatParse(node antlr.Tree) (term *ConcatenationParse) {
	ctx := node.(*parser.ConcatenationContext)
	term = new(ConcatenationParse)
	term.left = NewValueExpressionParse(ctx.GetLeft())
	term.right = NewValueExpressionParse(ctx.GetRight())
	return term
}

func (sp *ConcatenationParse) String(level int) (out []string) {
	out = append(out, Explain(sp.left, level+1)...)
	out = append(out, Explain(sp.right, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
================================================
*/
/*
================================================
PrimaryExpression block
================================================.
*/
type PrimaryExpressionParse struct {
	MSTree
	primaryType PrimaryExpressionEnum
}

func NewPrimaryExpressionParse(node antlr.Tree) (term *PrimaryExpressionParse) {
	term = new(PrimaryExpressionParse)
	/*
		Types up to BOOLEAN_LITERAL we can store directly in this node
		Other types have child nodes, which we handle like:
		  1-N child nodes required all of same type: store using node.AddChild
		  Multiple types of child node required: attach a single specialized parse node

	*/
	switch ctx := node.(type) {
	case *parser.NullLiteralContext:
		term.primaryType = NULL_LITERAL
	case *parser.ParameterContext:
		term.primaryType = PARAMETER
	case *parser.StringLiteralContext:
		term.primaryType = STRING_LITERAL
		term.payload = ctx.STRING().GetText()
	case *parser.BinaryLiteralContext:
		term.primaryType = BINARY_LITERAL
		term.payload = ctx.BINARY_LITERAL().GetText()
	case *parser.IntegerLiteralContext:
		term.primaryType = INTEGER_LITERAL
		intTerm, err := strconv.Atoi(ctx.INTEGER_VALUE().GetText())
		if err != nil {
			return nil
		}
		term.payload = int64(intTerm)
	case *parser.DecimalLiteralContext:
		term.primaryType = DECIMAL_LITERAL
		var err error
		term.payload, err = strconv.ParseFloat(ctx.DECIMAL_VALUE().GetText(), 64)
		if err != nil {
			return nil
		}
	case *parser.BoolLiteralContext:
		term.primaryType = BOOLEAN_LITERAL
		cctx := ctx.Booleanliteral().(*parser.BooleanliteralContext)
		term.payload = (cctx.TRUE() != nil)
	case *parser.TypeConstructorContext:
		term.primaryType = TYPE_CONSTRUCTOR
		if ctx.DOUBLE_PRECISION() != nil {
			term.payload = ctx.STRING().GetText()
		} else {
			term.payload = NewIDParse(ctx)
		}
	case *parser.IntervalLiteralContext:
		term.primaryType = INTERVAL_LITERAL
		term.AddChild(NewIntervalParse(ctx))
	case *parser.PositionContext:
		term.primaryType = POSITION
		for _, cctx := range ctx.AllValueExpression() {
			term.AddChild(NewValueExpressionParse(cctx))
		}
	case *parser.RowConstructorContext:
		term.primaryType = ROW_CONSTRUCTOR
		for _, cctx := range ctx.AllExpression() {
			term.AddChild(NewExpressionParse(cctx))
		}
	case *parser.FunctionCallContext:
		term.primaryType = FUNCTION_CALL
		term.AddChild(NewFunctionCallParse(ctx))
	case *parser.LambdaContext:
		term.primaryType = LAMBDA
		term.AddChild(NewLambdaParse(ctx))
	case *parser.SubqueryExpressionContext:
		term.primaryType = SUBQUERY_EXPRESSION
		term.AddChild(NewQueryParse(ctx.Query()))
	case *parser.ExistsContext:
		term.primaryType = EXISTS
		term.AddChild(NewQueryParse(ctx.Query()))
	case *parser.SimpleCaseContext:
		term.primaryType = SIMPLE_CASE
		term.AddChild(NewSimpleCaseParse(ctx))
	case *parser.SearchedCaseContext:
		term.primaryType = SEARCHED_CASE
		term.AddChild(NewSearchedCaseParse(ctx))
	case *parser.CastContext:
		term.primaryType = CAST
		term.AddChild(NewCastParse(ctx))
	case *parser.ArrayConstructorContext:
		term.primaryType = ARRAY_CONSTRUCTOR
		for _, cctx := range ctx.AllExpression() {
			term.AddChild(NewExpressionParse(cctx))
		}
	case *parser.SubscriptContext:
		term.primaryType = SUBSCRIPT
		term.AddChild(NewSubscriptParse(ctx))
	case *parser.ColumnReferenceContext:
		term.primaryType = COLUMN_REFERENCE
		term.AddChild(NewIDParse(ctx.Identifier()))
	case *parser.DereferenceContext:
		term.primaryType = DEREFERENCE
		term.AddChild(NewDereferenceParse(ctx))
	case *parser.SpecialDateTimeFunctionContext:
		term.primaryType = SPECIAL_DATE_TIME_FUNCTION
		term.AddChild(NewSpecialDateTimeFunctionParse(ctx))
	case *parser.SubstringContext:
		term.primaryType = SUBSTRING
		term.AddChild(NewSubstringParse(ctx))
	case *parser.NormalizeContext:
		term.primaryType = NORMALIZE
		term.AddChild(NewNormalizeParse(ctx))
	case *parser.ExtractContext:
		term.primaryType = EXTRACT
		term.AddChild(NewExtractParse(ctx))
	case *parser.ParenthesizedExpressionContext:
		term.primaryType = PARENTHESIZED_EXPRESSION
		term.AddChild(NewExpressionParse(ctx.Expression()))
	}
	return term
}

func (sp *PrimaryExpressionParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type FunctionCallParse struct {
	MSTree
	hasAsterisk, hasFilter, hasSetQuantifier bool
	qualifiedName, filter, over              IMSTree
	expressionList                           []IMSTree
	setQuantifier                            SetQuantifierEnum
}

func NewFunctionCallParse(node antlr.Tree) (term *FunctionCallParse) {
	ctx := node.(*parser.FunctionCallContext)
	term = new(FunctionCallParse)
	term.qualifiedName = NewQualifiedNameParse(ctx.QualifiedName())
	if ctx.Filter() != nil {
		term.hasFilter = true
		term.filter = NewFilterParse(ctx.Filter())
	}
	if ctx.ASTERISK() != nil {
		term.hasAsterisk = true
	}
	if ctx.SetQuantifier() != nil {
		term.hasSetQuantifier = true
		cctx := ctx.SetQuantifier().(*parser.SetQuantifierContext)
		switch {
		case cctx.DISTINCT() != nil:
			term.setQuantifier = DISTINCT_SET
		case cctx.ALL() != nil:
			term.setQuantifier = ALL_SET
		}
	}
	for _, expr := range ctx.AllExpression() {
		term.expressionList = append(term.expressionList,
			NewExpressionParse(expr))
	}
	if ctx.Over() != nil {
		term.over = NewOverParse(ctx.Over())
	}
	return term
}

func (sp *FunctionCallParse) String(level int) (out []string) {
	out = append(out, Explain(sp.qualifiedName, level+1)...)
	out = append(out, Explain(sp.filter, level+1)...)
	out = append(out, Explain(sp.over, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.expressionList, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type LambdaParse struct {
	MSTree
	expression  IMSTree
	identifiers []IMSTree
}

func NewLambdaParse(node antlr.Tree) (term *LambdaParse) {
	ctx := node.(*parser.LambdaContext)
	term = new(LambdaParse)
	term.expression = NewExpressionParse(ctx.Expression())
	for _, ident := range ctx.AllIdentifier() {
		term.identifiers = append(term.identifiers, NewIDParse(ident))
	}
	return term
}

func (sp *LambdaParse) String(level int) (out []string) {
	out = append(out, Explain(sp.expression, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.identifiers, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SimpleCaseParse struct {
	MSTree
	valueExpression, elseExpression IMSTree
	whenClause                      []IMSTree
}

func NewSimpleCaseParse(node antlr.Tree) (term *SimpleCaseParse) {
	ctx := node.(*parser.SimpleCaseContext)
	term = new(SimpleCaseParse)
	term.valueExpression = NewValueExpressionParse(ctx.ValueExpression())
	if ctx.GetElseExpression() != nil {
		term.elseExpression = NewExpressionParse(ctx.GetElseExpression())
	}
	for _, cctx := range ctx.AllWhenClause() {
		term.whenClause = append(term.whenClause,
			NewWhenParse(cctx))
	}
	return term
}

func (sp *SimpleCaseParse) String(level int) (out []string) {
	out = append(out, Explain(sp.valueExpression, level+1)...)
	out = append(out, Explain(sp.elseExpression, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.whenClause, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SearchedCaseParse struct {
	MSTree
	elseExpression IMSTree
	whenClause     []IMSTree
}

func NewSearchedCaseParse(node antlr.Tree) (term *SearchedCaseParse) {
	ctx := node.(*parser.SearchedCaseContext)
	term = new(SearchedCaseParse)
	if ctx.GetElseExpression() != nil {
		term.elseExpression = NewExpressionParse(ctx.GetElseExpression())
	}
	for _, cctx := range ctx.AllWhenClause() {
		term.whenClause = append(term.whenClause,
			NewWhenParse(cctx))
	}
	return term
}

func (sp *SearchedCaseParse) String(level int) (out []string) {
	out = append(out, Explain(sp.elseExpression, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.whenClause, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type CastParse struct {
	MSTree
	expression, type_t IMSTree
}

func NewCastParse(node antlr.Tree) (term *CastParse) {
	ctx := node.(*parser.CastContext)
	term = new(CastParse)
	term.expression = NewExpressionParse(ctx.Expression())
	term.type_t = NewTypeTParse(ctx.Type_t())
	return term
}

func (sp *CastParse) String(level int) (out []string) {
	out = append(out, Explain(sp.expression, level+1)...)
	out = append(out, Explain(sp.type_t, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SubscriptParse struct {
	MSTree
	value, index IMSTree
}

func NewSubscriptParse(node antlr.Tree) (term *SubscriptParse) {
	ctx := node.(*parser.SubscriptContext)
	term = new(SubscriptParse)
	term.value = NewPrimaryExpressionParse(ctx.GetValue())
	term.index = NewValueExpressionParse(ctx.GetIndex())
	return term
}

func (sp *SubscriptParse) String(level int) (out []string) {
	out = append(out, Explain(sp.value, level+1)...)
	out = append(out, Explain(sp.index, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type DereferenceParse struct {
	MSTree
	base, fieldName IMSTree
}

func NewDereferenceParse(node antlr.Tree) (term *DereferenceParse) {
	ctx := node.(*parser.DereferenceContext)
	term = new(DereferenceParse)
	term.base = NewPrimaryExpressionParse(ctx.GetBase())
	term.fieldName = NewIDParse(ctx.GetFieldName())
	return term
}

func (sp *DereferenceParse) String(level int) (out []string) {
	out = append(out, Explain(sp.base, level+1)...)
	out = append(out, Explain(sp.fieldName, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SpecialDateTimeFunctionParse struct {
	MSTree
	precision    int
	functionName FunctionNameEnum
}

func NewSpecialDateTimeFunctionParse(node antlr.Tree) (term *SpecialDateTimeFunctionParse) {
	ctx := node.(*parser.SpecialDateTimeFunctionContext)
	term = new(SpecialDateTimeFunctionParse)
	if ctx.INTEGER_VALUE() != nil {
		param, _ := strconv.Atoi(ctx.INTEGER_VALUE().GetText())
		term.precision = param
	}
	switch {
	case ctx.CURRENT_DATE() != nil:
		term.functionName = CURRENT_DATE
	case ctx.CURRENT_TIME() != nil:
		term.functionName = CURRENT_TIME
	case ctx.CURRENT_TIMESTAMP() != nil:
		term.functionName = CURRENT_TIMESTAMP
	case ctx.LOCALTIME() != nil:
		term.functionName = LOCALTIME
	case ctx.LOCALTIMESTAMP() != nil:
		term.functionName = LOCALTIMESTAMP
	}
	return term
}

func (sp *SpecialDateTimeFunctionParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type SubstringParse struct {
	MSTree
	subTerm, baseTerm, forTerm IMSTree
}

func NewSubstringParse(node antlr.Tree) (term *SubstringParse) {
	ctx := node.(*parser.SubstringContext)
	term = new(SubstringParse)
	term.subTerm = NewValueExpressionParse(ctx.GetSubterm())
	term.baseTerm = NewValueExpressionParse(ctx.GetBaseterm())
	if ctx.GetForterm() != nil {
		term.forTerm = NewValueExpressionParse(ctx.GetForterm())
	}
	return term
}

func (sp *SubstringParse) String(level int) (out []string) {
	out = append(out, Explain(sp.subTerm, level+1)...)
	out = append(out, Explain(sp.baseTerm, level+1)...)
	out = append(out, Explain(sp.forTerm, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type NormalizeParse struct {
	MSTree
	normalForm NormalFormEnum
	value      IMSTree
}

func NewNormalizeParse(node antlr.Tree) (term *NormalizeParse) {
	ctx := node.(*parser.NormalizeContext)
	term = new(NormalizeParse)
	if ctx.GetNormalform() != nil {
		switch ctx.GetNormalform().GetText() {
		case "NFD":
			term.normalForm = NFD
		case "NFC":
			term.normalForm = NFC
		case "NFKD":
			term.normalForm = NFKD
		case "NFKC":
			term.normalForm = NFKC
		}
	}
	term.value = NewValueExpressionParse(ctx.ValueExpression())
	return term
}

func (sp *NormalizeParse) String(level int) (out []string) {
	out = append(out, Explain(sp.value, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type ExtractParse struct {
	MSTree
	left, right IMSTree
}

func NewExtractParse(node antlr.Tree) (term *ExtractParse) {
	ctx := node.(*parser.ExtractContext)
	term = new(ExtractParse)
	term.left = NewIDParse(ctx.Identifier())
	term.right = NewIDParse(ctx.ValueExpression())
	return term
}

func (sp *ExtractParse) String(level int) (out []string) {
	out = append(out, Explain(sp.left, level+1)...)
	out = append(out, Explain(sp.right, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
================================================.
*/
type TimeZoneSpecifierParse struct {
	MSTree
	intervalZone IMSTree
	stringZone   string
}

func NewTimeZoneSpecifierParse(node antlr.Tree) (term *TimeZoneSpecifierParse) {
	ictx := node.GetChild(0)
	term = new(TimeZoneSpecifierParse)
	switch ctx := ictx.(type) {
	case *parser.TimeZoneIntervalContext:
		term.intervalZone = NewIntervalParse(ctx.Interval())
	case *parser.TimeZoneStringContext:
		term.stringZone = ctx.STRING().GetText()
	}
	return term
}

func (sp *TimeZoneSpecifierParse) String(level int) (out []string) {
	out = append(out, Explain(sp.intervalZone, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type IntervalParse struct {
	MSTree
	IsPlus, IsMinus    bool
	stringValue        string
	fromField, toField IMSTree
}

func NewIntervalParse(node antlr.Tree) (term *IntervalParse) {
	ctx := node.(*parser.IntervalContext)
	term = new(IntervalParse)
	switch ctx.GetSign().GetText() {
	case "+":
		term.IsPlus = true
	case "-":
		term.IsMinus = true
	}
	term.stringValue = ctx.STRING().GetText()
	term.fromField = NewIntervalFieldParse(ctx.GetFrom())
	if ctx.GetTo() != nil {
		term.toField = NewIntervalFieldParse(ctx.GetTo())
	}
	return term
}

func (sp *IntervalParse) String(level int) (out []string) {
	out = append(out, Explain(sp.fromField, level+1)...)
	out = append(out, Explain(sp.toField, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type IntervalFieldParse struct {
	MSTree
	value IntervalEnum
}

func NewIntervalFieldParse(node antlr.Tree) (term *IntervalFieldParse) {
	ctx := node.(*parser.IntervalFieldContext)
	term = new(IntervalFieldParse)
	switch {
	case ctx.YEAR() != nil:
		term.value = YEAR
	case ctx.MONTH() != nil:
		term.value = MONTH
	case ctx.DAY() != nil:
		term.value = DAY
	case ctx.HOUR() != nil:
		term.value = HOUR
	case ctx.MINUTE() != nil:
		term.value = MINUTE
	case ctx.SECOND() != nil:
		term.value = SECOND
	}
	return term
}

func (sp *IntervalFieldParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type TypeTParse struct {
	MSTree
	array, baseType                       IMSTree
	mapElem, rowIDElem, rowElem, typeElem []IMSTree
}

func NewTypeTParse(node antlr.Tree) (term *TypeTParse) {
	ctx := node.(*parser.Type_tContext)
	term = new(TypeTParse)
	switch {
	case ctx.ARRAY() != nil:
		term.array = NewTypeTParse(ctx.Get_type_t())
	case ctx.MAP() != nil:
		for _, mapelem := range ctx.GetMapelem() {
			term.mapElem = append(term.mapElem, NewTypeTParse(mapelem))
		}
	case ctx.ROW() != nil:
		for _, rowIDElem := range ctx.GetRowidelem() {
			term.rowIDElem = append(term.rowIDElem, NewIDParse(rowIDElem))
		}
		for _, rowElem := range ctx.GetRowelem() {
			term.rowElem = append(term.rowElem, NewIDParse(rowElem))
		}
	case ctx.BaseType() != nil:
		term.baseType = NewBaseTypeParse(ctx.BaseType())
		for _, typeElem := range ctx.GetTypeelem() {
			term.typeElem = append(term.typeElem,
				NewTypeParameterParse(typeElem))
		}
	}
	return term
}

func (sp *TypeTParse) String(level int) (out []string) {
	out = append(out, Explain(sp.array, level+1)...)
	out = append(out, Explain(sp.baseType, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.mapElem, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.rowIDElem, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.rowElem, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.typeElem, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type TypeParameterParse struct{ MSTree }

func NewTypeParameterParse(node antlr.Tree) (term *TypeParameterParse) {
	ctx := node.(*parser.TypeParameterContext)
	term = new(TypeParameterParse)
	switch {
	case ctx.INTEGER_VALUE() != nil:
		param, _ := strconv.Atoi(ctx.INTEGER_VALUE().GetText())
		term.payload = int64(param)
	case ctx.Type_t() != nil:
		term.AddChild(NewTypeTParse(ctx.Type_t()))
	}
	return term
}

func (sp *TypeParameterParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type BaseTypeParse struct {
	MSTree
	type_id BaseTypeEnum
}

func NewBaseTypeParse(node antlr.Tree) (term *BaseTypeParse) {
	ctx := node.(*parser.BaseTypeContext)
	term = new(BaseTypeParse)
	switch {
	case ctx.TIME_WITH_TIME_ZONE() != nil:
		term.type_id = TIME_WITH_TIME_ZONE
	case ctx.TIMESTAMP_WITH_TIME_ZONE() != nil:
		term.type_id = TIMESTAMP_WITH_TIME_ZONE
	case ctx.DOUBLE_PRECISION() != nil:
		term.type_id = DOUBLE_PRECISION
	case ctx.Identifier() != nil:
		term.AddChild(NewIDParse(ctx.Identifier()))
	}
	return term
}

func (sp *BaseTypeParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type WhenParse struct {
	MSTree
	condition, result IMSTree
}

func NewWhenParse(node antlr.Tree) (term *WhenParse) {
	ctx := node.(*parser.WhenClauseContext)
	term = new(WhenParse)
	term.condition = NewExpressionParse(ctx.GetCondition())
	term.result = NewExpressionParse(ctx.GetResult())
	return term
}

func (sp *WhenParse) String(level int) (out []string) {
	out = append(out, Explain(sp.condition, level+1)...)
	out = append(out, Explain(sp.result, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type FilterParse struct {
	MSTree
}

func NewFilterParse(node antlr.Tree) (term *FilterParse) {
	ctx := node.(*parser.FilterContext)
	term = new(FilterParse)
	term.AddChild(NewBooleanExpressionParse(ctx.BooleanExpression()))
	return term
}

func (sp *FilterParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type OverParse struct {
	MSTree
	partitions, sortItems []IMSTree
}

func NewOverParse(node antlr.Tree) (term *OverParse) {
	ctx := node.(*parser.OverContext)
	term = new(OverParse)
	for _, expr := range ctx.GetPartition() {
		term.partitions = append(term.partitions, NewExpressionParse(expr))
	}
	for _, expr := range ctx.GetOrderitem() {
		term.sortItems = append(term.sortItems, NewSortItemParse(expr))
	}
	if ctx.WindowFrame() != nil {
		term.AddChild(NewWindowFrameParse(ctx.WindowFrame()))
	}
	return term
}

func (sp *OverParse) String(level int) (out []string) {
	out = append(out, ExplainAllItemsInList(sp.partitions, level+1)...)
	out = append(out, ExplainAllItemsInList(sp.sortItems, level+1)...)
	return append(out, PrependLevel(GetStructString(sp), level))
}

type WindowFrameParse struct {
	MSTree
	IsRange, IsBetween bool
}

func NewWindowFrameParse(node antlr.Tree) (term *WindowFrameParse) {
	ctx := node.(*parser.WindowFrameContext)
	term = new(WindowFrameParse)
	if strings.EqualFold(ctx.GetFrameType().GetText(), "RANGE") {
		term.IsRange = true // If true, then ROWS is false and v.v.
	}
	term.AddChild(NewFrameBoundParse(ctx.GetStartFrame()))
	if ctx.BETWEEN() != nil {
		term.IsBetween = true
		term.AddChild(NewFrameBoundParse(ctx.GetEndFrame()))
	}
	return term
}

func (sp *WindowFrameParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type FrameBoundParse struct {
	MSTree
	IsCurrentRow, IsUnbounded, IsPreceding, IsFollowing bool
}

func NewFrameBoundParse(node antlr.Tree) (term *FrameBoundParse) {
	term = new(FrameBoundParse)
	ictx := node.GetChild(0)
	switch childCtx := ictx.(type) {
	case *parser.CurrentRowBoundContext:
		term.IsCurrentRow = true
	case *parser.UnboundedFrameContext:
		term.IsUnbounded = true
		switch {
		case strings.EqualFold(childCtx.GetBoundType().GetText(), "PRECEDING"):
			term.IsPreceding = true
		case strings.EqualFold(childCtx.GetBoundType().GetText(), "FOLLOWING"):
			term.IsPreceding = true
		}
	case *parser.BoundedFrameContext:
		switch {
		case strings.EqualFold(childCtx.GetBoundType().GetText(), "PRECEDING"):
			term.IsPreceding = true
		case strings.EqualFold(childCtx.GetBoundType().GetText(), "FOLLOWING"):
			term.IsPreceding = true
		}
		term.AddChild(NewExpressionParse(childCtx))
	}
	return term
}

func (sp *FrameBoundParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type ExplainOptionParse struct {
	MSTree
	IsFormat bool
}

func NewExplainOptionParse(node antlr.Tree) (term *ExplainOptionParse) {
	term = new(ExplainOptionParse)
	ictx := node.GetChild(0)
	switch childCtx := ictx.(type) {
	case *parser.ExplainFormatContext:
		term.IsFormat = true
		term.payload = childCtx.GetValue().GetText()
	case *parser.ExplainTypeContext:
		term.payload = childCtx.GetValue().GetText()
	}
	return term
}

func (sp *ExplainOptionParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type QualifiedNameParse struct{ MSTree }

func NewQualifiedNameParse(node antlr.Tree) (term *QualifiedNameParse) {
	term = new(QualifiedNameParse)
	for _, childNode := range node.GetChildren() {
		term.AddChild(NewIDParse(childNode))
	}
	return term
}

func (sp *QualifiedNameParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

type IDParse struct {
	MSTree
	name string
}

func NewIDParse(node antlr.Tree) (term *IDParse) {
	term = new(IDParse)
	switch ctx := node.(type) {
	case *parser.UnquotedIdentifierContext:
		term.name = ctx.IDENTIFIER().GetText()
	case *parser.DigitIdentifierContext:
		term.name = ctx.DIGIT_IDENTIFIER().GetText()
	case *parser.BackQuotedIdentifierContext:
		term.name = ctx.BACKQUOTED_IDENTIFIER().GetText()
		term.name = term.name[1 : len(term.name)-1]
	}
	return term
}

func (sp *IDParse) String(level int) (out []string) {
	return append(out, PrependLevel(GetStructString(sp), level))
}

/*
Utility Functions.
*/
type IMSTree interface {
	GetParent() IMSTree
	SetParent(IMSTree)
	GetPayload() interface{}
	GetChild(i int) IMSTree
	GetChildCount() int
	GetChildren() []IMSTree
	AddChild(IMSTree)
	String(int) []string
	Accept(IMSTreeVisitor) interface{}
}

type IMSTreeVisitor interface {
	Visit(IMSTree) interface{}
	VisitChildren(IMSTree) interface{}
}

type BaseMSTreeVisitor struct{}

var _ IMSTreeVisitor = &BaseMSTreeVisitor{}

func (mst *BaseMSTreeVisitor) Visit(tree IMSTree) interface{} {
	return tree.Accept(mst)
}

func (mst *BaseMSTreeVisitor) VisitChildren(tree IMSTree) interface{} {
	for _, child := range tree.GetChildren() {
		retval := child.Accept(mst)
		if retval != nil {
			return retval
		}
	}
	return nil
}

type MSTree struct {
	IMSTree
	BaseMSTreeVisitor
	parent   IMSTree
	children []IMSTree
	payload  interface{}
}

func (pt *MSTree) GetParent() IMSTree {
	return pt.parent
}

func (pt *MSTree) SetParent(arg IMSTree) {
	pt.parent = arg
}

func (pt *MSTree) GetPayload() interface{} {
	return pt.payload
}

func (pt *MSTree) GetChild(i int) IMSTree {
	if len(pt.children) < i+1 {
		return nil
	}
	return pt.children[i]
}

func (pt *MSTree) GetChildCount() int {
	return len(pt.children)
}

func (pt *MSTree) GetChildren() []IMSTree {
	return pt.children
}

func (pt *MSTree) AddChild(arg IMSTree) {
	pt.children = append(pt.children, arg)
}

func (pt *MSTree) String(level int) (out []string) {
	if pt != nil {
		return []string{PrependLevel(GetStructString(pt), level)}
	} else {
		return nil
	}
}

func Explain(ctx IMSTree, o_level ...int) (out []string) {
	if ctx == nil {
		return nil
	}
	var level int
	if len(o_level) != 0 {
		level = o_level[0]
	}

	for _, child := range ctx.GetChildren() {
		result := Explain(child, level+1)
		out = append(out, result...)
	}
	out = append(out, ctx.String(level)...)

	return out
}

func PrependLevel(msg string, level int) string {
	var buffer bytes.Buffer
	for i := 0; i < level; i++ {
		buffer.WriteString("  ")
	}
	return buffer.String() + msg
}

func GetStructString(sp interface{}) string {
	removeMSTree := func(input string) (output string) {
		begin := strings.Index(input, "MSTree:{")
		if begin == -1 {
			return input
		}
		input = input[begin+8:] // Skip past MSTree:{
		for {                   // Skip pairs of {} inside
			check := strings.Index(input, "{")
			if check != -1 { // Find the closing "}"
				input = input[check+1:]
				check = strings.Index(input, "}")
				if check != -1 {
					input = input[check+1:]
				} else {
					return input // Failed
				}
			} else {
				break
			}
		}
		begin = strings.Index(input, "}") + 1

		if input[begin] == ' ' {
			input = input[1:]
		}

		return "{" + input[begin:]
	}
	if sp != nil {
		var buffer bytes.Buffer
		typeName := reflect.TypeOf(sp).Elem().String()
		typeName = typeName[10 : len(typeName)-5]
		buffer.WriteString(typeName)
		buffer.WriteString(":")
		unfiltered := fmt.Sprintf("%+v", sp)
		structContents := removeMSTree(unfiltered[1:])
		buffer.WriteString(" " + structContents)
		return buffer.String()
	} else {
		return ""
	}
}

func ExplainAllItemsInList(items []IMSTree, level int) (out []string) {
	for _, item := range items {
		out = append(out, Explain(item, level+1)...)
	}
	return out
}

func PrintExplain(stmt string, input []string) {
	printFiller := func(num int) {
		for i := 0; i < num; i++ {
			fmt.Printf("=")
		}
		fmt.Printf("\n")
	}
	fmt.Printf("\n")
	printFiller(len(stmt))
	fmt.Printf("%s\n", stmt)
	printFiller(len(stmt))
	for i := len(input) - 1; i >= 0; i-- {
		fmt.Println(input[i])
	}
}
