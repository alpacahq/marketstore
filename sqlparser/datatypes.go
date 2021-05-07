package sqlparser

import "github.com/alpacahq/marketstore/v4/utils/io"

//go:generate ./buildVisitorCode.sh visitorcodegenerated.go
//go:generate stringer -type=StatementTypeEnum,PrimaryExpressionEnum

type Relation interface {
	Materialize() (cs *io.ColumnSeries, err error)
}

/*
Utility Functions
*/
type SortOrderEnum uint8

const (
	_ SortOrderEnum = iota
	ASCENDING
	DESCENDING
)

type NullOrderEnum uint8

const (
	_ NullOrderEnum = iota
	FIRST
	LAST
)

type SetOperatorEnum uint8

const (
	_ SetOperatorEnum = iota
	INTERSECT
	UNION
	EXCEPT
)

type SortItem struct {
	Order     SortOrderEnum
	NullOrder NullOrderEnum
}

type BaseTypeEnum uint8

const (
	_ BaseTypeEnum = iota
	TIME_WITH_TIME_ZONE
	TIMESTAMP_WITH_TIME_ZONE
	DOUBLE_PRECISION
)

type IntervalEnum uint8

const (
	_ IntervalEnum = iota
	YEAR
	MONTH
	DAY
	HOUR
	MINUTE
	SECOND
)

type ComparisonQuantifierEnum uint8

const (
	_ ComparisonQuantifierEnum = iota
	ALL
	SOME
	ANY
)

func StringToComparisonQuantifierEnum(opstr string) ComparisonQuantifierEnum {
	switch opstr {
	case "ALL":
		return ALL
	case "SOME":
		return SOME
	case "ANY":
		return ANY
	default:
		return 0
	}
}

func (cq ComparisonQuantifierEnum) String() string {
	switch cq {
	case ALL:
		return "ALL"
	case SOME:
		return "SOME"
	case ANY:
		return "ANY"
	default:
		return "NONE"
	}
}

type SampleTypeEnum uint8

const (
	_ SampleTypeEnum = iota
	BERNOULLI
	SYSTEM
	POISSONIZED
)

type JoinTypeEnum uint8

const (
	_ JoinTypeEnum = iota
	INNER
	LEFT_OUTER
	RIGHT_OUTER
	FULL_OUTER
)

type StatementTypeEnum uint8

const (
	_ StatementTypeEnum = iota
	QUERY_STMT
	CREATE_TABLE_STMT
	CREATE_TABLE_AS_SELECT_STMT
	DROP_TABLE_STMT
	INSERT_INTO_STMT
	DELETE_STMT
	RENAME_TABLE_STMT
	RENAME_COLUMN_STMT
	ADD_COLUMN_STMT
	CREATE_VIEW_STMT
	DROP_VIEW_STMT
	CALL_STMT
	GRANT_STMT
	REVOKE_STMT
	EXPLAIN_STMT
	SHOW_CREATE_TABLE_STMT
	SHOW_CREATE_VIEW_STMT
	SHOW_TABLES_STMT
	SHOW_SCHEMAS_STMT
	SHOW_CATALOGS_STMT
	SHOW_COLUMNS_STMT
	SHOW_FUNCTIONS_STMT
	SHOW_SESSION_STMT
	SET_SESSION_STMT
	RESET_SESSION_STMT
	START_TRANSACTION_STMT
	COMMIT_STMT
	ROLLBACK_STMT
	SHOW_PARTITIONS_STMT
	PREPARE_STMT
	DEALLOCATE_STMT
	EXECUTE_STMT
	DESCRIBE_INPUT_STMT
	DESCRIBE_OUTPUT_STMT
)

type BinaryOperatorEnum uint8

const (
	_ BinaryOperatorEnum = iota
	AND_OP
	OR_OP
)

type ArithmeticOperatorEnum uint8

const (
	_ ArithmeticOperatorEnum = iota
	MINUS
	PLUS
	MULTIPLY
	DIVIDE
	PERCENT
)

type PrimaryExpressionEnum uint8

const (
	_ PrimaryExpressionEnum = iota
	NULL_LITERAL
	PARAMETER
	STRING_LITERAL
	BINARY_LITERAL
	DECIMAL_LITERAL
	INTEGER_LITERAL
	BOOLEAN_LITERAL
	TYPE_CONSTRUCTOR
	INTERVAL_LITERAL
	POSITION
	ROW_CONSTRUCTOR
	FUNCTION_CALL
	LAMBDA
	SUBQUERY_EXPRESSION
	EXISTS
	SIMPLE_CASE
	SEARCHED_CASE
	CAST
	ARRAY_CONSTRUCTOR
	SUBSCRIPT
	COLUMN_REFERENCE
	DEREFERENCE
	SPECIAL_DATE_TIME_FUNCTION
	SUBSTRING
	NORMALIZE
	EXTRACT
	PARENTHESIZED_EXPRESSION
)

type NormalFormEnum uint8

const (
	_ NormalFormEnum = iota
	NFD
	NFC
	NFKD
	NFKC
)

type FunctionNameEnum uint8

const (
	_ FunctionNameEnum = iota
	CURRENT_DATE
	CURRENT_TIME
	CURRENT_TIMESTAMP
	LOCALTIME
	LOCALTIMESTAMP
)

type SetQuantifierEnum uint8

const (
	_ SetQuantifierEnum = iota
	DISTINCT_SET
	ALL_SET
)
