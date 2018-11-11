// Code generated by "stringer -type=StatementTypeEnum,PrimaryExpressionEnum"; DO NOT EDIT.

package SQLParser

import "strconv"

const _StatementTypeEnum_name = "QUERY_STMTCREATE_TABLE_STMTCREATE_TABLE_AS_SELECT_STMTDROP_TABLE_STMTINSERT_INTO_STMTDELETE_STMTRENAME_TABLE_STMTRENAME_COLUMN_STMTADD_COLUMN_STMTCREATE_VIEW_STMTDROP_VIEW_STMTCALL_STMTGRANT_STMTREVOKE_STMTEXPLAIN_STMTSHOW_CREATE_TABLE_STMTSHOW_CREATE_VIEW_STMTSHOW_TABLES_STMTSHOW_SCHEMAS_STMTSHOW_CATALOGS_STMTSHOW_COLUMNS_STMTSHOW_FUNCTIONS_STMTSHOW_SESSION_STMTSET_SESSION_STMTRESET_SESSION_STMTSTART_TRANSACTION_STMTCOMMIT_STMTROLLBACK_STMTSHOW_PARTITIONS_STMTPREPARE_STMTDEALLOCATE_STMTEXECUTE_STMTDESCRIBE_INPUT_STMTDESCRIBE_OUTPUT_STMT"

var _StatementTypeEnum_index = [...]uint16{0, 10, 27, 54, 69, 85, 96, 113, 131, 146, 162, 176, 185, 195, 206, 218, 240, 261, 277, 294, 312, 329, 348, 365, 381, 399, 421, 432, 445, 465, 477, 492, 504, 523, 543}

func (i StatementTypeEnum) String() string {
	i -= 1
	if i >= StatementTypeEnum(len(_StatementTypeEnum_index)-1) {
		return "StatementTypeEnum(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _StatementTypeEnum_name[_StatementTypeEnum_index[i]:_StatementTypeEnum_index[i+1]]
}

const _PrimaryExpressionEnum_name = "NULL_LITERALPARAMETERSTRING_LITERALBINARY_LITERALDECIMAL_LITERALINTEGER_LITERALBOOLEAN_LITERALTYPE_CONSTRUCTORINTERVAL_LITERALPOSITIONROW_CONSTRUCTORFUNCTION_CALLLAMBDASUBQUERY_EXPRESSIONEXISTSSIMPLE_CASESEARCHED_CASECASTARRAY_CONSTRUCTORSUBSCRIPTCOLUMN_REFERENCEDEREFERENCESPECIAL_DATE_TIME_FUNCTIONSUBSTRINGNORMALIZEEXTRACTPARENTHESIZED_EXPRESSION"

var _PrimaryExpressionEnum_index = [...]uint16{0, 12, 21, 35, 49, 64, 79, 94, 110, 126, 134, 149, 162, 168, 187, 193, 204, 217, 221, 238, 247, 263, 274, 300, 309, 318, 325, 349}

func (i PrimaryExpressionEnum) String() string {
	i -= 1
	if i >= PrimaryExpressionEnum(len(_PrimaryExpressionEnum_index)-1) {
		return "PrimaryExpressionEnum(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _PrimaryExpressionEnum_name[_PrimaryExpressionEnum_index[i]:_PrimaryExpressionEnum_index[i+1]]
}