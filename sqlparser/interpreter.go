package sqlparser

import (
	"fmt"

	. "github.com/antlr/antlr4/runtime/Go/antlr"

	. "github.com/alpacahq/marketstore/v4/sqlparser/parser"
)

// BuildQueryTree returns the query tree built from the parse tree
func BuildQueryTree(sourceString string) (tree IMSTree, err error) {
	input := NewInputStream(sourceString)
	lexer := NewSQLBaseLexer(input)
	lexErr := new(DescriptiveErrorListener)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(lexErr)

	tokens := NewCommonTokenStream(lexer, TokenDefaultChannel)
	if lexErr.err != nil {
		fmt.Println(lexErr.err.Error())
		return nil, lexErr.err
	}

	parser := NewSQLBaseParser(tokens)
	parser.BuildParseTrees = true
	parseErr := new(DescriptiveErrorListener)
	parser.AddErrorListener(parseErr)

	// This is how we can print the ANTLR grammar parsed syntax
	//	fmt.Println(ast.statementSource)
	//	fmt.Println(parser.Statements().ToStringTree([]string{"\n"}, parser))

	tree = NewStatementsParse(parser.Statements(), sourceString)
	if tree == nil {
		return nil, fmt.Errorf("unable to create query tree from parse tree")
	}
	if parseErr.err != nil {
		fmt.Println(parseErr.err.Error())
		return nil, parseErr.err
	}
	return tree, nil
}

/*
func print_tree(input Tree, lev int) {
	for i := 0; i < lev; i++ {
		fmt.Printf("  ")
	}
	fmt.Println(input)
	for _, node := range input.GetChildren() {
		print_tree(node, lev+1)
	}
}
*/

/*
Utility Functions
*/
type DescriptiveErrorListener struct {
	DefaultErrorListener
	err error
}

func (de *DescriptiveErrorListener) SyntaxError(recognizer Recognizer, offendingSymbol interface{},
	line, column int, msg string, e RecognitionException) {
	de.err = fmt.Errorf("Syntax Error[%d:%d]: %s", line, column, msg)
}
