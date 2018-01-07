#!/bin/bash

if [ $# -lt 1 ]; then
  exit 1
fi

OUTFILE=$1

TMPFILE=/tmp/tmp$$

generateHeader() {
printf "package SQLParser\n\n"
}

generateAcceptMethods() {
	line1[0]="func (this *"
	line1[1]=") Accept(visitor IMSTreeVisitor) interface{} {"
	line2[0]="	switch t := visitor.(type) {"
	line3[0]="	case ISQLQueryTreeVisitor:"
	line4[0]="		return t.Visit"
	line4[1]="(this)"
	line5[0]="	default:"
	line6[0]="		return t.VisitChildren(this)"
	line7[0]="	}"
	line8[0]="}"

	while read class
	do
  		printf "%s%s%s\n" "${line1[0]}" $class "${line1[1]}"
  		printf "%s\n" "${line2[0]}"
  		printf "%s\n" "${line3[0]}"
  		printf "%s%s%s\n" "${line4[0]}" $class "${line4[1]}"
  		printf "%s\n" "${line5[0]}"
  		printf "%s\n" "${line6[0]}"
  		printf "%s\n" "${line7[0]}"
  		printf "%s\n" "${line8[0]}"
	done < $TMPFILE
}

generateVisitInterface() {
	printf "type ISQLQueryTreeVisitor interface {\n"
	printf "	IMSTreeVisitor\n"
	while read class
	do
  		printf "	Visit%s(ctx *%s) interface{}\n" $class $class
	done < $TMPFILE
	printf "}\n\n"
}

generateVisitBaseImplementation() {
	printf "type BaseSQLQueryTreeVisitor struct {\n"
	printf "	*BaseMSTreeVisitor\n"
	printf "}\n\n"
	printf "var _ ISQLQueryTreeVisitor = &BaseSQLQueryTreeVisitor{}\n\n"
	printf "func (this *BaseSQLQueryTreeVisitor) Visit(tree IMSTree) interface{} {\n"
	printf "	return tree.Accept(this)\n"
	printf "}\n\n"
	printf "func (this *BaseSQLQueryTreeVisitor) VisitChildren(tree IMSTree) interface{} {\n"
	printf "	for _, child := range tree.GetChildren() {\n"
	printf "		retval := child.Accept(this)\n"
	printf "		if retval != nil {\n"
	printf "			return retval\n"
	printf "		}\n"
	printf "	}\n"
	printf "	return nil\n"
	printf "}\n\n"

	while read class
	do
  		printf "func (this *BaseSQLQueryTreeVisitor) Visit%s(ctx *%s) interface{} { return this.VisitChildren(ctx) }\n" $class $class
	done < $TMPFILE
}

grep type sqlparsetree.go | grep struct | awk '{print $2}' | grep -v MSTree > $TMPFILE

generateHeader > $OUTFILE
generateAcceptMethods >> $OUTFILE
generateVisitInterface >> $OUTFILE
generateVisitBaseImplementation >> $OUTFILE

gofmt -w $OUTFILE
