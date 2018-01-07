#!/bin/bash

if [ $# -lt 1 ]; then
  exit 1
fi

OUTFILE=$1

TMPFILE=/tmp/tmp$$

generateHeader() {
cat <<EOF
package io

import (
	"reflect"
	"fmt"
)
EOF
}

TYPESET="int int8 float32 int32 float64 int64 int16 uint8 uint16 uint32 uint64"

generateCoerceColumnType() {
	cat <<EOF
func (cs *ColumnSeries) CoerceColumnType(ds DataShape) (err error) {
	//TODO: Make this generic and maintainable
	if ds.Type == BOOL || ds.Type == STRING {
		return fmt.Errorf("Can not cast to boolean or string")
	}
	i_col := cs.GetByName(ds.Name)

	switch col := i_col.(type) {
EOF
for type in $TYPESET
do
	cat <<EOF
	case []$type:
		switch ds.Type.Kind() {
EOF
	for tInner in $TYPESET
	do
		CamelCase="$(tr '[:lower:]' '[:upper:]' <<< ${tInner:0:1})${tInner:1}"
		cat <<EOF
			case reflect.$CamelCase:
				var newCol []$tInner
				for _, value := range col {
					newCol = append(newCol, $tInner(value))
				}
				cs.columns[ds.Name] = newCol
EOF
	done
echo "		}"
done
echo "	}"
cat <<EOF
	return nil
}

EOF
}

generateRestrictViaBitmap() {
	cat <<EOF
func (cs *ColumnSeries) RestrictViaBitmap(bitmap []bool) (err error) {
	var bitmapValidLength int
	for _, val := range bitmap {
		if !val {
			bitmapValidLength++
		}
	}
	for _, key := range cs.orderedNames {
		i_col := cs.columns[key]
		switch col := i_col.(type) {
EOF

for type in $TYPESET
do
	cat <<EOF
		case []$type:
			newCol := make([]$type, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
EOF
done
cat <<EOF
		}
	}
	return nil
}
EOF
}

rm -f $OUTFILE
generateHeader >> $OUTFILE
generateCoerceColumnType >> $OUTFILE
generateRestrictViaBitmap >> $OUTFILE

gofmt -w $OUTFILE
