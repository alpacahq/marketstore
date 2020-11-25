package io

func (cs *ColumnSeries) RestrictViaBitmap(bitmap []bool) (err error) {
	var bitmapValidLength int
	for _, val := range bitmap {
		if !val {
			bitmapValidLength++
		}
	}
	for _, key := range cs.orderedNames {
		iCol := cs.columns[key]
		switch col := iCol.(type) {
		case []int:
			newCol := make([]int, bitmapValidLength)
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
		case []int8:
			newCol := make([]int8, bitmapValidLength)
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
		case []float32:
			newCol := make([]float32, bitmapValidLength)
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
		case []int32:
			newCol := make([]int32, bitmapValidLength)
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
		case []float64:
			newCol := make([]float64, bitmapValidLength)
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
		case []int64:
			newCol := make([]int64, bitmapValidLength)
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
		case []int16:
			newCol := make([]int16, bitmapValidLength)
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
		case []uint8:
			newCol := make([]uint8, bitmapValidLength)
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
		case []uint16:
			newCol := make([]uint16, bitmapValidLength)
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
		case []uint32:
			newCol := make([]uint32, bitmapValidLength)
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
		case []uint64:
			newCol := make([]uint64, bitmapValidLength)
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
		}
	}
	return nil
}
