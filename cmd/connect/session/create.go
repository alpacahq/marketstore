package session

import (
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
)

// create generates new subdirectories and buckets for a database.
func (c *Client) create(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "create"
	parts := strings.Split(args[0], ":")
	if len(parts) != 2 {
		fmt.Println("Key is not in proper format, see \"\\help create\" ")
		return
	}
	tbk := io.NewTimeBucketKey(parts[0], parts[1])
	if tbk == nil {
		fmt.Println("Key is not in proper format, see \"\\help create\" ")
		return
	}

	dsv, err := dataShapesFromInputString(args[1])
	if err != nil {
		return
	}

	rowType := args[2]
	switch rowType {
	case "fixed", "variable":
	default:
		fmt.Printf("Error: Record type \"%s\" is not one of fixed or variable\n", rowType)
		return
	}

	rootDir := executor.ThisInstance.RootDir
	year := int16(time.Now().Year())
	tf, err := tbk.GetTimeFrame()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	rt := io.EnumRecordTypeByName(rowType)
	tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Default", year, dsv, rt)

	err = executor.ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
	if err != nil {
		err = fmt.Errorf("Error: Creation of new catalog entry failed: %s", err.Error())
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Successfully created a new catalog entry: %s\n", tbk.GetItemKey())
}

func dataShapesFromInputString(inputStr string) (dsa []io.DataShape, err error) {
	splitString := strings.Split(inputStr, ":")
	dsa = make([]io.DataShape, 0)
	for _, group := range splitString {
		twoParts := strings.Split(group, "/")
		if len(twoParts) != 2 {
			err = fmt.Errorf("Error: %s: Data shape is not described by a list of column names followed by type.", group)
			fmt.Println(err.Error())
			return nil, err
		}
		elementNames := strings.Split(twoParts[0], ",")
		elementType := twoParts[1]
		eType := io.EnumElementTypeFromName(elementType)
		if eType == io.NONE {
			err = fmt.Errorf("Error: %s: Data type is not a supported type", group)
			fmt.Println(err.Error())
			return nil, err
		}
		for _, name := range elementNames {
			dsa = append(dsa, io.DataShape{Name: name, Type: eType})
		}
	}
	return dsa, nil
}
