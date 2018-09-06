package session

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/alpacahq/marketstore/cmd/connect/loader"
	"github.com/alpacahq/marketstore/utils/io"
)

/*
	load executes data loading into the DB from csv files.

	Note that the format of the CSV file used for loading can optionally include nanosecond precision extensions
	to the timestamp used for the Epoch column. For example, a normal timestamp format would be like this:
			Epoch
			20161230 21:37:57
	With the extension that includes nanosecond precision, we add the number of nanoseconds as a six digit fixed field:
			Epoch
			20161230 21:37:57 140000
*/
func (c *Client) load(line string) {
	args := strings.Split(line, " ")
	args = args[1:]
	if len(args) == 0 {
		fmt.Println("Not enough arguments to load - try help")
		return
	}

	tbk, dataFD, loaderFD, err := parseLoadArgs(args)
	if err != nil {
		fmt.Printf("Error while parsing arguments: %v\n", err)
		return
	}
	if dataFD != nil {
		defer dataFD.Close()
	}

	/*
		Verify the presence of a bucket with the input key
	*/
	resp, err := c.GetBucketInfo(tbk)
	if err != nil {
		fmt.Printf("Error finding existing bucket: %v\n", err)
		return
	}
	fmt.Printf("Latest Year: %v\n", resp.LatestYear)

	/*
		Read the metadata about the CSV file
	*/
	csvReader, cvm, err := loader.ReadMetadata(dataFD, loaderFD, resp.DSV)
	if err != nil {
		fmt.Println("Error: ", err.Error())
		return
	}

	//	fmt.Println("Composed metadata:")
	//	fmt.Println(cvm)

	for {
		npm, endReached, err := loader.CSVtoNumpyMulti(csvReader, cvm, 1000000)
		if err != nil {
			fmt.Println("Error: ", err.Error())
			return
		}
		fmt.Println("Chunk line length: ", npm.Len())
		if endReached {
			break
		}
	}

	return
}

func parseLoadArgs(args []string) (mk *io.TimeBucketKey, inputFD, controlFD *os.File, err error) {
	if len(args) < 2 {
		return nil, nil, nil, errors.New("Not enough arguments, see \"\\help load\"")
	}
	mk = io.NewTimeBucketKey(args[0])
	if mk == nil {
		return nil, nil, nil, errors.New("Key is not in proper format, see \"\\help load\"")
	}
	/*
		We need to read two file names that open successfully
	*/
	var first, second bool
	var tryFD *os.File
	for _, arg := range args[1:] {
		fmt.Printf("Opening %s as ", arg)
		tryFD, err = os.Open(arg)
		if err != nil {
			return nil, nil, nil, err
		}
		fs, err := tryFD.Stat()
		if err != nil {
			return nil, nil, nil, err
		}
		if fs.Size() != 0 {
			if first {
				second = true
				controlFD = tryFD
				fmt.Printf("loader control (yaml) file.\n")
				break
			} else {
				first = true
				inputFD = tryFD
				fmt.Printf("data file.\n")
			}
			continue
		} else {
			return nil, nil, nil, err
		}
	}

	if second {
		return mk, inputFD, controlFD, nil
	} else if first {
		return mk, inputFD, nil, nil
	}
	return nil, nil, nil, nil
}
