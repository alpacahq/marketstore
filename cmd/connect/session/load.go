package session

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/alpacahq/marketstore/v4/frontend"

	"github.com/alpacahq/marketstore/v4/cmd/connect/loader"
	"github.com/alpacahq/marketstore/v4/utils/io"
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

	tbk_p, dataFD, loaderFD, err := parseLoadArgs(args)
	if err != nil {
		fmt.Printf("Error while parsing arguments: %v\n", err)
		return
	}
	if dataFD != nil {
		defer dataFD.Close()
	}
	tbk := *tbk_p

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

	/*
		Read the CSV data in chunks until the end of the file
	*/
	for {
		chunkSize := 1000000
		// chunkSize := 100

		npm, endReached, err := loader.CSVtoNumpyMulti(csvReader, tbk, cvm, chunkSize, resp.RecordType == io.VARIABLE)
		if err != nil {
			fmt.Println("Error: ", err.Error())
			return
		}
		if npm != nil { // npm will be empty if we've read the whole file in the last pass
			// LAL ================= DEBUG
			/*
				//fmt.Println("LAL: npm:", npm)
				csmT, err := npm.ToColumnSeriesMap()
				if err != nil {
					fmt.Println("LAL Error: ", err)
					return
				}
				fmt.Println("LAL: csm:", csmT)
				fmt.Println("LAL: cs:", csmT[tbk])
			*/
			// LAL ^^^^^^^^^^^^^^^^^^ DEBUG

			err = writeNumpy(c, npm, resp.RecordType == io.VARIABLE)
			if err != nil {
				fmt.Println("Error: ", err.Error())
				return
			}
			// LAL ================= DEBUG
			/*
				return
			*/
			// LAL ^^^^^^^^^^^^^^^^^^ DEBUG
		}
		if endReached {
			break
		}
	}

	return
}

func writeNumpy(c *Client, npm *io.NumpyMultiDataset, isVariable bool) (err error) {
	req := frontend.WriteRequest{npm, isVariable}
	reqs := &frontend.MultiWriteRequest{
		Requests: []frontend.WriteRequest{req},
	}
	responses := &frontend.MultiServerResponse{}

	if c.mode == local {
		ds := frontend.DataService{}
		err = ds.Write(nil, reqs, responses)
	} else {
		var respI interface{}
		respI, err = c.rc.DoRPC("Write", reqs)
		if respI != nil {
			responses = respI.(*frontend.MultiServerResponse)
		}
	}
	if err != nil {
		return err
	}
	/*
		Process the single response
	*/
	if len(responses.Responses) != 0 {
		return fmt.Errorf("%s", responses.Responses[0].Error)
	}

	return nil
}

func parseLoadArgs(args []string) (mk *io.TimeBucketKey, inputFD, controlFD *os.File, err error) {
	if len(args) < 2 {
		return nil, nil, nil, errors.New(`not enough arguments, see "\help load"`)
	}
	mk = io.NewTimeBucketKey(args[0])
	if mk == nil {
		return nil, nil, nil, errors.New(`key is not in proper format, see "\help load"`)
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
