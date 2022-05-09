package session

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/alpacahq/marketstore/v4/cmd/connect/loader"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
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
func (c *Client) load(line string) error {
	tbk, dataFD, loaderFD, cleanup, err := parseLine(line)
	if err != nil {
		return fmt.Errorf("failed to parse line: %w", err)
	}
	defer cleanup()
	/*
		Verify the presence of a bucket with the input key
	*/
	resp, err := c.GetBucketInfo(tbk)
	if err != nil {
		log.Error("Error finding existing bucket: %v\n", err)
		return fmt.Errorf("error finding existing bucket: %w", err)
	}
	log.Info("Latest Year: %v\n", resp.LatestYear)

	/*
		Read the metadata about the CSV file
	*/
	csvReader, cvm, err := loader.ReadMetadata(dataFD, loaderFD, resp.DSV)
	if err != nil {
		log.Error("Error: ", err.Error())
		return fmt.Errorf("error: %w", err)
	}

	/*
		Read the CSV data in chunks until the end of the file
	*/
	for {
		chunkSize := 1000000
		// chunkSize := 100

		npm, endReached, err := loader.CSVtoNumpyMulti(csvReader, *tbk, cvm, chunkSize, resp.RecordType == io.VARIABLE)
		if err != nil {
			log.Error("Error: ", err.Error())
			return fmt.Errorf("error: %w", err)
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
				log.Error("Error: ", err.Error())
				return fmt.Errorf("error: %w", err)
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
	return nil
}

func parseLine(line string) (tbk *io.TimeBucketKey, dataFD, loaderFD *os.File, cleanup func(), err error) {
	cleanup = func() {}
	args := strings.Split(line, " ")
	args = args[1:]
	if len(args) == 0 {
		return nil, nil, nil, cleanup, errors.New("not enough arguments to load - try help")
	}

	tbk, dataFD, loaderFD, err = parseLoadArgs(args)
	if err != nil {
		return nil, nil, nil, cleanup,
			fmt.Errorf("error while parsing arguments: %w", err)
	}
	if dataFD != nil {
		cleanup = func() {
			if err2 := dataFD.Close(); err2 != nil {
				log.Error("failed to close a file to load: %v", err2)
			}
		}
	}

	return tbk, dataFD, loaderFD, cleanup, nil
}

func writeNumpy(c *Client, npm *io.NumpyMultiDataset, isVariable bool) (err error) {
	req := frontend.WriteRequest{Data: npm, IsVariableLength: isVariable}
	reqs := &frontend.MultiWriteRequest{
		Requests: []frontend.WriteRequest{req},
	}
	responses := &frontend.MultiServerResponse{}

	err = c.apiClient.Write(reqs, responses)
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
	const argLen = 2
	if len(args) < argLen {
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
		log.Info("Opening %s as ", arg)
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
				log.Info("loader control (yaml) file.\n")
				break
			} else {
				first = true
				inputFD = tryFD
				log.Info("data file.\n")
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
