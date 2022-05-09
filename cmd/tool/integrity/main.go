package integrity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	usage   = "integrity"
	short   = "Evaluate checksums on database internals"
	long    = "This command evaluates checksums on database internals"
	example = "marketstore tool integrity --dir <path> --fix --parallel"

	// Flag descriptions.
	rootDirPathDesc      = "set filesystem path of the directory containing the files to evaulate"
	numChunksPerFileDesc = "set number of checksum chunks per file, excluding the header"
	yearStartDesc        = "limit the evaluation to years later than yearStart (inclusive)"
	yearEndDesc          = "limit the evaluation to years earlier than yearEnd (inclusive)"
	monthStartDesc       = "limit the evaluation to months later than monthStart (inclusive)"
	monthEndDesc         = "limit the evaluation to months earlier than monthEnd (inclusive)"
	parallelDesc         = "run evaluation in parallel, default is false"
	fixHeadersDesc       = "fix known errors in headers if found, default is false"

	// Flag defaults.
	defaultNumChunksPerFile = 12
)

var (
	// Available flags.
	rootDirPath                              string
	numChunksPerFile                         int
	yearStart, yearEnd, monthStart, monthEnd int
	parallel, fixHeaders                     bool

	// Cmd is the integrity command.
	Cmd = &cobra.Command{
		Use:     usage,
		Short:   short,
		Long:    long,
		Aliases: []string{"ic", "integritycheck"},
		Example: example,
		RunE:    executeIntegrity,
	}

	cksums      []int64
	filechunks  []string
	readBuffers [][]byte
	chunkNames  []string
)

func init() {
	// Parse flags.
	Cmd.Flags().StringVarP(&rootDirPath, "dir", "d", "", rootDirPathDesc)
	if err := Cmd.MarkFlagRequired("dir"); err != nil {
		log.Error("failed to mark 'dir' flag required", err.Error())
	}
	Cmd.Flags().IntVar(&numChunksPerFile, "chunks", defaultNumChunksPerFile, numChunksPerFileDesc)
	Cmd.Flags().IntVar(&yearStart, "yearStart", 0, yearStartDesc)
	Cmd.Flags().IntVar(&yearEnd, "yearEnd", 0, yearEndDesc)
	Cmd.Flags().IntVar(&monthStart, "monthStart", 0, monthStartDesc)
	Cmd.Flags().IntVar(&monthEnd, "monthEnd", 0, monthEndDesc)
	Cmd.Flags().BoolVar(&parallel, "parallel", false, parallelDesc)
	Cmd.Flags().BoolVar(&fixHeaders, "fix", false, fixHeadersDesc)

	rootDirPath = filepath.Clean(rootDirPath)
	if !exists(rootDirPath) {
		log.Error(fmt.Sprintf("Root directory: %s does not exist\n", rootDirPath))
		os.Exit(0)
	}
	if !isDir(rootDirPath) {
		log.Error("Root directory: %s is not a directory\n", rootDirPath)
		os.Exit(0)
	}

	if !parallel {
		log.Info("Running single threaded")
	} else {
		log.Info("Running in parallel")
	}

	if yearEnd == 0 {
		yearEnd = 10000
	}
	if monthEnd == 0 {
		monthEnd = 10000
	} else if monthEnd < 1 || monthEnd > 12 {
		log.Error("Ending month must be in the range 1-12")
		os.Exit(1)
	}
	if monthStart != 0 {
		if monthStart < 1 || monthStart > 12 {
			log.Error("Start month must be in the range 1-12")
			os.Exit(1)
		}
	}

	cksums = make([]int64, numChunksPerFile+1)
	filechunks = make([]string, numChunksPerFile+1)
	readBuffers = make([][]byte, numChunksPerFile+1)
	chunkNames = make([]string, numChunksPerFile+1)
	for i := range chunkNames {
		switch i {
		case 0:
			chunkNames[i] = "Hdr"
		case int(time.January):
			chunkNames[i] = "Jan"
		case int(time.February):
			chunkNames[i] = "Feb"
		case int(time.March):
			chunkNames[i] = "Mar"
		case int(time.April):
			chunkNames[i] = "Apr"
		case int(time.May):
			chunkNames[i] = "May"
		case int(time.June):
			chunkNames[i] = "Jun"
		case int(time.July):
			chunkNames[i] = "Jul"
		case int(time.August):
			chunkNames[i] = "Aug"
		case int(time.September):
			chunkNames[i] = "Sep"
		case int(time.October):
			chunkNames[i] = "Oct"
		case int(time.November):
			chunkNames[i] = "Nov"
		case int(time.December):
			chunkNames[i] = "Dec"
		default:
			chunkNames[i] = strconv.Itoa(i + 1)
		}
	}
}

// executeIntegrity implements the integrity tool.
func executeIntegrity(_ *cobra.Command, _ []string) error {
	log.SetLevel(log.INFO)

	log.Info("Root directory: %v", rootDirPath)

	// Perform integrity check.
	return filepath.Walk(rootDirPath, cksumDataFiles)
}

func cksumDataFiles(filePath string, fi os.FileInfo, _ error) (err error) {
	if !isFile(filePath) {
		return fmt.Errorf("%s is not a file", filePath)
	}
	checkFile, _ := filepath.Rel(rootDirPath, filePath)
	if ext := filepath.Ext(checkFile); ext == ".bin" {
		checkFile = checkFile[:len(checkFile)-4]
		year, _ := strconv.Atoi(filepath.Base(checkFile))
		if year < yearStart || year > yearEnd {
			return fmt.Errorf("incorrect start or end dates")
		}

		// Subtract the header size to get our gross chunksize
		size := fi.Size() - io.Headersize
		// Size the chunk buffer to be a multiple of 8-bytes
		chunkSize := io.AlignedSize(int(size/int64(numChunksPerFile) + size%int64(numChunksPerFile)))
		log.Info("Chunksize: ", chunkSize)

		fp, err := os.Open(filePath)
		if err != nil {
			return err
		}

		allocationSize := chunkSize
		if allocationSize < io.Headersize {
			allocationSize = io.Headersize
		}
		// File is open, read it in chunks and calculate checksums
		for i := range cksums {
			cksums[i] = 0
			realloc := false
			if readBuffers[i] == nil {
				realloc = true
			} else if len(readBuffers[i]) < allocationSize {
				realloc = true
			}
			if realloc {
				readBuffers[i] = make([]byte, allocationSize)
			}
		}

		/*
			Read chunks and checksum them in parallel
		*/
		wg := sync.WaitGroup{}
		chunkNum := 0
		for {
			if chunkNum > numChunksPerFile {
				break
			}
			// First chunk is the header
			if chunkNum != 0 {
				if chunkNum > monthEnd {
					break
				}
				if chunkNum < monthStart {
					chunkNum = monthStart
				}
			}

			offset := int64((chunkNum-1)*chunkSize) + io.Headersize
			bufferSize := chunkSize
			if chunkNum == 0 {
				offset = 0
				bufferSize = io.Headersize
			}
			wg.Add(1)
			if parallel {
				go func() {
					err = processChunk(chunkNum, offset, readBuffers[chunkNum][:bufferSize], fp, checkFile, &wg)
					if err != nil {
						log.Error("failed to processChunk async: %w", err)
					}
				}()
			} else {
				err = processChunk(chunkNum, offset, readBuffers[chunkNum][:bufferSize], fp, checkFile, &wg)
				if err != nil {
					return fmt.Errorf("failed to processChunk sync: %w", err)
				}
			}
			chunkNum++
		}
		wg.Wait()
		if err2 := fp.Close(); err2 != nil {
			log.Error("failed to close checksum data file", err2.Error())
		}

		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Printf("%30s", filechunks[0])
		for i, sum := range cksums {
			//			if sum != 0 {
			if sum < 0 {
				sum = -sum
			}
			// nolint:forbidigo // CLI output needs fmt.Println
			fmt.Printf(",%3s %4d", chunkNames[i], sum%10000)
			//			}
		}
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Printf("\n")
	}
	return nil
}

func processChunk(myChunk int, offset int64, buffer []byte, fp *os.File, filename string, wg *sync.WaitGroup) error {
	defer wg.Done()
	//		nread, err := fp.ReadAt(buffer, int64(myChunk*chunkSize))
	nread, err := fp.ReadAt(buffer, offset)
	if err != nil {
		if err.Error() != "EOF" {
			log.Error("Error reading " + fp.Name() + ": " + err.Error())
			return fmt.Errorf("reading %s: %w", fp.Name(), err)
		}
	}
	if nread == 0 {
		log.Error("Short read " + fp.Name())
		return fmt.Errorf("short read %s: %w", fp.Name(), err)
	}
	// Align the checksum range to 8-bytes
	sumRange := io.AlignedSize(nread)
	if sumRange > nread {
		// Zero out padding bytes
		// fmt.Println("sumRange, nread = ", sumRange, nread)
		for i := nread; i < sumRange; i++ {
			buffer[i] = 0
		}
	}
	//				fmt.Println("Sumrange: ", sumRange)
	filechunks[myChunk] = filename
	cksums[myChunk], err = bufferSum(buffer[:sumRange])
	if err != nil {
		return fmt.Errorf("calculate a checksum of the buffer: %w", err)
	}
	/*
		Optionally fix errors in the metadata headers
		This is done only if the chunknum is 0 (header) and if the fixHeader flag has been set
	*/
	if fixHeaders && myChunk == 0 {
		fixKnownHeaderProblems(buffer, fp.Name())
	}
	return nil
}

func fixKnownHeaderProblems(buffer []byte, filePath string) {
	header := (*io.Header)(unsafe.Pointer(&buffer[0]))
	tbinfo := io.NewTimeBucketInfoFromHeader(header, filePath)

	/*
		Check for OHLC with elementTypes = {1,1,1,1}
	*/
	if planner.ElementsEqual(tbinfo.GetElementTypes(), []io.EnumElementType{io.INT32, io.INT32, io.INT32, io.INT32}) {
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Println("found/fixing OHLC type error for ", filePath)
		err := tbinfo.SetElementTypes([]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32})
		if err != nil {
			log.Error("failed to set element types", err.Error())
		}
	}

	/*
		Write the new fileinfo to the file header
	*/
	const allowAll = 0o777
	fp, err := os.OpenFile(filePath, os.O_WRONLY, allowAll)
	if err != nil {
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Println("Unable to write new header to file, terminating...")
		os.Exit(1)
	}
	_ = io.WriteHeader(fp, tbinfo)
}

func bufferSum(buffer []byte) (sum int64, err error) {
	// Swap the byte buffer for an int64 slice for higher performance
	idata, err := io.SwapSliceByte(buffer, int64(0))
	if err != nil {
		return 0, err
	}
	data, ok := idata.([]int64)
	if !ok {
		return 0, errors.New("failed to cast buffer to int64 slice")
	}
	for i := 0; i < len(buffer)/8; i++ {
		sum += data[i]
	}
	return sum, nil
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	if !fi.IsDir() {
		return false
	}
	return true
}

func isFile(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	if !fi.IsDir() {
		return true
	}
	return false
}
