package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"sync"
	"syscall"
	"unsafe"

	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"
)

var rootDir string
var numChunksPerFile int
var cksums []int64
var filechunks []string
var readBuffers [][]byte
var chunkNames []string
var parallel, fixHeaders bool
var yearStart, yearEnd, monthStart, monthEnd int

func init() {
	// set logging to output to standard error
	flag.Lookup("logtostderr").Value.Set("true")
	SetLogLevel(INFO)

	_rootDir := flag.String("rootDir", "", "Root directory to be checked")
	flag.IntVar(&numChunksPerFile, "numChunksPerFile", 12, "Number of checksum chunks per file, excluding the header")
	flag.IntVar(&yearStart, "yearStart", 0, "Limit the checker to years later than and including yearStart")
	flag.IntVar(&yearEnd, "yearEnd", 0, "Limit the checker to years earlier than and including yearEnd")
	flag.IntVar(&monthStart, "monthStart", 0, "Limit the checker to months later than and including monthStart")
	flag.IntVar(&monthEnd, "monthEnd", 0, "Limit the checker to months earlier than and including monthEnd")
	flag.BoolVar(&parallel, "parallel", false, "Run checker in parallel, default is false")
	flag.BoolVar(&fixHeaders, "fixHeaders", false, "fix known errors in headers if found, default is false")
	flag.Parse()
	rootDir = filepath.Clean(*_rootDir)
	if rootDir == "" {
		fmt.Println("Must enter a root directory (-rootDir)")
		os.Exit(0)
	}
	if !exists(rootDir) {
		fmt.Printf("Root directory: %s does not exist\n", rootDir)
		os.Exit(0)
	}
	if !isDir(rootDir) {
		fmt.Printf("Root directory: %s is not a directory\n", rootDir)
		os.Exit(0)
	}

	if !parallel {
		Log(INFO, "Running single threaded")
	} else {
		Log(INFO, "Running in parallel")
	}

	if yearEnd == 0 {
		yearEnd = 10000
	}
	if monthEnd == 0 {
		monthEnd = 10000
	} else {
		if monthEnd < 1 || monthEnd > 12 {
			Log(FATAL, "Ending month must be in the range 1-12")
		}
	}
	if monthStart != 0 {
		if monthStart < 1 || monthStart > 12 {
			Log(FATAL, "Start month must be in the range 1-12")
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
		case 1:
			chunkNames[i] = "Jan"
		case 2:
			chunkNames[i] = "Feb"
		case 3:
			chunkNames[i] = "Mar"
		case 4:
			chunkNames[i] = "Apr"
		case 5:
			chunkNames[i] = "May"
		case 6:
			chunkNames[i] = "Jun"
		case 7:
			chunkNames[i] = "Jul"
		case 8:
			chunkNames[i] = "Aug"
		case 9:
			chunkNames[i] = "Sep"
		case 10:
			chunkNames[i] = "Oct"
		case 11:
			chunkNames[i] = "Nov"
		case 12:
			chunkNames[i] = "Dec"
		default:
			chunkNames[i] = strconv.Itoa(i + 1)
		}
	}

	sigChannel := make(chan os.Signal)
	go func() {
		for sig := range sigChannel {
			switch sig {
			case syscall.SIGUSR1:
				Log(INFO, "Dumping stack traces due to SIGUSR1 request")
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			case syscall.SIGINT:
				Log(INFO, "Initiating shutdown due to SIGINT request")
				os.Exit(0)
			}
		}
	}()
	signal.Notify(sigChannel, syscall.SIGUSR1)
	signal.Notify(sigChannel, syscall.SIGINT)

}

func main() {
	fmt.Println("Root directory: ", rootDir)
	filepath.Walk(rootDir, cksumDataFiles)
}

func cksumDataFiles(filePath string, fi os.FileInfo, pathErr error) (err error) {
	if !isFile(filePath) {
		return nil
	}
	checkFile, _ := filepath.Rel(rootDir, filePath)
	ext := filepath.Ext(checkFile)
	if ext == ".bin" {
		checkFile = checkFile[:len(checkFile)-4]
		year, _ := strconv.Atoi(filepath.Base(checkFile))
		if year < yearStart || year > yearEnd {
			return nil
		}

		//Subtract the header size to get our gross chunksize
		size := fi.Size() - io.Headersize
		// Size the chunk buffer to be a multiple of 8-bytes
		chunkSize := io.AlignedSize(int(size/int64(numChunksPerFile) + size%int64(numChunksPerFile)))

		//		fmt.Println("Chunksize: ", chunkSize)
		fp, err := os.Open(filePath)
		if err != nil {
			fmt.Println(err.Error())
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
				go processChunk(chunkNum, offset, readBuffers[chunkNum][:bufferSize], fp, checkFile, &wg)
			} else {
				processChunk(chunkNum, offset, readBuffers[chunkNum][:bufferSize], fp, checkFile, &wg)
			}
			chunkNum++
		}
		wg.Wait()
		fp.Close()

		fmt.Printf("%30s", filechunks[0])
		for i, sum := range cksums {
			//			if sum != 0 {
			if sum < 0 {
				sum = -sum
			}
			fmt.Printf(",%3s %4d", chunkNames[i], sum%10000)
			//			}
		}
		fmt.Printf("\n")
	}
	return nil
}

func processChunk(myChunk int, offset int64, buffer []byte, fp *os.File, filename string, wg *sync.WaitGroup) {
	defer wg.Done()
	//		nread, err := fp.ReadAt(buffer, int64(myChunk*chunkSize))
	nread, err := fp.ReadAt(buffer, offset)
	if err != nil {
		if err.Error() != "EOF" {
			Log(FATAL, "Error reading "+fp.Name()+": "+err.Error())
		}
	}
	if nread == 0 {
		Log(FATAL, "Short read "+fp.Name())
	}
	// Align the checksum range to 8-bytes
	sumRange := io.AlignedSize(nread)
	if sumRange > nread {
		// Zero out padding bytes
		//fmt.Println("sumRange, nread = ", sumRange, nread)
		for i := nread; i < sumRange; i++ {
			buffer[i] = 0
		}
	}
	//				fmt.Println("Sumrange: ", sumRange)
	filechunks[myChunk] = filename
	cksums[myChunk] = bufferSum(buffer[:sumRange])
	/*
		Optionally fix errors in the metadata headers
		This is done only if the chunknum is 0 (header) and if the fixHeader flag has been set
	*/
	if fixHeaders && myChunk == 0 {
		fixKnownHeaderProblems(buffer, fp.Name())
	}
}

func fixKnownHeaderProblems(buffer []byte, filePath string) {
	header := (*io.Header)(unsafe.Pointer(&buffer[0]))
	tbinfo := io.NewTimeBucketInfoFromHeader(header, filePath)

	/*
		Check for OHLC with elementTypes = {1,1,1,1}
	*/
	if planner.ElementsEqual(tbinfo.GetElementTypes(), []io.EnumElementType{io.INT32, io.INT32, io.INT32, io.INT32}) {
		fmt.Println("found/fixing OHLC type error for ", filePath)
		tbinfo.SetElementTypes([]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32})
	}

	/*
		Write the new fileinfo to the file header
	*/
	fp, err := os.OpenFile(filePath, os.O_WRONLY, 0777)
	if err != nil {
		fmt.Println("Unable to write new header to file, terminating...")
		os.Exit(1)
	}
	io.WriteHeader(fp, tbinfo)
}

func bufferSum(buffer []byte) (sum int64) {
	// Swap the byte buffer for an int64 slice for higher performance
	data := io.SwapSliceByte(buffer, int64(0)).([]int64)
	for i := 0; i < len(buffer)/8; i++ {
		sum += data[i]
	}
	return sum
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
