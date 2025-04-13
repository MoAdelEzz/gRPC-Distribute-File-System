package utilities

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/schollz/progressbar/v3"
)

// =================================================================
// =========================== CODES ===============================
// =================================================================

const (
	FILE_EXISTS            = "File Already Exists"
	FILE_NOT_FOUND         = "No Such File On The System"
	NO_AVAILABLE_DATA_NODE = "No Available Data Node"
	BEING_TRANSFERED       = "File Is Being Transfered"
	RESIDENT               = "File Is Resident On The Node"
	ABORTED                = "Transfer Aborted"
	ACK                    = "ACK"
	EOF                    = "EOF"
)

var read_ack_num int = 0;
var write_ack_num int = 0;

// =================================================================
// ================= OS & IO Utilities =============================
// =================================================================
func OpenFile(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return file
}

func GetFileMetaData(path string) (string, int64) {
	file, err := os.Stat(path)
	if err != nil {
		log.Fatal(err)
		return "", -1
	}
	return file.Name(), file.Size()
}

func MakeDirIfNotExists(directory string) bool {
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		err := os.Mkdir(directory, os.ModePerm)
		if err != nil {
			fmt.Println("Error creating directory:", err)
			return false
		} else {
			fmt.Println("Directory created successfully")
			return true
		}
	} else {
		return true
	}
}

func FileReader(filePath string, chunkSize int) (func() ([]byte, error), func()) {
	file, err := os.Open(filePath)
	if err != nil {
		return func() ([]byte, error) {
			return nil, err
		}, func() {}
	}

	// Cleanup function to close the file
	closeFunc := func() {
		file.Close()
	}

	// Closure function that returns the next chunk
	return func() ([]byte, error) {
		buffer := make([]byte, chunkSize)
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		return buffer[:n], nil
	}, closeFunc
}

// =================================================================
// ================= Network Utilities =============================
// =================================================================

func ReadChunckFromNetwork(reader *bufio.Reader) (int, []byte, bool) {
	buffer, err := reader.ReadBytes('\n')
	if err != nil {
		return 0, nil, false
	} 

	if strings.TrimSpace(string(buffer)) == "EOF" {
		return 0, nil, true
	} else {
		return len(buffer) - 1, buffer[:len(buffer) - 1], false
	}
}

func WriteChunckToNetwork(writer *bufio.Writer, buffer []byte) int {
	buffer = append(buffer, '\n')
	n, err := writer.Write(buffer)
	writer.Flush()
	
	if err != nil {
		return -1
	}
	return n
}

func TryWriteChunckToNetwork(writer *bufio.Writer, buffer []byte) bool {
	n := WriteChunckToNetwork(writer, buffer)
	return n != -1;
}

func ReadFileFromNetwork(filename string, reader *bufio.Reader, writer *bufio.Writer, folder string, showProgress bool) (bool, string, int) {
	fmt.Println("Reading file from network...")

	CHUNK_SIZE, _ := strconv.Atoi(os.Getenv("CHUNK_SIZE"))

	if len(filename) == 0 {
		_, buffer, _ := ReadChunckFromNetwork(reader)
		if buffer == nil {
			fmt.Println("Error While Reading Filename")
			return false, "", -1
		}
		filename = string(buffer)
		filename = strings.TrimSpace(filename)
	}

	n, buffer, _ := ReadChunckFromNetwork(reader)
	if buffer == nil {
		return false, filename, -1
	}
	
	fileSize, _ := strconv.Atoi( strings.TrimSpace(string(buffer[:n])) )
	fmt.Printf("Receiving file '%v' of size %v bytes...\n", filename, fileSize)
	

	done := MakeDirIfNotExists(folder)
	if !done {
		return false, filename, -1
	}

	file, err := os.Create(folder + "/" + filename)
	if err != nil {
		fmt.Println("OS Error")
		fmt.Println(err)
		return false, filename, -1
	}
	defer file.Close()

	var bar *progressbar.ProgressBar
	if showProgress {
		bar = progressbar.Default(int64(fileSize))
	} else {
		fmt.Println("file: ", filename, " is being downloaded")
	}

	byteCount := 0

	for fileSize > 0 {
		toRead := min(fileSize, CHUNK_SIZE)
		buffer := make([]byte, toRead) 
		io.ReadFull(reader, buffer)
		file.Write( buffer )
		fileSize -= toRead
		byteCount += toRead

		if showProgress {
			bar.Add(toRead)
		}
	}

	return true, filename, byteCount
}

func WriteFileToNetwork(path string, reader *bufio.Reader, writer *bufio.Writer, sendName bool, showProgress bool) (bool, int) {
	file := OpenFile(path)
	defer file.Close()

	if sendName && !TryWriteChunckToNetwork(writer, []byte(filepath.Base(path))){
		return false, -1;
	}

	fileInfo, _ := file.Stat()
	if !TryWriteChunckToNetwork(writer, []byte(strconv.Itoa(int(fileInfo.Size())))) {
		return false, -1;
	}
	
	fmt.Println("file: ", filepath.Base(path), " is being uploaded")

	bar := progressbar.DefaultBytes(fileInfo.Size(), "uploading " + filepath.Base(path))
	n, err := io.Copy(io.MultiWriter(writer, bar), file)
	if err != nil {
		return false, 0
	}

    if err := writer.Flush(); err != nil {
        return false, 0
    }

	fmt.Println("Upload Complete")
	return true, int(n)
}

// =================================================================
// ================= Array Utilities ===============================
// =================================================================

func RemoveValueFromArray(slice []string, value string) []string {
	newSlice := []string{}
	for _, v := range slice {
		if v != value {
			newSlice = append(newSlice, v) // Keep elements that are not the target value
		}
	}
	return newSlice
}

func AppendIfNotExist(lst []string, value string) []string {
	for _, v := range lst {
		if v == value {
			return lst
		}
	}
	return append(lst, value)
}
