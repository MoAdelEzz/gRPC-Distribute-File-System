package utilities

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"

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
		fmt.Println("Directory already exists")
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

func ReadChunckFromNetwork(conn *net.Conn, exclude ...string ) (int, []byte) {
	
	chunckSize, _ := strconv.Atoi(os.Getenv("CHUNK_SIZE"))
	buffer := make([]byte, chunckSize)
	n, err := (*conn).Read(buffer)
	if err != nil { 
		return 0, nil
	} 

	(*conn).Write([]byte(ACK))
	return n, buffer
}

func TryWriteChunckToNetwork(conn *net.Conn, buffer []byte) bool {
	n := WriteChunckToNetwork(conn, buffer)
	return n != -1;
}

func WriteChunckToNetwork(conn *net.Conn, buffer []byte) int {
	chunckSize, _ := strconv.Atoi(os.Getenv("CHUNK_SIZE"))
	ack := make([]byte, chunckSize)
	_, err := (*conn).Write(buffer)
	if err != nil {
		fmt.Println(err)
		return -1
	}
	n, err := (*conn).Read(ack)
	if err != nil {
		fmt.Println(err)
		return -1
	}
	return n
}

func ReadFileFromNetwork(filename string, conn *net.Conn, folder string, showProgress bool) (bool, string, int) {
	if len(filename) == 0 {
		n, buffer := ReadChunckFromNetwork(conn)
		if buffer == nil {
			return false, "", -1
		}
		filename = string(buffer[:n])
		println("Recieved Filename = ", filename)
	}

	n, buffer := ReadChunckFromNetwork(conn)
	if buffer == nil {
		return false, filename, -1
	}

	fileSize, _ := strconv.Atoi(string(buffer[:n]))

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
	for {
		n, buffer := ReadChunckFromNetwork(conn)
		if buffer == nil {
			return false, filename, -1
		}

		message := string(buffer[:n])
		if n == 0 || message == EOF {
			fmt.Println("Download Complete")
			break
		}

		byteCount += n
		file.Write(buffer[:n])
		if showProgress {
			bar.Add(n)
		}
	}

	return true, filename, byteCount
}

func WriteFileToNetwork(path string, conn *net.Conn, sendName bool, showProgress bool) (bool, int) {
	file := OpenFile(path)
	defer file.Close()

	if sendName && !TryWriteChunckToNetwork(conn, []byte(filepath.Base(path))){
		return false, -1;
	}

	fileInfo, _ := file.Stat()
	if !TryWriteChunckToNetwork(conn, []byte(strconv.Itoa(int(fileInfo.Size())))) {
		return false, -1;
	}

	chunckSize, _ := strconv.Atoi(os.Getenv("CHUNK_SIZE"))
	byteWritten := 0
	nextChunk, closeFile := FileReader(path, chunckSize)
	defer closeFile()

	var bar *progressbar.ProgressBar
	if showProgress {
		bar = progressbar.Default(int64(fileInfo.Size()))
	} else {
		fmt.Println("file: ", filepath.Base(path), " is being uploaded")
	}

	for {
		buffer, err := nextChunk()

		if err != nil {
			if err == io.EOF {
				if !TryWriteChunckToNetwork(conn, []byte("EOF")) {
					return false, byteWritten
				} else {
					break
				}
			}
			return false, byteWritten
		}

		if n := WriteChunckToNetwork(conn, buffer); n == -1 {
			return false, byteWritten
		} else {
			byteWritten += n
		}

		if showProgress {
			bar.Add(len(buffer))
		}
	}

	fmt.Println("Upload Complete")

	return true, byteWritten
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
