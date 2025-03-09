package Utils

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

const (
	SUCCESS   = 200
	FAILED    = 500
	NOT_FOUND = 404

	FILE_EXISTS  = "FILE_ALREADY_EXISTS"
	NO_SUCH_FILE = "NO_SUCH_FILE"

	NO_AVAILABLE_DATA_KEEPER = "NO_AVAILABLE_DATA_KEEPER"
)

type FileState string

const (
	BEING_TRANSFERED FileState = "FILE_BEING_TRANSFERED"
	RESIDENT         FileState = "RESIDENT"
	ABORTED          FileState = "ABORTED"
)

func OpenFile(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return file
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

func ReadChunck(conn *net.Conn) (int, []byte) {
	buffer := make([]byte, 1024)
	n, err := (*conn).Read(buffer)
	if err != nil {
		fmt.Println(err)
		return 0, nil
	}

	(*conn).Write([]byte("ACK"))
	return n, buffer
}

func WriteChunck(conn *net.Conn, buffer []byte) int {
	ack := make([]byte, 1024)
	_, err := (*conn).Write(buffer)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	n, err := (*conn).Read(ack)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	fmt.Println("ACK = ", string(ack[:n]))
	return n
}

func GetFileMetaData(path string) (string, int64) {
	file, err := os.Stat(path)
	if err != nil {
		log.Fatal(err)
	}
	return file.Name(), file.Size()
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

func ReadFileFromNetwork(filename string, conn *net.Conn, folder string) (bool, string, int) {
	if len(filename) == 0 {
		n, buffer := ReadChunck(conn)
		filename = string(buffer[:n])
		println("Recieved Filename = ", filename)
	}

	done := MakeDirIfNotExists(folder)
	if !done {
		return false, "", -1
	}

	file, err := os.Create(folder + "/" + filename)
	if err != nil {
		fmt.Println("OS Error")
		return false, "", -1
	}
	defer file.Close()

	byteCount := 0
	for {
		n, buffer := ReadChunck(conn)

		message := string(buffer[:n])
		if message == "EOF" {
			break
		}

		byteCount += n
		file.Write(buffer[:n])
	}

	return true, filename, byteCount
}

func WriteFileToNetwork(filename string, conn *net.Conn, sendName bool) (bool, int) {
	file := OpenFile(filename)
	defer file.Close()

	if sendName {
		WriteChunck(conn, []byte(filename))
	}

	byteWritten := 0
	nextChunk, closeFile := FileReader(filename, 1024)
	defer closeFile()

	for {
		buffer, err := nextChunk()
		if err != nil {
			if err == io.EOF {
				WriteChunck(conn, []byte("EOF"))
				break
			}
			return false, byteWritten
		}
		println(string(buffer))
		println("==============")
		byteWritten += WriteChunck(conn, buffer)
	}

	return true, byteWritten
}
