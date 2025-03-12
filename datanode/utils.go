package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	Utils "MoA/Distubted-File-System"
	Services "MoA/Distubted-File-System/services"
)

// Filename: Transfer State
var ongoingTransfers = make(map[string]string)
var transfersBorder = sync.WaitGroup{}

// Filename: Size
var filesystem = make(map[string]int)
var filesystemBorder = sync.WaitGroup{}

func ReadFileSystem() []*Services.FileInfo {
	filesystemBorder.Add(1)

	directory := "fs"
	files, err := os.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(directory, file.Name())
		info, err := os.Stat(filePath)
		if err != nil {
			log.Fatal(err)
			continue
		}

		filesystem[file.Name()] = int(info.Size())
	}

	filesystemBorder.Done()

	residentFiles := []*Services.FileInfo{}
	for filename, size := range filesystem {
		residentFiles = append(residentFiles, &Services.FileInfo{Filename: filename, SizeInBytes: int32(size)})
	}
	return residentFiles
}

func AppendFileToSystem(name string, size int) {
	filesystemBorder.Add(1)
	filesystem[name] = size
	filesystemBorder.Done()
}

func GetFileTransferState(name string) *Services.FileTransferStateResponse {
	transfersBorder.Add(1)
	defer transfersBorder.Done()

	if _, ok := ongoingTransfers[name]; !ok {
		return &Services.FileTransferStateResponse{Status: string(ongoingTransfers[name])}
	}

	return &Services.FileTransferStateResponse{Status: string(Utils.RESIDENT)}
}

func AbortFileTransfer(name string) {
	transfersBorder.Add(1)
	defer transfersBorder.Done()
	ongoingTransfers[name] = Utils.ABORTED
}

func IsFileExists(filename string) bool {
	transfersBorder.Add(1)
	defer transfersBorder.Done()
	_, ok := filesystem[filename]
	return ok
}
