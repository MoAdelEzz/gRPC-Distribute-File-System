package Utils

import (
	"log"
	"os"
	"path/filepath"

	keeperServices "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"
	MasterTrackerServices "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"
	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils"
)

type FileTransferInstance struct {
	name   string
	status Utils.FileState
}

type FileMetadata struct {
	name string
	size int
}

var transfers = make(map[string]FileTransferInstance)
var filesystem = make(map[string]int)

func ReadFileSystem() []*MasterTrackerServices.FileInfo {
	if len(filesystem) == 0 {
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

			name := file.Name()
			size := info.Size()
			filesystem[name] = int(size)
		}
	}

	residentFiles := []*MasterTrackerServices.FileInfo{}
	for filename, size := range filesystem {
		residentFiles = append(residentFiles, &MasterTrackerServices.FileInfo{
			Name: filename,
			Size: int32(size),
		})
	}
	return residentFiles
}

func AppendFileToSystem(name string, size int) {
	if _, ok := filesystem[name]; ok {
		return
	}
	filesystem[name] = size
}

func GetFileTransferState(name string) *keeperServices.FileTransferStateResponse {
	if _, ok := transfers[name]; !ok {
		return &keeperServices.FileTransferStateResponse{
			Status: string(transfers[name].status),
		}
	}

	return &keeperServices.FileTransferStateResponse{
		Status: string(Utils.RESIDENT),
	}
}

func AbortFileTransfer(name string) {
	transfers[name] = FileTransferInstance{
		name:   name,
		status: Utils.ABORTED,
	}
}

func IsFileExists(filename string) bool {
	_, ok := filesystem[filename]
	return ok
}
