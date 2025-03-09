package Utils

import (
	"path/filepath"
	"log"
	"os"

	keeperServices "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"
	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils"
)

type FileTransferInstance struct {
	name string
	status Utils.FileState
}

type FileMetadata struct {
	name string
	size int
}

var transfers = make(map[string]FileTransferInstance);
var residentFiles []FileMetadata;

func GetResidentFiles() []*keeperServices.FileMetadata {
	if len(residentFiles) == 0 {
		residentFiles = LoadResidentFiles();
	}

	files := make([]*keeperServices.FileMetadata, 0, len(residentFiles));

	for _, file := range residentFiles {
		files = append(files, &keeperServices.FileMetadata{
			Name: file.name,
			Size: int32(file.size),
			FileState: string(Utils.RESIDENT),
		})
	}

	return files;
}

func LoadResidentFiles() []FileMetadata {
	directory := "fs";
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

		residentFiles = append(residentFiles, FileMetadata{
			name: name,
			size: int(size),
		})
	}

	return residentFiles;
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
		name: name, 
		status: Utils.ABORTED,
	};
}

