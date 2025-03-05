package Utils

import (
	"path/filepath"
	"log"
	"os"

	DC2MS "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"
	common "github.com/MoAdelEzz/gRPC-Distribute-File-System/common"
)

type FileTransferInstance struct {
	name string
	status common.FileState
}

type FileMetadata struct {
	name string
	size int
}

var transfers = make(map[string]FileTransferInstance);
var residentFiles []FileMetadata;

func _GetFile(name string) FileMetadata {
	directory := "fs";
	filePath := filepath.Join(directory, name);

	info, err := os.Stat(filePath) // Get file metadata
	if err != nil {
		log.Println("Error getting file info:", err)
	}

	return FileMetadata{
		name: name,
		size: int(info.Size()),
	}
}

func GetResidentFiles() []*DC2MS.FileMetadata {
	if len(residentFiles) == 0 {
		residentFiles = LoadResidentFiles();
	}

	files := make([]*DC2MS.FileMetadata, 0, len(residentFiles));

	for _, file := range residentFiles {
		files = append(files, &DC2MS.FileMetadata{
			Name: file.name,
			Size: int32(file.size),
			FileState: string(common.RESIDENT),
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

func GetFileTransferState(name string) *DC2MS.FileTransferStateResponse {
	if _, ok := transfers[name]; !ok {
		return &DC2MS.FileTransferStateResponse{
			Status: string(transfers[name].status),
		}
	}

	return &DC2MS.FileTransferStateResponse{
		Status: string(common.RESIDENT),
	}
}

func FinishFileTransfer(name string) {
	transfers[name] = FileTransferInstance{
		name: name, 
		status: common.RESIDENT,
	};

	residentFiles = append(residentFiles, _GetFile(name));
}

func AbortFileTransfer(name string) {
	transfers[name] = FileTransferInstance{
		name: name, 
		status: common.ABORTED,
	};
}

