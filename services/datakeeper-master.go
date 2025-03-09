package gRPC_Distribute_File_System

import (
	"log"

	DC2MS "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"
	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils/data-keeper"

	"golang.org/x/net/context"
)

type Datakeeper2MasterServer struct {
	DC2MS.UnimplementedDatakeeperServicesServer
}

func (s *Datakeeper2MasterServer) GetResidentFiles(ctx context.Context, req *DC2MS.ResidentFilesRequest) (*DC2MS.ResidentFilesResponse, error) {
	log.Printf("Received Get Resident Files Request: %v", req)

	residentFiles := Utils.GetResidentFiles()

	return &DC2MS.ResidentFilesResponse{Files: residentFiles}, nil
}

func (s *Datakeeper2MasterServer) GetFileTransferState(ctx context.Context, req *DC2MS.FileTransferStateRequset) (*DC2MS.FileTransferStateResponse, error) {
	log.Printf("Received File Transfer State Check Request: %v", req)
	return Utils.GetFileTransferState(req.Name), nil
}
