package gRPC_Distribute_File_System

import (
	"fmt"
	"log"
	"net"

	DC2MS "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"
	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils"
	KeeperUtils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils/data-keeper"

	"golang.org/x/net/context"
)

type Datakeeper2MasterServer struct {
	DC2MS.UnimplementedDatakeeperServicesServer
}

func (s *Datakeeper2MasterServer) GetFileTransferState(ctx context.Context, req *DC2MS.FileTransferStateRequset) (*DC2MS.FileTransferStateResponse, error) {
	log.Printf("Received File Transfer State Check Request: %v", req)
	return KeeperUtils.GetFileTransferState(req.Name), nil
}

func (s *Datakeeper2MasterServer) ReplicateTo(ctx context.Context, req *DC2MS.ReplicateRequest) (*DC2MS.ReplicateResponse, error) {

	conn, err := net.Dial("tcp", req.To)
	if err != nil {
		fmt.Println("did not connect:", err)
		return &DC2MS.ReplicateResponse{Ok: false}, err
	}
	defer conn.Close()

	path := "fs/" + req.Filename
	done, _ := Utils.WriteFileToNetwork(path, &conn, true)
	if !done {
		fmt.Println("Error While Replicating File")
		return &DC2MS.ReplicateResponse{Ok: false}, err
	}

	return &DC2MS.ReplicateResponse{Ok: true}, nil
}
