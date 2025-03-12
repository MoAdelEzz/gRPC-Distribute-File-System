package main

import (
	"fmt"
	"log"
	"net"

	Utils "MoA/Distubted-File-System"
	Services "MoA/Distubted-File-System/services"

	"golang.org/x/net/context"
)

type DataNode2MasterServer struct {
	Services.UnimplementedDatakeeperServicesServer
}

func (s *DataNode2MasterServer) GetFileTransferState(ctx context.Context, req *Services.FileTransferStateRequset) (*Services.FileTransferStateResponse, error) {
	log.Printf("Received File Transfer State Check Request: %v", req)
	return GetFileTransferState(req.Filename), nil
}

func (s *DataNode2MasterServer) ReplicateTo(ctx context.Context, req *Services.ReplicateRequest) (*Services.ReplicateResponse, error) {

	conn, err := net.Dial("tcp", req.MachineAddress)
	if err != nil {
		fmt.Println("did not connect:", err)
		return &Services.ReplicateResponse{Ok: false}, err
	}
	defer conn.Close()

	path := "fs/" + req.Filename
	done, _ := Utils.WriteFileToNetwork(path, &conn, true)
	if !done {
		fmt.Println("Error While Replicating File")
		return &Services.ReplicateResponse{Ok: false}, err
	}

	return &Services.ReplicateResponse{Ok: true}, nil
}
