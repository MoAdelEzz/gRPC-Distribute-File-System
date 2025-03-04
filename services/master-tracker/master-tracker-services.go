package gRPC_Distribute_File_System

import (
	"errors"
	"log"

	KeeperNodes "github.com/MoAdelEzz/gRPC-Distribute-File-System/common"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type MasterTrackerServer struct {
	UnimplementedMasterTrackerServicesServer
}

func (s *MasterTrackerServer) SelectMachineToCopyTo(ctx context.Context, req *SelectMachineRequest) (*SelectMachineResponse, error) {
	log.Printf("Received Select Machine Request");

	machineKey := KeeperNodes.GetSuitableMachine(req.Filename, int(req.Size))
	port := KeeperNodes.GetPort(machineKey)

	if machineKey == KeeperNodes.FILE_EXISTS ||
		machineKey == KeeperNodes.NO_AVAILABLE_DATA_KEEPER {
		return nil, errors.New(machineKey)
	} else if port == -1 {
		return nil, errors.New("invalid port")
	} else {
		return &SelectMachineResponse{Port: int32(port)}, nil
	}
}

func (s *MasterTrackerServer) DownloadFile(ctx context.Context, req *GetSourceMachineRequest) (*GetSourceMachineResponse, error) {
	log.Printf("Received: %v", req)
	return &GetSourceMachineResponse{Port: 8080}, nil
}

func (s *MasterTrackerServer) RegisterFile(ctx context.Context, req *RegisterFileRequest) (*RegisterFileResponse, error) {
	log.Printf("Received: %v", req)
	return &RegisterFileResponse{StatusCode: 200}, nil
}

func (s *MasterTrackerServer) SubscripeAsDataNode(ctx context.Context, req *SubscripeAsDataNodeRequest) (*SubscripeAsDataNodeResponse, error) {
	log.Printf("Received: %v", req)
	// print the ip:port of the client
	return &SubscripeAsDataNodeResponse{StatusCode: 200}, nil
}

func (s *MasterTrackerServer) HeartBeat(ctx context.Context, req *HeartBeatRequest) (*HeartBeatResponse, error) {
	p, ok := peer.FromContext(ctx)
	md, ok2 := metadata.FromIncomingContext(ctx)
	if ok && ok2 {
		KeeperNodes.RegisterHeartBeat(p.Addr.String(), md.Get("nodeName")[0], int(req.TotalSpace))
		return &HeartBeatResponse{StatusCode: 200}, nil
	} else {
		return &HeartBeatResponse{StatusCode: 500}, nil
	}
}
