package gRPC_Distribute_File_System

import (
	"log"

	KeeperNodes "github.com/MoAdelEzz/gRPC-Distribute-File-System/common/master-tracker"
	M2DCS "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"

	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)


type Master2DatakeeperServer struct {
	M2DCS.UnimplementedMaster2DatakeeperServicesServer
}


func (s *Master2DatakeeperServer) RegisterFile(ctx context.Context, req *M2DCS.RegisterFileRequest) (*M2DCS.RegisterFileResponse, error) {
	log.Printf("Received: %v", req)
	return &M2DCS.RegisterFileResponse{StatusCode: 200}, nil
}

func (s *Master2DatakeeperServer) SubscripeAsDataNode(ctx context.Context, req *M2DCS.SubscripeAsDataNodeRequest) (*M2DCS.SubscripeAsDataNodeResponse, error) {
	log.Printf("Received: %v", req)
	// print the ip:port of the client
	return &M2DCS.SubscripeAsDataNodeResponse{StatusCode: 200}, nil
}

func (s *Master2DatakeeperServer) HeartBeat(ctx context.Context, req *M2DCS.HeartBeatRequest) (*M2DCS.HeartBeatResponse, error) {
	p, ok := peer.FromContext(ctx)
	md, ok2 := metadata.FromIncomingContext(ctx)
	if ok && ok2 {
		log.Printf("Received Heartbeat from %v", p.Addr.String())
		KeeperNodes.RegisterHeartBeat(p.Addr.String(), md.Get("nodeName")[0], int(req.TotalSpace))
		return &M2DCS.HeartBeatResponse{StatusCode: 200}, nil
	} else {
		return &M2DCS.HeartBeatResponse{StatusCode: 500}, nil
	}
}