package gRPC_Distribute_File_System

import (
	"log"

	M2DCS "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"
	KeeperNodes "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils/master-tracker"

	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type Master2DatakeeperServer struct {
	M2DCS.UnimplementedMaster2DatakeeperServicesServer
}

func (s *Master2DatakeeperServer) RegisterFile(ctx context.Context, req *M2DCS.RegisterFileRequest) (*M2DCS.RegisterFileResponse, error) {
	p, ok := peer.FromContext(ctx)
	_, ok2 := metadata.FromIncomingContext(ctx)
	if ok && ok2 {
		log.Printf("Received Register File Request from %v", p.Addr.String())
		KeeperNodes.RegisterFileTransferComplete(p.Addr.String(), req.Filename)
	}

	return &M2DCS.RegisterFileResponse{StatusCode: 200}, nil
}

func (s *Master2DatakeeperServer) HeartBeat(ctx context.Context, req *M2DCS.HeartBeatRequest) (*M2DCS.HeartBeatResponse, error) {
	p, ok := peer.FromContext(ctx)
	md, ok2 := metadata.FromIncomingContext(ctx)
	if ok && ok2 {
		// log.Printf("Received Heartbeat from %v", p.Addr.String())
		KeeperNodes.RegisterHeartBeat(p.Addr.String(), md.Get("nodeName")[0], req)
		return &M2DCS.HeartBeatResponse{StatusCode: 200}, nil
	} else {
		return &M2DCS.HeartBeatResponse{StatusCode: 500}, nil
	}
}
