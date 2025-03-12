package main

import (
	Services "MoA/Distubted-File-System/services"
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type Master2DataNodeServer struct {
	Services.UnimplementedMaster2DatakeeperServicesServer
}

func (s *Master2DataNodeServer) RegisterFile(ctx context.Context, req *Services.RegisterFileRequest) (*Services.RegisterFileResponse, error) {
	p, ok := peer.FromContext(ctx)
	_, ok2 := metadata.FromIncomingContext(ctx)
	if ok && ok2 {
		log.Printf("Received Register File Request from %v", p.Addr.String())
		RegisterFileTransferComplete(p.Addr.String(), req.Filename)
	}

	return &Services.RegisterFileResponse{Ok: true}, nil
}

func (s *Master2DataNodeServer) HeartBeat(ctx context.Context, req *Services.HeartBeatRequest) (*Services.HeartBeatResponse, error) {
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("Received Heartbeat from %v", p.Addr.String())
		RegisterHeartBeat(p.Addr.String(), req)
		return &Services.HeartBeatResponse{Ok: true}, nil
	} else {
		return &Services.HeartBeatResponse{Ok: true}, nil
	}
}
