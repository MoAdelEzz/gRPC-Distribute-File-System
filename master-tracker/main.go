package main

import (
	"fmt"
	"log"
	"net"
	"time"

	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker"
	KeeperNodes "github.com/MoAdelEzz/gRPC-Distribute-File-System/common"
	"google.golang.org/grpc"
)

const DATA_KEEPER_PORT = ":8080";
const CLIENTS_PORT = ":8081";

func ListenToDataKeepers () {
	lis, err := net.Listen("tcp", DATA_KEEPER_PORT);
	if err != nil {
		fmt.Println(err);
		return;
	}

	grpcServer := grpc.NewServer();
	mt.RegisterMasterTrackerServicesServer(grpcServer, &mt.MasterTrackerServer{});
	fmt.Println("Server started. Listening on port 8080...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err);
	}
}

func ListenToClient () {
	lis, err := net.Listen("tcp", CLIENTS_PORT);
	if err != nil {
		fmt.Println(err);
		return;
	}

	grpcServer := grpc.NewServer();
	mt.RegisterMasterTrackerServicesServer(grpcServer, &mt.MasterTrackerServer{});
	fmt.Println("Server started. Listening on port 8081...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err);
	}
}

func WatchFileTransferState () {
	for {
		time.Sleep(5 * time.Second)
		KeeperNodes.AbortIdleFileTransfers();
		println("Done Checking")
	}
}

func main() {
	go ListenToDataKeepers();
	go ListenToClient();
	go WatchFileTransferState();

	for {
		
	}
}