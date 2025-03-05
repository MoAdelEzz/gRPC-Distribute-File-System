package main

import (
	"fmt"
	"log"
	"time"
	"net"

	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services"
	cs "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/client"
	ks "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"

	utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/common/master-tracker"

	"google.golang.org/grpc"

	"sync"
	"os"
	"github.com/joho/godotenv"
)


var wg sync.WaitGroup

func ListenToDataKeepers () {
	defer wg.Done();

	datakeepersPort := ":" + os.Getenv("MASTER_DATAKEEPERS_PORT")
	lis, err := net.Listen("tcp", datakeepersPort);
	if err != nil {
		fmt.Println(err);
		return;
	}

	grpcServer := grpc.NewServer();
	ks.RegisterMaster2DatakeeperServicesServer(grpcServer, &mt.Master2DatakeeperServer{});
	fmt.Println("Datakeepers Server started. Listening on port " + datakeepersPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err);
	}
}

func ListenToClient () {
	defer wg.Done();
	
	clientsPort := ":" + os.Getenv("MASTER_CLIENTS_PORT")
	lis, err := net.Listen("tcp", clientsPort);
	if err != nil {
		fmt.Println(err);
		return;
	}

	grpcServer := grpc.NewServer();
	cs.RegisterMaster2ClientServicesServer(grpcServer, &mt.Master2ClientServer{});
	fmt.Println("Client Server started. Listening on port " + clientsPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err);
	}
}

func WatchFileTransferState () {
	defer wg.Done();

	for {
		time.Sleep(10 * time.Second);
		utils.DeAttachGhostedMachines();
		utils.EraseAbortedTransfers();
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	wg.Add(3);

	go ListenToDataKeepers();
	go ListenToClient();
	go WatchFileTransferState();

	wg.Wait();
}