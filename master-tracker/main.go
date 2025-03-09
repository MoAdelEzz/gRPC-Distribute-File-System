package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	MasterTracker "github.com/MoAdelEzz/gRPC-Distribute-File-System/services"
	MasterClientServices "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/client"
	MasterKeeperServices "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"
	MasterUtils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils/master-tracker"
	"github.com/joho/godotenv"
)

var MainSyncGroup sync.WaitGroup

func ListenToDataKeepers() {
	defer MainSyncGroup.Done()

	datakeepersPort := ":" + os.Getenv("MASTER_DATAKEEPERS_PORT")
	lis, err := net.Listen("tcp", datakeepersPort)
	if err != nil {
		fmt.Println(err)
		return
	}

	grpcServer := grpc.NewServer()
	MasterKeeperServices.RegisterMaster2DatakeeperServicesServer(grpcServer, &MasterTracker.Master2DatakeeperServer{})
	fmt.Println("Datakeepers Server started. Listening on port " + datakeepersPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func ListenToClient() {
	defer MainSyncGroup.Done()

	clientsPort := ":" + os.Getenv("MASTER_CLIENTS_PORT")
	lis, err := net.Listen("tcp", clientsPort)
	if err != nil {
		fmt.Println(err)
		return
	}

	grpcServer := grpc.NewServer()
	MasterClientServices.RegisterMaster2ClientServicesServer(grpcServer, &MasterTracker.Master2ClientServer{})
	fmt.Println("Client Server started. Listening on port " + clientsPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func WatchDatakeepersState() {
	defer MainSyncGroup.Done()

	for {
		time.Sleep(15 * time.Second)
		MasterUtils.DeAttachGhostedMachines()
		MasterUtils.EraseAbortedTransfers()
	}
}

func ReplicateFiles() {
	defer MainSyncGroup.Done()
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	go ListenToDataKeepers()
	go ListenToClient()
	go WatchDatakeepersState()
	go ReplicateFiles()

	MainSyncGroup.Add(4)
	MainSyncGroup.Wait()
}
