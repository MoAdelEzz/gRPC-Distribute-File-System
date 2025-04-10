package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	Services "MoA/Distubted-File-System/services"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

var MainSyncGroup sync.WaitGroup

func ListenToDataNodes() {
	defer MainSyncGroup.Done()

	datanodesPort := ":" + os.Getenv("MASTER_DATANODES_PORT")
	lis, err := net.Listen("tcp", datanodesPort)
	if err != nil {
		fmt.Println(err)
		return
	}

	grpcServer := grpc.NewServer()
	Services.RegisterMaster2DatakeeperServicesServer(grpcServer, &Master2DataNodeServer{})
	fmt.Println("Datanodes Server started. Listening on port " + datanodesPort + "...")

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
	Services.RegisterMaster2ClientServicesServer(grpcServer, &Master2ClientServer{})
	fmt.Println("Client Server started. Listening on port " + clientsPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func WatchDataNodesState() {
	defer MainSyncGroup.Done()

	for {
		time.Sleep(15 * time.Second)
		DeAttachGhostedMachines()
		EraseAbortedTransfers()
	}
}

func ReplicateFiles() {
	defer MainSyncGroup.Done()

	for {
		println("Started File Replication Check")

		filesToReplicate := GetFilesToReplicate()

		if len(filesToReplicate) == 0 {
			println("No Files To Replicate")
		} else {
			println("Files To Replicate: ")

			for _, file := range filesToReplicate {
				println(file)
			}
		}

		for _, file := range filesToReplicate {
			fromMachine, toMachines := GetMachineToReplicate(file)
			if fromMachine == nil {
				println("No Available Machine To Replicate To")
				break
			}

			resp, err := fromMachine.ReplicateTo(context.Background(), &Services.ReplicateRequest{
				Filename:       file,
				MachineAddresses: toMachines,
			})

			if err != nil || !resp.Ok {
				println("Error Replicating File: ", err)
				break
			}

			RegisterReplicateComplete(toMachines, file)
		}

		time.Sleep(10 * time.Second)
	}

}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	go ListenToDataNodes()
	go ListenToClient()
	go WatchDataNodesState()
	go ReplicateFiles()

	MainSyncGroup.Add(4)
	MainSyncGroup.Wait()
}
