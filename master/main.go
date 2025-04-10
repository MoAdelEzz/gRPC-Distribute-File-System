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
		log.Fatalf("Couldn't Start A Server On Port %v, Error: %v", datanodesPort, err)
		os.Exit(500)
	}

	grpcServer := grpc.NewServer()
	Services.RegisterMaster2DatakeeperServicesServer(grpcServer, &Master2DataNodeServer{})
	fmt.Println("Datanodes Server started. Listening on port " + datanodesPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Couldn't Attach GRPC Server On Port %v, Error: %v", datanodesPort, err)
		os.Exit(500)
	}
}

func ListenToClient() {
	defer MainSyncGroup.Done()

	clientsPort := ":" + os.Getenv("MASTER_CLIENTS_PORT")
	lis, err := net.Listen("tcp", clientsPort)
	if err != nil {
		log.Fatalf("Couldn't Start A Server On Port %v, Error: %v", clientsPort, err)
		os.Exit(500)
	}

	grpcServer := grpc.NewServer()
	Services.RegisterMaster2ClientServicesServer(grpcServer, &Master2ClientServer{})
	fmt.Println("Client Server started. Listening on port " + clientsPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Couldn't Attach GRPC Server On Port %v, Error: %v", clientsPort, err)
		os.Exit(500)
	}
}

func WatchDataNodesState() {
	defer MainSyncGroup.Done()
	for {
		time.Sleep(10 * time.Second)
		DeAttachGhostedMachines()
		EraseAbortedTransfers()
	}
}

func ReplicateFile(fromMachine Services.DatakeeperServicesClient, filename string, toMachines []string) {
	RegisterFileReplicateStart(toMachines, filename)
	fmt.Printf("Replicating file '%v' to machines: %v\n", filename, toMachines)

	resp, err := fromMachine.ReplicateTo(context.Background(), &Services.ReplicateRequest{
		Filename:       filename,
		MachineAddresses: toMachines,
	})

	if err != nil || !resp.Ok {
		println("Error Replicating File: ", err)
		AbortReplicate(toMachines, filename)
	} else {
		RegisterReplicateComplete(toMachines, filename)
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
			go ReplicateFile(fromMachine, file, toMachines)
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
