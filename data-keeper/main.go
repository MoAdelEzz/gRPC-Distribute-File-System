package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"net"
	"strconv"
	"time"
	"sync"
	"io"

	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"

	dc 	"github.com/MoAdelEzz/gRPC-Distribute-File-System/services"
	dcs "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)


var available_space = 1000;
var name = "";
var ctx context.Context = nil;

var mainBorder sync.WaitGroup;
var masterTrackerBorder sync.WaitGroup;

func KeepalivePing(ctx context.Context, master *mt.Master2DatakeeperServicesClient) {
	defer masterTrackerBorder.Done();
	for {
		(*master).HeartBeat(ctx, &mt.HeartBeatRequest{TotalSpace: int32(available_space)});
		time.Sleep(time.Second);
	}
}

func MasterTrackerConnectionHandler() {
	defer mainBorder.Done();

	masterAddr := os.Getenv("MASTER_IP") + ":" + os.Getenv("MASTER_DATAKEEPERS_PORT")
	max_space, err := strconv.Atoi(os.Getenv("DATAKEEPER_MAX_CAPACITY"));
	if err != nil {
		max_space = 1000;
	}

	available_space = max_space;

	conn, err := grpc.Dial(masterAddr, grpc.WithInsecure());
	if err != nil {
		fmt.Println("did not connect:", err);
		return;
	}
	defer conn.Close();

	println(conn.Target());

	MasterTracker := mt.NewMaster2DatakeeperServicesClient(conn);

	masterTrackerBorder.Add(1);
	go KeepalivePing(ctx, &MasterTracker);
	masterTrackerBorder.Wait();
}

func handleFileTransferConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)

	n, err := conn.Read(buffer)
	if err != nil {
		if err == io.EOF {
			fmt.Println("Connection closed")
		} else {
			fmt.Println(err)
		}
		return
	}

	// write the buffer to some output file in this directory
	file, err := os.Create(name + ".txt")
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = file.Write(buffer[:n])
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
}

func ListenToClientFileTransfer () {
	defer mainBorder.Done();

	listener, err := net.Listen("tcp", ":" + os.Getenv("DATAKEEPER_FILE_TRANSFER_PORT"))
	if err != nil {
		fmt.Println(err)
		return
	}
	println("File Transfer Server started. Listening on port " + os.Getenv("DATAKEEPER_FILE_TRANSFER_PORT") + "...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go handleFileTransferConnection(conn)
	}
}

func StartServicesServer () {
	defer mainBorder.Done();

	datakeepersServicesPort := ":" + os.Getenv("DATAKEEPER_SERVICES_PORT")
	lis, err := net.Listen("tcp", datakeepersServicesPort);
	if err != nil {
		fmt.Println(err);
		return;
	}

	grpcServer := grpc.NewServer();
	dcs.RegisterDatakeeperServicesServer(grpcServer, &dc.Datakeeper2MasterServer{});
	fmt.Println("Datakeepers Services Server started. Listening on port " + datakeepersServicesPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err);
	}
}

func main() {
	if len(os.Args) > 1 {
		name = os.Args[1];
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	
	md := metadata.Pairs("nodeName", name)
	ctx = metadata.NewOutgoingContext(context.Background(), md);

	mainBorder.Add(3);
	go MasterTrackerConnectionHandler();
	go ListenToClientFileTransfer();
	go StartServicesServer();
	mainBorder.Wait();
	
}