package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"

	dc "github.com/MoAdelEzz/gRPC-Distribute-File-System/services"
	dcs "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"

	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils"
	KeeperUtils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils/data-keeper"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var MasterServices mt.Master2DatakeeperServicesClient

var name = ""
var ctx context.Context = nil

var mainBorder sync.WaitGroup
var masterTrackerBorder sync.WaitGroup

func KeepalivePing(ctx context.Context, master *mt.Master2DatakeeperServicesClient) {
	defer masterTrackerBorder.Done()

	filesystem := KeeperUtils.ReadFileSystem()
	fileTransferPort, _ := strconv.Atoi(os.Getenv("DATAKEEPER_FILE_TRANSFER_PORT"))
	replicatePort, _ := strconv.Atoi(os.Getenv("DATAKEEPER_REPLICATE_PORT"))

	for {
		_, err := (*master).HeartBeat(ctx, &mt.HeartBeatRequest{
			Files:            filesystem,
			FileTransferPort: int32(fileTransferPort),
			ReplicatePort:    int32(replicatePort),
		})
		if err != nil {
			fmt.Println("Error: ", err)
			os.Exit(404)
		}

		time.Sleep(time.Second)
	}
}

func HandleFileUpload(conn net.Conn) bool {
	// Reading The File From Network
	done, filename, byteCount := Utils.ReadFileFromNetwork("", &conn, "fs")

	if !done {
		fmt.Println("Error While Receiving File")
		return false
	}

	// update the master tracker
	resp, err := MasterServices.RegisterFile(ctx, &mt.RegisterFileRequest{
		Filename: filename,
		Filesize: int32(byteCount),
	})
	// TODO: revise this
	// notify the client
	if err != nil || resp.StatusCode != 200 {
		conn.Write([]byte("ERROR"))
		conn.Read([]byte{})
		fmt.Println(err)
		return false
	} else {
		conn.Write([]byte("OK"))
		conn.Read([]byte{})
	}

	// Update The Lookup Table
	KeeperUtils.AppendFileToSystem(filename, byteCount)

	return true
}
func HandleFileDownload(conn net.Conn) bool {
	n, buffer := Utils.ReadChunck(&conn)
	filename := string(buffer[:n])
	path := "fs/" + filename

	done, _ := Utils.WriteFileToNetwork(path, &conn, false)
	if !done {
		fmt.Println("Error While Sending File")
		return false
	}

	return true
}

func ClientsFileTransfer(conn net.Conn) bool {
	defer conn.Close()

	n, buffer := Utils.ReadChunck(&conn)

	if string(buffer[:n]) == "UPLOAD" {
		HandleFileUpload(conn)
	} else {
		HandleFileDownload(conn)
	}

	Utils.WriteChunck(&conn, []byte("ACK"))
	return true
}

func ReplicateFileTransfer(conn net.Conn) bool {
	defer conn.Close()

	done, _, _ := Utils.ReadFileFromNetwork("", &conn, "fs")
	if !done {
		fmt.Println("Error While Sending File")
		return false
	}

	return true
}

func ConnectToMasterServices() {
	defer mainBorder.Done()

	masterAddr := os.Getenv("MASTER_IP") + ":" + os.Getenv("MASTER_DATAKEEPERS_PORT")

	conn, err := grpc.Dial(masterAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}
	defer conn.Close()

	MasterServices = mt.NewMaster2DatakeeperServicesClient(conn)
	go KeepalivePing(ctx, &MasterServices)

	masterTrackerBorder.Add(1)
	masterTrackerBorder.Wait()
}

// ===============================================================================
// =============================== Datanode Services ============================================
// ===============================================================================

func StartDatanodeServices() {
	defer mainBorder.Done()

	datakeepersServicesPort := ":" + os.Getenv("DATAKEEPER_SERVICES_PORT")
	lis, err := net.Listen("tcp", datakeepersServicesPort)
	if err != nil {
		fmt.Println(err)
		return
	}

	grpcServer := grpc.NewServer()
	dcs.RegisterDatakeeperServicesServer(grpcServer, &dc.Datakeeper2MasterServer{})
	fmt.Println("Datakeepers Services Server started. Listening on port " + datakeepersServicesPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func ListenToClientFileTransfer() {
	defer mainBorder.Done()

	listener, err := net.Listen("tcp", ":"+os.Getenv("DATAKEEPER_FILE_TRANSFER_PORT"))
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

		go ClientsFileTransfer(conn)
	}
}

func ListenToReplicateFileTransfer() {
	defer mainBorder.Done()

	listener, err := net.Listen("tcp", ":"+os.Getenv("DATAKEEPER_REPLICATE_PORT"))
	if err != nil {
		fmt.Println(err)
		return
	}
	println("Replicate File Transfer Server started. Listening on port " + os.Getenv("DATAKEEPER_REPLICATE_PORT") + "...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go ReplicateFileTransfer(conn)
	}
}

func main() {
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	md := metadata.Pairs("nodeName", name)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	mainBorder.Add(4)
	go StartDatanodeServices()
	go ConnectToMasterServices()
	go ListenToClientFileTransfer()
	go ListenToReplicateFileTransfer()
	mainBorder.Wait()

}
