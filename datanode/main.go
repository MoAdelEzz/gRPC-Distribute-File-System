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

	Utils "MoA/Distubted-File-System"
	Services "MoA/Distubted-File-System/services"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var MasterServices Services.Master2DatakeeperServicesClient

var name = ""
var ctx context.Context = nil

var mainBorder sync.WaitGroup
var masterTrackerBorder sync.WaitGroup

func RangedList(start int, count int) []int32 {
	result := []int32{}
	for i := start; i < start+count; i++ {
		result = append(result, int32(i))		
	}
	return result
}

func GetClientTransferPorts() []int32 {
	fileTransferPortsStart, _ := strconv.Atoi(os.Getenv("DATANODE_FILE_TRANSFER_START_PORT"))
	fileTransferPortCount, _  := strconv.Atoi(os.Getenv("DATANODE_FILE_TRANSFER_PORT_COUNT"))
	return RangedList(fileTransferPortsStart, fileTransferPortCount)
}

func GetReplicateTransferPorts() []int32 {
	replicatePortStart, _  := strconv.Atoi(os.Getenv("DATANODE_REPLICATE_START_PORT"))
	replicatePortCount, _  := strconv.Atoi(os.Getenv("DATANODE_REPLICATE_PORT_COUNT"))
	return RangedList(replicatePortStart, replicatePortCount)
}

func KeepalivePing(ctx context.Context, master *Services.Master2DatakeeperServicesClient) {
	defer masterTrackerBorder.Done()

	filesystem := ReadFileSystem()

	for {
		_, err := (*master).HeartBeat(ctx, &Services.HeartBeatRequest{
			Filesystem:    filesystem,
			ClientsPorts:   GetClientTransferPorts(),
			ReplicatePorts: GetReplicateTransferPorts(),
		})
		if err != nil {
			log.Fatalf("Master Services Unavailable")
			os.Exit(500)
		}

		time.Sleep(time.Second)
	}
}
func HandleFileUpload(conn net.Conn) bool {
	// Reading The File From Network
	done, filename, byteCount := Utils.ReadFileFromNetwork("", &conn, "fs", false)

	if !done {
		fmt.Println("Error While Receiving File")
		return false
	}

	// update the master tracker
	resp, err := MasterServices.RegisterFile(ctx, &Services.RegisterFileRequest{Filename: filename})
	// TODO: revise this
	// notify the client
	if err != nil || !resp.Ok {
		conn.Write([]byte("ERROR"))
		conn.Read([]byte{})
		fmt.Println(err)
		return false
	} else {
		println("here")
		conn.Write([]byte("OK"))
		conn.Read([]byte{})
	}

	// Update The Lookup Table
	AppendFileToSystem(filename, byteCount)

	return true
}
func HandleFileDownload(conn net.Conn) bool {
	n, buffer := Utils.ReadChunckFromNetwork(&conn)
	filename := string(buffer[:n])
	path := "fs/" + filename

	done, _ := Utils.WriteFileToNetwork(path, &conn, false, false)
	if !done {
		fmt.Println("Error While Sending File")
		return false
	}

	return true
}
func ClientsFileTransfer(conn net.Conn) bool {
	defer conn.Close()

	n, buffer := Utils.ReadChunckFromNetwork(&conn)

	if string(buffer[:n]) == "UPLOAD" {
		return HandleFileUpload(conn)
	} else {
		return HandleFileDownload(conn)
	}
}

func ReplicateFileTransfer(conn net.Conn) bool {
	defer conn.Close()

	done, _, _ := Utils.ReadFileFromNetwork("", &conn, "fs", false)
	if !done {
		fmt.Println("Error While Sending File")
		return false
	}

	return true
}

func ConnectToMasterServices() {
	defer mainBorder.Done()

	masterAddr := os.Getenv("MASTER_IP") + ":" + os.Getenv("MASTER_DATANODES_PORT")

	conn, err := grpc.Dial(masterAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}
	defer conn.Close()

	MasterServices = Services.NewMaster2DatakeeperServicesClient(conn)
	go KeepalivePing(ctx, &MasterServices)

	masterTrackerBorder.Add(1)
	masterTrackerBorder.Wait()
}

// ===============================================================================
// =============================== Datanode Services ============================================
// ===============================================================================

func StartDatanodeServices() {
	defer mainBorder.Done()

	dataNodesServicesPort := ":" + os.Getenv("DATANODE_SERVICES_PORT")
	lis, err := net.Listen("tcp", dataNodesServicesPort)
	if err != nil {
		fmt.Println(err)
		return
	}

	grpcServer := grpc.NewServer()
	Services.RegisterDatakeeperServicesServer(grpcServer, &DataNode2MasterServer{})
	fmt.Println("Datanodes Services Server started. Listening on port " + dataNodesServicesPort + "...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func ListenToClientFileTransfer(port int32) {
	defer mainBorder.Done()

	listener, err := net.Listen("tcp", ":"+ strconv.Itoa(int(port)))
	if err != nil {
		fmt.Println(err)
		return
	}
	println("File Transfer Server started. Listening on port " + strconv.Itoa(int(port)) + "...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go ClientsFileTransfer(conn)
	}
}

func ListenToReplicateFileTransfer(port int32) {
	defer mainBorder.Done()

	listener, err := net.Listen("tcp", ":"+ strconv.Itoa(int(port)))
	if err != nil {
		fmt.Println(err)
		return
	}
	println("Replicate File Transfer Server started. Listening on port " + strconv.Itoa(int(port)) + "...")

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
	clientPorts := GetClientTransferPorts()
	replicatePorts := GetReplicateTransferPorts()


	mainBorder.Add(2)
	go StartDatanodeServices()
	go ConnectToMasterServices()

	for _, port := range clientPorts {
		mainBorder.Add(1)
		go ListenToClientFileTransfer(port)
	}

	for _, port := range replicatePorts {
		mainBorder.Add(1)
		go ListenToReplicateFileTransfer(port)
	}
	mainBorder.Wait()

}
