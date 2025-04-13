package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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
	fileTransferPortCount, _ := strconv.Atoi(os.Getenv("DATANODE_FILE_TRANSFER_PORT_COUNT"))
	return RangedList(fileTransferPortsStart, fileTransferPortCount)
}

func GetReplicateTransferPorts() []int32 {
	replicatePortStart, _ := strconv.Atoi(os.Getenv("DATANODE_REPLICATE_START_PORT"))
	replicatePortCount, _ := strconv.Atoi(os.Getenv("DATANODE_REPLICATE_PORT_COUNT"))
	return RangedList(replicatePortStart, replicatePortCount)
}

func KeepalivePing(ctx context.Context, master *Services.Master2DatakeeperServicesClient) {
	defer masterTrackerBorder.Done()

	filesystem := ReadFileSystem()

	for {
		_, err := (*master).HeartBeat(ctx, &Services.HeartBeatRequest{
			Filesystem:     filesystem,
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
func HandleFileUpload(reader *bufio.Reader, writer *bufio.Writer) bool {
	// Reading The File From Network	
	done, filename, byteCount := Utils.ReadFileFromNetwork("", reader, writer, "fs", false)
	if !done {
		fmt.Println("Error While Receiving File, Aborting Transfer")
		AbortFileTransfer(filename)
		return false
	} else {
		fmt.Printf("Received file '%v' of size %v bytes\n", filename, byteCount)
	}

	// update the master tracker
	resp, err := MasterServices.RegisterFile(ctx, &Services.RegisterFileRequest{Filename: filename})
	fmt.Printf("Registered file '%v' with master\n", filename)

	if err != nil || !resp.Ok {
		Utils.WriteChunckToNetwork(writer, []byte("ERROR"))
		println("Failed")
		return false
	} else {
		Utils.WriteChunckToNetwork(writer, []byte("OK"))
		AppendFileToSystem(filename, byteCount)
		println("Ok")
		return true
	}
}
func HandleFileDownload(nodePort int32, reader *bufio.Reader, writer *bufio.Writer) bool {
	n, buffer, _ := Utils.ReadChunckFromNetwork(reader)
	filename := strings.TrimSpace(string(buffer[:n]))
	path := "fs/" + filename

	done, _ := Utils.WriteFileToNetwork(path, reader, writer, false, false)
	if !done {
		fmt.Println("Error While Sending File")
		return false
	} else {
		MasterServices.RegisterDownloadComplete(ctx, &Services.RegisterDownloadCompleteRequest{Port: strconv.Itoa(int(nodePort))})
	}

	return true
}
func ClientsFileTransfer(conn net.Conn) bool {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	nodePort := int32(conn.LocalAddr().(*net.TCPAddr).Port)

	n, buffer, _ := Utils.ReadChunckFromNetwork(reader)

	if strings.TrimSpace(string(buffer[:n])) == "UPLOAD" {
		return HandleFileUpload(reader, writer)
	} else {
		return HandleFileDownload(nodePort, reader, writer)
	}
}

func ReplicateFileTransfer(conn net.Conn) bool {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	done, filename, _ := Utils.ReadFileFromNetwork("", reader, writer, "fs", false)
	if !done {
		fmt.Println("Error While Reciving Replicated File File")
		os.Remove("fs/" + filename)
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

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(port)))
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

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(port)))
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
