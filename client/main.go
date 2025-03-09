package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	MasterServices "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/client"
	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

var masterAddr string
var clientConn *grpc.ClientConn
var MasterTracker MasterServices.Master2ClientServicesClient = nil

func ConnectToMasterServices() {
	masterAddr = os.Getenv("MASTER_IP") + ":" + os.Getenv("MASTER_CLIENTS_PORT")
	var err error
	clientConn, err = grpc.Dial(masterAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}

	MasterTracker = MasterServices.NewMaster2ClientServicesClient(clientConn)
}

func GetDatanodeAddress(mode string, filepath string) string {
	if mode == "upload" {
		fileName, fileSize := Utils.GetFileMetaData(filepath)
		resp, err := MasterTracker.SelectMachineToCopyTo(context.Background(),
			&MasterServices.SelectMachineRequest{
				Filename: fileName,
				Size:     int32(fileSize)})

		println(resp.Ip)
		println(resp.Port)

		if err != nil {
			fmt.Println("Error: ", err)
			return ""
		}

		return resp.Ip + ":" + strconv.Itoa(int(resp.Port))
	} else {
		resp, err := MasterTracker.GetSourceMachine(context.Background(),
			&MasterServices.GetSourceMachineRequest{Filename: filepath})
		if err != nil {
			fmt.Println("Error: ", err)
			return ""
		}
		return resp.Ip + ":" + strconv.Itoa(int(resp.Port))
	}
}

func UploadFile(path string) bool {
	// selecting a proper machine from the master tracker
	conn, err := net.Dial("tcp", GetDatanodeAddress("upload", path))
	if err != nil {
		fmt.Println("did not connect:", err)
		return false
	}
	defer conn.Close()

	Utils.WriteChunck(&conn, []byte("UPLOAD"))

	// Writing File To Network
	done, _ := Utils.WriteFileToNetwork(path, &conn, true)
	if !done {
		fmt.Println("Error While Receiving File")
		return false
	}

	// waiting the data keeper to register the file transfer for the master tracker
	n, buffer := Utils.ReadChunck(&conn)
	message := string(buffer[:n])
	if message == "OK" {
		fmt.Println("File uploaded successfully")
	}

	return true
}

func DownloadFile(name string) bool {
	// selecting a proper machine from the master tracker
	datanodeAddr := GetDatanodeAddress("download", name)
	if datanodeAddr == "" {
		fmt.Println("Error: No datanode available")
		return false
	}

	conn, err := net.Dial("tcp", datanodeAddr)
	if err != nil {
		fmt.Println("did not connect:", err)
		return false
	}
	defer conn.Close()

	Utils.WriteChunck(&conn, []byte("DOWNLOAD"))
	Utils.WriteChunck(&conn, []byte(name))

	// Writing File To Network
	done, _, _ := Utils.ReadFileFromNetwork(name, &conn, "download")
	return done
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Missing arguments")
		// print usage <mode> <path or name>
		fmt.Println("Usage: client <mode> <path or name>")
	}

	mode := os.Args[1]
	path := os.Args[2]

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	ConnectToMasterServices()
	defer clientConn.Close()

	if mode == "upload" {
		UploadFile(path)
	} else {
		DownloadFile(path)
	}
}
