package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	Utils "MoA/Distubted-File-System"
	Services "MoA/Distubted-File-System/services"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

var masterAddr string
var clientConn *grpc.ClientConn
var MasterTracker Services.Master2ClientServicesClient = nil

func ConnectToMasterServices() {
	masterAddr = os.Getenv("MASTER_IP") + ":" + os.Getenv("MASTER_CLIENTS_PORT")
	var err error
	clientConn, err = grpc.Dial(masterAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}

	MasterTracker = Services.NewMaster2ClientServicesClient(clientConn)
}

func GetDatanodeAddress(mode string, filepath string) string {
	if mode == "upload" {
		fileName, fileSize := Utils.GetFileMetaData(filepath)
		resp, err := MasterTracker.SelectMachineToCopyTo(context.Background(),
			&Services.SelectMachineRequest{
				Filename:    fileName,
				SizeInBytes: int32(fileSize)})

		if err != nil {
			fmt.Println("Error: ", err)
			return ""
		}

		return resp.MachineAddress
	} else {
		resp, err := MasterTracker.GetSourceMachine(context.Background(),
			&Services.GetSourceMachineRequest{Filename: filepath})
		if err != nil {
			fmt.Println("Error: ", err)
			return ""
		}
		return resp.MachineAddress
	}
}

func UploadFile(path string) bool {
	// selecting a proper machine from the master tracker
	address := GetDatanodeAddress("upload", path)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("did not connect:", err)
		return false
	}
	defer conn.Close()

	Utils.WriteChunckToNetwork(&conn, []byte("UPLOAD"))

	// Writing File To Network
	done, _ := Utils.WriteFileToNetwork(path, &conn, true, true)
	if !done {
		fmt.Println("Error While Receiving File")
		return false
	}
	
	// waiting the data keeper to register the file transfer for the master tracker
	n, buffer := Utils.ReadChunckFromNetwork(&conn)
	message := string(buffer[:n])
	if message == "OK" {
		fmt.Println("File uploaded successfully")
	} else {
		fmt.Println("Error: ", message)
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

	Utils.WriteChunckToNetwork(&conn, []byte("DOWNLOAD"))
	Utils.WriteChunckToNetwork(&conn, []byte(name))

	// Writing File To Network
	done, _, _ := Utils.ReadFileFromNetwork(name, &conn, "download", true)
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
