package main

import (
	"fmt"
	"context"
	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/client"
	"google.golang.org/grpc"
	"log"
	"os"
	"io"
	"net"
	"github.com/joho/godotenv"
)

var masterAddr string;
var clientConn *grpc.ClientConn;
var MasterTracker mt.Master2ClientServicesClient = nil;

func ConnectToMasterTrackerGRPC() {
	masterAddr = os.Getenv("MASTER_IP") + ":" + os.Getenv("MASTER_CLIENTS_PORT")
	var err error;
	clientConn, err = grpc.Dial(masterAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}
	
	MasterTracker = mt.NewMaster2ClientServicesClient(clientConn)
}

func UploadFile(path string) {
	fileSize := 500;
	resp, err := MasterTracker.SelectMachineToCopyTo(context.Background(), 
	&mt.SelectMachineRequest{Filename: "test2.mp4" , Size: int32(fileSize)})

	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(404)
	}
	
	println(path)
	
	ip := resp.Ip
	// port := resp.Port

	
	return;
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ip, 32770))
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}
	defer conn.Close()

	
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer f.Close()

	buf := make([]byte, 1024)
	n, err := f.Read(buf)

	if err != nil && err != io.EOF {
		log.Fatalf("%v", err)
	}
	
	_, err = conn.Write([]byte(buf[:n]))

	if err != nil {
		fmt.Println("Error sending data:", err.Error())
		return
	}


}

func DownloadFile(name string) {

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

	ConnectToMasterTrackerGRPC();
	defer clientConn.Close();

	if mode == "upload" {
		UploadFile(path)
	} else {
		DownloadFile(path)
	}

	
}