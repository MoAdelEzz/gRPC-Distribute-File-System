package main

import (
	"fmt"
	"context"
	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker"
	"google.golang.org/grpc"
)


const MASTER_PORT = ":8081";
func main() {
	conn, err := grpc.Dial("localhost" + MASTER_PORT, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}
	defer conn.Close()

	MasterTracker := mt.NewMasterTrackerServicesClient(conn)

	fileSize := 500;

	// Call the RPC method
	resp, err := MasterTracker.SelectMachineToCopyTo(context.Background(), 
	&mt.SelectMachineRequest{Filename: "test.mp4" , Size: int32(fileSize)})

	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("Port:", resp.Port)
	}
}