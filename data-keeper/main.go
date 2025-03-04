package main

import (
	"fmt"
	"time"
	"os"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"context"
	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker"
)


const DATA_KEEPER_PORT = ":8080";
var available_space = 1000;
var name = "";
var ctx context.Context = nil;

func KeepalivePing(ctx context.Context, master *mt.MasterTrackerServicesClient) {
	for {
		(*master).HeartBeat(ctx, &mt.HeartBeatRequest{TotalSpace: int32(available_space)});
		time.Sleep(time.Second);
	}
}

func MasterTrackerConnectionHandler() {
	conn, err := grpc.Dial("localhost" + DATA_KEEPER_PORT, grpc.WithInsecure());
	if err != nil {
		fmt.Println("did not connect:", err);
		return;
	}
	defer conn.Close();

	MasterTracker := mt.NewMasterTrackerServicesClient(conn);
	
	go KeepalivePing(ctx, &MasterTracker);

	for {

	}
}

func ListenToClient () {
	
}

func main() {
	if len(os.Args) > 1 {
		name = os.Args[1];
	}
	
	md := metadata.Pairs("nodeName", name)
	ctx = metadata.NewOutgoingContext(context.Background(), md);

	go MasterTrackerConnectionHandler();
	go ListenToClient();

	for {

	}
}