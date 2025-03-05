package KeeperNodes

import (
	"strings"
	"time"
	"strconv"
	"context"
	"os"
	"google.golang.org/grpc"
	"fmt"

	dc "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"
	common "github.com/MoAdelEzz/gRPC-Distribute-File-System/common"
)

type DataKeeper struct {
	available_space int
	last_seen time.Time
	name string
}

type FileLocation struct {
	ip string
	port int
	state common.FileState
}

var datakeepers = make(map[string]DataKeeper); // the string will be ip:port
var inverseFileLookup = make(map[string][]FileLocation);
var ongoingTransfers = make(map[string]string);

func GetInstance() *map[string]DataKeeper {
    return &datakeepers
}


func HasKey(key string) bool {
	_, ok := datakeepers[key];
	return ok	
}

func GetAvailableSpace(key string) int {
	return datakeepers[key].available_space
}

func ResolveAddress(key string) (string, int) {
	if key == "" {
		return "", -1
	}

	parts := strings.Split(key, ":")
	num, err := strconv.Atoi(parts[len(parts) - 1])
	if err == nil {
		return parts[0], num
	} else {
		return "", -1
	}
}

func GetLastSeen(key string) time.Time {
	return datakeepers[key].last_seen
}

func GetIpPort(key string) (ip string, port int) {
	parts := strings.Split(key, ":")
	port, err := strconv.Atoi(parts[len(parts) - 1])
	if err != nil {
		port = -1
	}
	return strings.Join(parts[:len(parts) - 1], ":"), port
}

func GetSuitableMachine(filename string, fileSize int) string {
	if _, ok := inverseFileLookup[filename]; ok {
		return common.FILE_EXISTS;
	}

	for key, value := range datakeepers {
		if value.available_space >= fileSize {
			return key
		}
	}

	return common.NO_AVAILABLE_DATA_KEEPER;
}

func RegisterHeartBeat(key string, nodeName string, available_space int) {
	datakeepers[key] = DataKeeper{
		name: nodeName,
		available_space: available_space, 
		last_seen: time.Now(),
	}
}

func RegisterFileTransferStart(key string, filename string) {
	fmt.Println("Registering transfer ", filename, " to ", key)
	ongoingTransfers[key] = filename
}

func DeAttachGhostedMachines() {
	for key, value := range datakeepers {
		if time.Since(value.last_seen) > time.Second + 10 * time.Millisecond {
			fmt.Println("Machine ", key, " has gone offline")
			delete(datakeepers, key)
		}
	}
}

func EraseAbortedTransfers() {
	for machineAddr, filename := range ongoingTransfers {
		if _, ok := datakeepers[machineAddr]; !ok {
			fmt.Println("uploading file ", filename, " to machine ", machineAddr, " has been aborted")
			delete(ongoingTransfers, machineAddr)
			continue
		}

		ip, _ := ResolveAddress(machineAddr)

		conn, err := grpc.Dial(ip + ":" + os.Getenv("DATAKEEPER_SERVICES_PORT"), grpc.WithInsecure());
		if err != nil {
			fmt.Println("did not connect:", err);
			return;
		}
		defer conn.Close();

		DataKeeper := dc.NewDatakeeperServicesClient(conn);
		resp, err := DataKeeper.GetFileTransferState(context.Background(), &dc.FileTransferStateRequset{
			Name: filename,
		})		

		if err != nil {
			fmt.Println("Error: ", err)
			os.Exit(404)
		}

		if common.FileState(resp.Status) == common.ABORTED {
			fmt.Println("uploading file ", filename, " to machine ", machineAddr, " has been aborted")
			delete(ongoingTransfers, machineAddr)
		}
	}
}

func RevalidateLookupTable() {
	for key, fileLocation := range inverseFileLookup {
		for _, location := range fileLocation {
			if _, ok := datakeepers[location.ip + ":" + strconv.Itoa(location.port)]; !ok {
				fmt.Println("file ", key, " is no longer available on machine ", location.ip + ":" + strconv.Itoa(location.port))
				delete(inverseFileLookup, key)
			}
		}
	}
}