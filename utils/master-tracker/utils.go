package Utils

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	dc "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/datakeeper"
	mt "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/datakeeper"
	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils"
)

type DataKeeper struct {
	last_seen              time.Time
	name                   string
	filesystem             []*mt.FileInfo
	clientFileTransferPort int32
}

type FileLocation struct {
	ip   string
	port int
}

var datakeepers = make(map[string]DataKeeper) // the string will be ip:port
var inverseFileLookup = make(map[string]map[string]bool)
var ongoingTransfers = make(map[string][]string)
var unscannedMachines = make([]string, 0)

func GetUnscannedMachines() []string {
	return unscannedMachines
}

func GetAvailableSpace(key string) int {
	size := 0
	for _, file := range datakeepers[key].filesystem {
		size += int(file.Size)
	}
	return size
}

func ResolveAddress(key string) (string, int) {
	if key == "" {
		return "", -1
	}

	parts := strings.Split(key, ":")
	num, err := strconv.Atoi(parts[len(parts)-1])
	if err == nil {
		return parts[0], num
	} else {
		return "", -1
	}
}

func GetSuitableMachine(filename string, fileSize int) (string, int32) {
	if _, ok := inverseFileLookup[filename]; ok {
		return Utils.FILE_EXISTS, -1
	}

	for key, node := range datakeepers {
		// TODO: change this
		if fileSize > 0 {
			return key, node.clientFileTransferPort
		}
	}

	return Utils.NO_AVAILABLE_DATA_KEEPER, -1
}

func removeValue(slice []string, value string) []string {
	newSlice := []string{}
	for _, v := range slice {
		if v != value {
			newSlice = append(newSlice, v) // Keep elements that are not the target value
		}
	}
	return newSlice
}

func RegisterFileTransferComplete(key string, filename string) {
	ongoingTransfers[key] = removeValue(ongoingTransfers[key], filename)

	if _, ok := inverseFileLookup[filename]; !ok {
		inverseFileLookup[filename] = make(map[string]bool)
	}
	inverseFileLookup[filename][key] = true
}

func RegisterHeartBeat(key string, nodeName string, req *mt.HeartBeatRequest) {

	datakeepers[key] = DataKeeper{
		name:                   nodeName,
		filesystem:             req.Files,
		last_seen:              time.Now(),
		clientFileTransferPort: req.FileTransferPort,
	}

	println("Machine ", datakeepers[key].clientFileTransferPort, " has been registered")

	ip, _ := ResolveAddress(key)

	for _, file := range req.Files {
		nodeKey := ip + ":" + strconv.Itoa(int(req.FileTransferPort))

		if _, ok := inverseFileLookup[file.Name]; !ok {
			inverseFileLookup[file.Name] = make(map[string]bool)
		}
		inverseFileLookup[file.Name][nodeKey] = true
	}
}

func RegisterFileTransferStart(key string, filename string) {
	fmt.Println("Registering transfer ", filename, " to ", key)
	ongoingTransfers[key] = append(ongoingTransfers[key], filename)
}

func DeAttachGhostedMachines() {
	for key, value := range datakeepers {
		if time.Since(value.last_seen) > time.Second+10*time.Millisecond {
			fmt.Println("Machine ", key, " has gone offline")
			delete(datakeepers, key)
		}
	}
}

func EraseAbortedTransfers() {
	for machineAddr, filenames := range ongoingTransfers {
		for _, filename := range filenames {
			if _, ok := datakeepers[machineAddr]; !ok {
				fmt.Println("uploading file ", filename, " to machine ", machineAddr, " has been aborted")
				delete(ongoingTransfers, machineAddr)
				continue
			}

			ip, _ := ResolveAddress(machineAddr)

			conn, err := grpc.Dial(ip+":"+os.Getenv("DATAKEEPER_SERVICES_PORT"), grpc.WithInsecure())
			if err != nil {
				fmt.Println("did not connect:", err)
				return
			}
			defer conn.Close()

			DataKeeper := dc.NewDatakeeperServicesClient(conn)
			resp, err := DataKeeper.GetFileTransferState(context.Background(), &dc.FileTransferStateRequset{
				Name: filename,
			})

			if err != nil {
				fmt.Println("Error: ", err)
				os.Exit(404)
			}

			if Utils.FileState(resp.Status) == Utils.ABORTED {
				fmt.Println("uploading file ", filename, " to machine ", machineAddr, " has been aborted")
				delete(ongoingTransfers, machineAddr)
			}
		}
	}
}

func RevalidateLookupTable() {
	for filename, machineAddresses := range inverseFileLookup {
		for address, _ := range machineAddresses {
			if _, ok := datakeepers[address]; !ok {
				fmt.Println("file ", filename, " is no longer available on machine ", machineAddresses)
				delete(machineAddresses, address)
			}
		}
	}
}

func GetSourceMachine(filename string) (string, int32) {
	if _, ok := inverseFileLookup[filename]; !ok {
		return "", -1
	}

	for key, _ := range inverseFileLookup[filename] {
		ip, port := ResolveAddress(key)
		return ip, int32(port)
	}

	return "", -1
}
