package Utils

import (
	"context"
	"fmt"
	"math/rand"
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
	last_seen                 time.Time
	name                      string
	filesystem                []*mt.FileInfo
	clientFileTransferPort    int32
	replicateFileTransferPort int32
	services                  dc.DatakeeperServicesClient
}

var datakeepers = make(map[string]DataKeeper) // the string will be ip:port
var inverseFileLookup = make(map[string]map[string]bool)
var ongoingTransfers = make(map[string][]string)

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

	for ip, node := range datakeepers {
		// TODO: change this
		if fileSize > 0 {
			return ip, node.clientFileTransferPort
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

func RegisterFileTransferStart(ip string, filename string) {
	fmt.Println("Registering transfer ", filename, " to ", ip)
	ongoingTransfers[ip] = append(ongoingTransfers[ip], filename)
}

func RegisterFileTransferComplete(key string, filename string) {
	ip, _ := ResolveAddress(key)

	ongoingTransfers[ip] = removeValue(ongoingTransfers[ip], filename)
	if _, ok := inverseFileLookup[filename]; !ok {
		inverseFileLookup[filename] = make(map[string]bool)
	}
	inverseFileLookup[filename][ip] = true
}

func RegisterReplicateComplete(key string, filename string) {
	ip, _ := ResolveAddress(key)

	println("Registering replicate ", filename, " to ", ip)

	if _, ok := inverseFileLookup[filename]; !ok {
		inverseFileLookup[filename] = make(map[string]bool)
	}
	inverseFileLookup[filename][ip] = true
}

func RegisterHeartBeat(address string, nodeName string, req *mt.HeartBeatRequest) {
	var DataKeeperServices dc.DatakeeperServicesClient
	ip, _ := ResolveAddress(address)

	if _, ok := datakeepers[ip]; ok {
		DataKeeperServices = datakeepers[ip].services
	} else {
		port := os.Getenv("DATAKEEPER_SERVICES_PORT")
		conn, err := grpc.Dial(ip+":"+port, grpc.WithInsecure())
		if err != nil {
			fmt.Println("did not connect:", err)
			DataKeeperServices = nil
		} else {
			DataKeeperServices = dc.NewDatakeeperServicesClient(conn)
		}
	}

	datakeepers[ip] = DataKeeper{
		name:                      nodeName,
		filesystem:                req.Files,
		last_seen:                 time.Now(),
		clientFileTransferPort:    req.FileTransferPort,
		replicateFileTransferPort: req.ReplicatePort,
		services:                  DataKeeperServices,
	}

	for _, file := range req.Files {
		if _, ok := inverseFileLookup[file.Name]; !ok {
			inverseFileLookup[file.Name] = make(map[string]bool)
		}
		inverseFileLookup[file.Name][ip] = true
	}
}

func DeAttachGhostedMachines() {
	for ip, value := range datakeepers {
		if time.Since(value.last_seen) > time.Second+10*time.Millisecond {
			fmt.Println("Machine ", ip, " has gone offline")
			delete(datakeepers, ip)
		}
	}
}

func EraseAbortedTransfers() {
	for ip, filenames := range ongoingTransfers {
		for _, filename := range filenames {
			if _, ok := datakeepers[ip]; !ok {
				fmt.Println("uploading file ", filename, " to machine ", ip, " has been aborted")
				delete(ongoingTransfers, ip)
				continue
			}

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
				fmt.Println("uploading file ", filename, " to machine ", ip, " has been aborted")
				delete(ongoingTransfers, ip)
			}
		}
	}
}

func RevalidateLookupTable() {
	for filename, machineIp := range inverseFileLookup {
		for address, _ := range machineIp {
			if _, ok := datakeepers[address]; !ok {
				fmt.Println("file ", filename, " is no longer available on machine ", machineIp)
				delete(machineIp, address)
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

func GetFilesToReplicate() []string {
	var files []string
	for filename, machinesList := range inverseFileLookup {
		if len(machinesList) == 0 {
			delete(inverseFileLookup, filename)
		}
		if len(machinesList) < 2 {
			files = append(files, filename)
		}
	}
	return files
}

func GetMachineToReplicate(filename string) (dc.DatakeeperServicesClient, string) {
	machinesIp := inverseFileLookup[filename]

	keys := make([]string, 0, len(machinesIp))
	for k := range machinesIp {
		keys = append(keys, k)
	}
	randMachine := rand.Intn(len(keys))
	from := datakeepers[keys[randMachine]].services

	for ip, node := range datakeepers {
		// if the machine is already having the file
		if _, ok := machinesIp[ip]; ok {
			continue
		} else {
			port := node.replicateFileTransferPort
			return from, ip + ":" + strconv.Itoa(int(port))
		}
	}
	return nil, ""
}
