package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	Utils "MoA/Distubted-File-System"
	Services "MoA/Distubted-File-System/services"
)

type DataNode struct {
	clientFileTransferPort    int32
	replicateFileTransferPort int32
	ongoingTransfers          map[string]bool
	services                  Services.DatakeeperServicesClient
	last_seen                 time.Time
	mutex                     *sync.Mutex
}

// Ip: Node Metadata
var activeMachines = make(map[string]DataNode)
var activeMachinesBorder = sync.WaitGroup{}

// Filename: List of Machines IP
var fileTable = make(map[string]map[string]bool)
var fileTableBorder = sync.WaitGroup{}

func ResolveAddress(key string) (string, string) {
	parts := strings.Split(key, ":")
	return parts[0], parts[1]
}

func GetSuitableMachine(filename string, fileSize int) (string, int32) {
	fileTableBorder.Wait()
	if _, ok := fileTable[filename]; ok {
		return Utils.FILE_EXISTS, -1
	}

	activeMachinesBorder.Wait()
	for ip, node := range activeMachines {
		// TODO: change this
		node.mutex.Lock()
		if fileSize >= 0 {
			node.mutex.Unlock()
			return ip, node.clientFileTransferPort
		}
		node.mutex.Unlock()
	}
	return Utils.NO_AVAILABLE_DATA_NODE, -1
}

func RegisterFileTransferStart(ip string, filename string) {
	fmt.Println("File ", filename, " Is Being Transferred to ", ip)

	activeMachines[ip].mutex.Lock()
	activeMachines[ip].ongoingTransfers[filename] = true
	activeMachines[ip].mutex.Unlock()
}

func RegisterFileTransferComplete(nodeAddress string, filename string) {
	fmt.Println("File ", filename, " Has Been Transferred to ", nodeAddress, " successfully")
	ip, _ := ResolveAddress(nodeAddress)

	activeMachines[ip].mutex.Lock()
	delete(activeMachines[ip].ongoingTransfers, filename)
	activeMachines[ip].mutex.Unlock()

	if _, ok := fileTable[filename]; !ok {
		fileTable[filename] = make(map[string]bool)
	}
	fileTable[filename][ip] = true
}

func RegisterReplicateComplete(nodeAddresses []string, filename string) {
	for _, nodeAddress := range nodeAddresses {
		fmt.Println("File ", filename, " Has Been Replicated to ", nodeAddress, " successfully")
		ip, _ := ResolveAddress(nodeAddress)
		fileTable[filename][ip] = true
	}
}

func RegisterHeartBeat(machineAddress string, req *Services.HeartBeatRequest) {
	ip, _ := ResolveAddress(machineAddress)
	port := os.Getenv("DATANODE_SERVICES_PORT")

	if node, exists := activeMachines[ip]; exists {
		node.mutex.Lock()
		node.last_seen = time.Now()
		activeMachines[ip] = node
		node.mutex.Unlock()
	} else {
		conn, _ := grpc.Dial(ip+":"+port, grpc.WithInsecure())
		servicesConn := Services.NewDatakeeperServicesClient(conn)

		activeMachinesBorder.Add(1)
		activeMachines[ip] = DataNode{
			last_seen:                 time.Now(),
			clientFileTransferPort:    req.ClientsPort,
			replicateFileTransferPort: req.ReplicatePort,
			ongoingTransfers:          make(map[string]bool),
			services:                  servicesConn,
			mutex:                     &sync.Mutex{},
		}
		activeMachinesBorder.Done()
	}

	fileTableBorder.Add(1)
	for _, file := range req.Filesystem {
		if _, ok := fileTable[file.Filename]; !ok {
			fileTable[file.Filename] = make(map[string]bool)
		}
		fileTable[file.Filename][ip] = true
	}
	fileTableBorder.Done()
}

func DeAttachGhostedMachines() {
	activeMachinesBorder.Add(1)

	OfflineFound := false
	for ip, value := range activeMachines {
		if time.Since(value.last_seen) > time.Second+50*time.Millisecond {
			fmt.Println("Machine ", ip, " has gone offline")
			OfflineFound = true
			delete(activeMachines, ip)
		}
	}

	if OfflineFound {
		RevalidateLookupTable()
	}
	activeMachinesBorder.Done()
}

func EraseAbortedTransfers() {
	for ip, node := range activeMachines {
		node.mutex.Lock()
		for filename, _ := range node.ongoingTransfers {
			resp, _ := node.services.GetFileTransferState(context.Background(), &Services.FileTransferStateRequset{Filename: filename})

			if resp.Status == Utils.ABORTED {
				fmt.Println("uploading file ", filename, " to machine ", ip, " has been aborted")
				delete(node.ongoingTransfers, filename)
			}
		}
		node.mutex.Unlock()
	}
}

func RevalidateLookupTable() {
	activeMachinesBorder.Add(1)
	fileTableBorder.Add(1)

	defer activeMachinesBorder.Done()
	defer fileTableBorder.Done()

	for filename, machineIp := range fileTable {
		for ip, _ := range machineIp {
			if _, ok := activeMachines[ip]; !ok {
				fmt.Println("file ", filename, " is no longer available on machine ", machineIp)
				delete(machineIp, ip)
			}

			if len(machineIp) == 0 {
				fmt.Println("file ", filename, " is no longer available")
				delete(fileTable, filename)
			}
		}
	}
}

func GetSourceMachine(filename string) (string, int32) {
	fileTableBorder.Wait()
	fileTableBorder.Add(1)
	defer fileTableBorder.Done()

	if _, ok := fileTable[filename]; !ok {
		return "", -1
	}

	for ip, _ := range fileTable[filename] {
		port := activeMachines[ip].clientFileTransferPort
		return ip, int32(port)
	}

	return "", -1
}

func GetFilesToReplicate() []string {
	fileTableBorder.Wait()
	fileTableBorder.Add(1)
	defer fileTableBorder.Done()
	NUM_REPLICA, _ := strconv.Atoi(os.Getenv("NUM_REPLICA"))

	var files []string
	for filename, machinesIp := range fileTable {
		if len(machinesIp) == 0 {
			delete(fileTable, filename)
		}
		if len(machinesIp) < NUM_REPLICA {
			files = append(files, filename)
		}
	}
	return files
}

func GetMachineToReplicate(filename string) (Services.DatakeeperServicesClient, []string) {
	fileTableBorder.Wait()
	fileTableBorder.Add(1)
	defer fileTableBorder.Done()

	activeMachinesBorder.Wait()
	activeMachinesBorder.Add(1)
	defer activeMachinesBorder.Done()

	machinesIp := fileTable[filename]

	if len(machinesIp) == 0 {
		return nil, []string{}
	}

	keys := make([]string, 0, len(machinesIp))
	for k := range machinesIp {
		keys = append(keys, k)
	}
	randMachine := rand.Intn(len(keys))
	from := activeMachines[keys[randMachine]].services

	NUM_REPLICA, _ := strconv.Atoi(os.Getenv("NUM_REPLICA"));
	toMachines := []string{}

	for ip, node := range activeMachines {
		// if the machine is already having the file
		if _, ok := machinesIp[ip]; ok {
			continue
		} else {
			port := node.replicateFileTransferPort
			toMachines = append(toMachines, ip+":"+strconv.Itoa(int(port)))
			if len(toMachines) >= NUM_REPLICA - len(keys) {
				return from, toMachines
			}
		}
	}
	return nil, []string{}
}
