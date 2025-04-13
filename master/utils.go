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
	clientFileTransferPorts    []int32
	replicateFileTransferPorts []int32
	busyPorts                  map[string]bool
	ongoingTransfers           map[string]string // filename: port: state
	ongoungReplicates          map[string]string // filename: port: state
	services                   Services.DatakeeperServicesClient
	last_seen                  time.Time
	mutex                      *sync.Mutex
	capacity                   int
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

func GetRandomPort(machineIp string, target string) string {
	node := activeMachines[machineIp]

	availablePorts := []int32{}
	if target == "REPLICATE" {
		for _, port := range node.replicateFileTransferPorts {
			if !node.busyPorts[strconv.Itoa(int(port))] {
				availablePorts = append(availablePorts, port)
			}
		}
	} else {
		for _, port := range node.clientFileTransferPorts {
			if !node.busyPorts[strconv.Itoa(int(port))] {
				availablePorts = append(availablePorts, port)
			}
		}
	}

	if len(availablePorts) > 0 {
		rand.NewSource(time.Now().UnixNano())
		selectedPort := availablePorts[rand.Intn(len(availablePorts))]
		return strconv.Itoa(int(selectedPort))
	}
	return ""
}

func GetSuitableMachine(filename string, fileSize int) (string, string) {
	fileTableBorder.Wait()
	if _, ok := fileTable[filename]; ok {
		return Utils.FILE_EXISTS, "-1"
	}

	for _, machine := range activeMachines {
		machine.mutex.Lock()
			_, beingTransfered := machine.ongoingTransfers[filename]
			_, beingReplicated := machine.ongoungReplicates[filename]
			if beingTransfered || beingReplicated {
				machine.mutex.Unlock()
				return Utils.FILE_EXISTS, "-1"
			}
		machine.mutex.Unlock()
	}

	activeMachinesBorder.Wait()
	for ip, node := range activeMachines {
		node.mutex.Lock()
			// TODO: change this
			if fileSize >= 0 {
				port := GetRandomPort(ip, "CLIENT")	
				if port != "" {
					node.mutex.Unlock()
					return ip, port
				}
			}
		node.mutex.Unlock()
	}
	return Utils.NO_AVAILABLE_DATA_NODE, "-1"
}

func RegisterFileTransferStart(ip string, port string, filename string) {
	
	activeMachines[ip].mutex.Lock()
		activeMachines[ip].ongoingTransfers[filename] = port
		activeMachines[ip].busyPorts[port] = true
	activeMachines[ip].mutex.Unlock()
	fmt.Println("File ", filename, " Is Being Transferred to ", ip , ":", port)
}

func RegisterFileReplicateStart(toAddresses []string, filename string) {
	for _, address := range toAddresses {
		ip, port := ResolveAddress(address)
		fmt.Println("Replicating to IP:", ip, "on port:", port)

		activeMachines[ip].mutex.Lock()
			activeMachines[ip].busyPorts[port] = true
			activeMachines[ip].ongoungReplicates[filename] = port
		activeMachines[ip].mutex.Unlock()
	}
}

func RegisterDownloadComplete(nodeAddress string, port string) {
	ip, _ := ResolveAddress(nodeAddress)
	activeMachines[ip].mutex.Lock()
		activeMachines[ip].busyPorts[port] = false
	activeMachines[ip].mutex.Unlock()
}

func RegisterFileTransferComplete(nodeAddress string, filename string) {
	fmt.Println("File ", filename, " Has Been Transferred to ", nodeAddress, " successfully")
	ip, _ := ResolveAddress(nodeAddress)

	activeMachines[ip].mutex.Lock()
		activeMachines[ip].busyPorts[activeMachines[ip].ongoingTransfers[filename]] = false
		delete(activeMachines[ip].ongoingTransfers, filename)
	activeMachines[ip].mutex.Unlock()

	if _, ok := fileTable[filename]; !ok {
		fileTable[filename] = make(map[string]bool)
	}
	fileTable[filename][ip] = true
}

func RegisterReplicateComplete(nodeAddresses []string, filename string) {
	for _, nodeAddress := range nodeAddresses {
		ip, _ := ResolveAddress(nodeAddress)

		activeMachines[ip].mutex.Lock()
			activeMachines[ip].busyPorts[activeMachines[ip].ongoungReplicates[filename]] = false
			delete(activeMachines[ip].ongoungReplicates, filename)
		activeMachines[ip].mutex.Unlock()

		fmt.Println("File ", filename, " Has Been Replicated to ", nodeAddress, " successfully")
		fileTable[filename][ip] = true
	}
}

func AbortReplicate(machineAddresses []string, filename string) {
	for _, address := range(machineAddresses) {
		ip, _ := ResolveAddress(address)
		fmt.Println("Aborting replication on machine with ip ", ip)

		_, stillAlive := activeMachines[ip]
		if !stillAlive {
			continue
		}
		
		activeMachines[ip].mutex.Lock()
			activeMachines[ip].busyPorts[activeMachines[ip].ongoungReplicates[filename]] = false
			delete(activeMachines[ip].ongoungReplicates, filename)
		activeMachines[ip].mutex.Unlock()
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
			last_seen:                  time.Now(),
			clientFileTransferPorts:    req.ClientsPorts,
			replicateFileTransferPorts: req.ReplicatePorts,
			ongoingTransfers:           make(map[string]string),
			ongoungReplicates:          make(map[string]string),
			busyPorts: 					make(map[string]bool),			
			services:                   servicesConn,
			mutex:                      &sync.Mutex{},
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
		if time.Since(value.last_seen) > 2*time.Second+50*time.Millisecond {
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
					node.busyPorts[node.ongoingTransfers[filename]] = false
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
				fmt.Println("file ", filename, " is no longer available on machine ", ip)
				delete(machineIp, ip)
			}

			if len(machineIp) == 0 {
				fmt.Println("file ", filename, " is no longer available")
				delete(fileTable, filename)
			}
		}
	}
}

func GetSourceMachine(filename string) (string, string) {
	fileTableBorder.Wait()
	fileTableBorder.Add(1)
	defer fileTableBorder.Done()

	if _, ok := fileTable[filename]; !ok {
		return "", "-1"
	}

	for ip, _ := range fileTable[filename] {
		activeMachines[ip].mutex.Lock()
		// to handle ghosted machines before the daemon capture them so we ignore them here
		if activeMachines[ip].last_seen.Before(time.Now().Add(- 5 *time.Second)) {
			activeMachines[ip].mutex.Unlock()
			continue
		}
		activeMachines[ip].mutex.Unlock()

		port := GetRandomPort(ip, "CLIENT")

		if port != "" {
			activeMachines[ip].mutex.Lock()
				activeMachines[ip].busyPorts[port] = true
			activeMachines[ip].mutex.Unlock()
			return ip, port
		}
	}

	return "", "-2"
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

	numOfReplicas, _ := strconv.Atoi(os.Getenv("NUM_REPLICA"))
	numOfReplicas -= len(keys)

	for _, machine := range activeMachines {
		_, isReplicating := machine.ongoungReplicates[filename]
		if isReplicating {
			numOfReplicas -= 1
		}
	}

	if numOfReplicas <= 0 {
		return nil, []string{}
	}

	toMachines := []string{}

	for ip, _ := range activeMachines {
		// if the machine is already having the file
		if _, ok := machinesIp[ip]; ok {
			continue
		} else {
			port := GetRandomPort(ip, "REPLICATE")
			if port == "" {
				continue
			}

			fmt.Printf("selected port %v for machine %v\n", port, ip)
			
			toMachines = append(toMachines, ip+":"+port)
			if len(toMachines) == numOfReplicas {
				break
			}
		}
	}

	return from, toMachines

}
