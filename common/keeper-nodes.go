package KeeperNodes

import (
	"strings"
	"time"
	"strconv"
)

type DataKeeper struct {
	available_space int
	last_seen time.Time
	name string
}

type FileLocation struct {
	ip string
	port int
	state FileState
	last_state_change time.Time 
}

var datakeepers = make(map[string]DataKeeper); // the string will be ip:port
var inverseFileLookup = make(map[string][]FileLocation);

func GetInstance() *map[string]DataKeeper {
    return &datakeepers
}

func AbortIdleFileTransfers() {
	
	for key, machines := range inverseFileLookup {
		for _, record := range machines {
			if record.state == BEING_TRANSFERED && time.Since(record.last_state_change) > 5 * time.Second {
				println("Aborting idle file transfer for file ", key, " on machine ", record.ip, ":", record.port)
				delete(inverseFileLookup, key)
			}
		}
	}
}

func HasKey(key string) bool {
	_, ok := datakeepers[key];
	return ok	
}

func GetAvailableSpace(key string) int {
	return datakeepers[key].available_space
}

func GetPort(key string) int {
	if key == "" {
		return -1
	}
	parts := strings.Split(key, ":")
	num, err := strconv.Atoi(parts[len(parts) - 1])
	if err == nil {
		return num
	} else {
		return -1
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
	DeAttachAbsentKeepers()

	if _, ok := inverseFileLookup[filename]; ok {
		return FILE_EXISTS;
	}

	for key, value := range datakeepers {
		if value.available_space >= fileSize {
			ip, port := GetIpPort(key)
			fileRecord := FileLocation{
				ip: ip,
				port: port,
				state: BEING_TRANSFERED,
				last_state_change: time.Now(),
			}
			inverseFileLookup[filename] = append(inverseFileLookup[filename], fileRecord)
			return key
		}
	}

	return NO_AVAILABLE_DATA_KEEPER;
}

func RegisterHeartBeat(key string, nodeName string, available_space int) {
	datakeepers[key] = DataKeeper{
		name: nodeName,
		available_space: available_space, 
		last_seen: time.Now(),
	}
}

func DeAttachAbsentKeepers() {
	for key, value := range datakeepers {
		if time.Since(value.last_seen) > time.Second + 100 * time.Millisecond {
			delete(datakeepers, key)
		}
	}
}
