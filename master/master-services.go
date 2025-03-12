package main

import (
	Utils "MoA/Distubted-File-System"
	Services "MoA/Distubted-File-System/services"

	"errors"
	"log"
	"strconv"

	"golang.org/x/net/context"
)

type Master2ClientServer struct {
	Services.UnimplementedMaster2ClientServicesServer
}

func (s *Master2ClientServer) SelectMachineToCopyTo(ctx context.Context, req *Services.SelectMachineRequest) (*Services.SelectMachineResponse, error) {
	log.Printf("Received Select Machine Request")

	machineIp, FileTransferPort := GetSuitableMachine(req.Filename, int(req.SizeInBytes))

	if machineIp == Utils.FILE_EXISTS ||
		machineIp == Utils.NO_AVAILABLE_DATA_NODE {
		return nil, errors.New(machineIp)
	} else if FileTransferPort == -1 {
		return nil, errors.New("invalid port")
	} else {
		RegisterFileTransferStart(machineIp, req.Filename)
		return &Services.SelectMachineResponse{MachineAddress: machineIp + ":" + strconv.Itoa(int(FileTransferPort))}, nil
	}
}

func (s *Master2ClientServer) GetSourceMachine(ctx context.Context, req *Services.GetSourceMachineRequest) (*Services.GetSourceMachineResponse, error) {
	ip, port := GetSourceMachine(req.Filename)

	if ip == "" || port == -1 {
		return nil, errors.New(Utils.FILE_NOT_FOUND)
	} else {
		println("Found file " + req.Filename + " on " + ip + ":" + strconv.Itoa(int(port)))
		return &Services.GetSourceMachineResponse{MachineAddress: ip + ":" + strconv.Itoa(int(port))}, nil
	}
}
