package gRPC_Distribute_File_System

import (
	"errors"
	"log"
	"strconv"

	MasterClientServices "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/client"
	Utils "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils"
	KeeperNodes "github.com/MoAdelEzz/gRPC-Distribute-File-System/utils/master-tracker"

	"golang.org/x/net/context"
)

type Master2ClientServer struct {
	MasterClientServices.UnimplementedMaster2ClientServicesServer
}

func (s *Master2ClientServer) SelectMachineToCopyTo(ctx context.Context, req *MasterClientServices.SelectMachineRequest) (*MasterClientServices.SelectMachineResponse, error) {
	log.Printf("Received Select Machine Request")

	machineKey, FileTransferPort := KeeperNodes.GetSuitableMachine(req.Filename, int(req.Size))
	ip, _ := KeeperNodes.ResolveAddress(machineKey)

	if machineKey == Utils.FILE_EXISTS ||
		machineKey == Utils.NO_AVAILABLE_DATA_KEEPER {
		return nil, errors.New(machineKey)
	} else if FileTransferPort == -1 {
		return nil, errors.New("invalid port")
	} else {
		KeeperNodes.RegisterFileTransferStart(machineKey, req.Filename)
		return &MasterClientServices.SelectMachineResponse{Ip: ip, Port: FileTransferPort}, nil
	}
}

func (s *Master2ClientServer) GetSourceMachine(ctx context.Context, req *MasterClientServices.GetSourceMachineRequest) (*MasterClientServices.GetSourceMachineResponse, error) {
	ip, port := KeeperNodes.GetSourceMachine(req.Filename)

	if ip == "" || port == -1 {
		return nil, errors.New("file does not exist")
	} else {
		println("Found file " + req.Filename + " on " + ip + ":" + strconv.Itoa(int(port)))
		return &MasterClientServices.GetSourceMachineResponse{
			Ip:   ip,
			Port: port,
		}, nil
	}
}
