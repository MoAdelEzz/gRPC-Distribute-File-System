package gRPC_Distribute_File_System

import (
	"errors"
	"log"

	KeeperNodes "github.com/MoAdelEzz/gRPC-Distribute-File-System/common/master-tracker"
	common "github.com/MoAdelEzz/gRPC-Distribute-File-System/common"
	M2CS "github.com/MoAdelEzz/gRPC-Distribute-File-System/services/master-tracker/client"

	"golang.org/x/net/context"
)


type Master2ClientServer struct {
	M2CS.UnimplementedMaster2ClientServicesServer
}

func (s *Master2ClientServer) SelectMachineToCopyTo(ctx context.Context, req *M2CS.SelectMachineRequest) (*M2CS.SelectMachineResponse, error) {
	log.Printf("Received Select Machine Request");

	machineKey := KeeperNodes.GetSuitableMachine(req.Filename, int(req.Size))
	ip, port := KeeperNodes.ResolveAddress(machineKey)

	if machineKey == common.FILE_EXISTS ||
		machineKey == common.NO_AVAILABLE_DATA_KEEPER {
		return nil, errors.New(machineKey)
	} else if port == -1 {
		return nil, errors.New("invalid port")
	} else {
		KeeperNodes.RegisterFileTransferStart(machineKey, req.Filename);
		return &M2CS.SelectMachineResponse{Ip: ip, Port: int32(port)}, nil
	}
}

func (s *Master2ClientServer) DownloadFile(ctx context.Context, req *M2CS.GetSourceMachineRequest) (*M2CS.GetSourceMachineResponse, error) {
	log.Printf("Received: %v", req)
	return &M2CS.GetSourceMachineResponse{Port: 8080}, nil
}
