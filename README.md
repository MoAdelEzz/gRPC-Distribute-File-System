#   Distributed File System (DFS)

This project implements a distributed file system with a master-datanode architecture, providing file storage, retrieval, and replication for data availability and fault tolerance.

##   Architecture

The system uses a master-datanode architecture with the following components:

* **Master:** Manages file system metadata and coordinates datanode activities.
* **Datanodes:** Store file data and handle replication.
* **Client:** Interacts with the master to perform file operations.

Communication between these components is handled by gRPC.

##   Functionality

###   File Operations

* **File Upload:**
    * Client requests a datanode address from the master.
    * Client sends the file data to the datanode.
    * Datanode informs the master upon completion.
* **File Download:**
    * Client requests the file location from the master.
    * Client retrieves the file data from the datanode.
* **File Replication:**
    * The master periodically checks for files that need replication.
    * The master instructs datanodes to replicate files.
    * Datanodes replicate the file data.

###   Fault Tolerance

* File replication ensures data redundancy and availability.
* The master monitors datanode health using heartbeat messages.
* The master removes offline datanodes from its active list.

##   Code Structure

The project is organized as follows:

* **`protobuf/`**:  Contains Protobuf definitions for gRPC services.
    * `controller.proto`: Defines messages and services for master-datanode communication, including `RegisterFile` and `HeartBeat` RPCs. 
    * `datanode.proto`: Defines messages and services for datanode operations, such as `GetFileTransferState` and `ReplicateTo` RPCs. 
    * `master.proto`: Defines messages and services for master-client communication, including `SelectMachineToCopyTo` and `GetSourceMachine` RPCs.
* **`master/`**: Contains the master node implementation.
    * `main.go`:  Main application for the master node, handling gRPC servers, datanode management, and file replication. 
    * `master-services.go`: Implements gRPC services for client communication. 
    * `utils.go`: Provides utility functions for master node operations, including datanode management, file metadata handling, and file replication logic. 
    * `controller-services.go`: Implements gRPC services for datanode communication, handling `RegisterFile` and `HeartBeat` requests. 
* `datanode/`: Contains the datanode implementation.
    * `main.go`: Main application for the datanode, handling gRPC services, file transfer, and communication with the master. 
    * `utils.go`: Provides utility functions for datanode operations, including file I/O and file transfer handling.
    * `datanode-services.go`: Implements gRPC services for communication with other datanodes and the master, including `GetFileTransferState` and `ReplicateTo` RPCs. 
* `client/`: Contains the client application.
    * `main.go`:  Main application for the client, providing functionality to upload and download files. 
* `utils.go`:  Provides common utility functions for file I/O, network operations, and data manipulation used across the project. 

##   Getting Started

###   Prerequisites

* Go, gRPC, and Protobuf compiler installed.

###   Installation

1.  Clone the repository.
2.  Install dependencies: `go mod tidy`.
3.  Compile Protobuf files.

###   Configuration

* Environment variables in `.env` files (master/, datanode/, client/).

###   Running the System

1.  Start the master node.
2.  Start the datanode(s).
3.  Run the client to upload/download files.

##   Contributing

Contributions are welcome.
