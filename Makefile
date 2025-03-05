gen:
	if not exist "services/master-tracker/client" mkdir "services/master-tracker/client"
	if not exist "services/master-tracker/datakeeper" mkdir "services/master-tracker/datakeeper"
	if not exist "services/master-tracker/datakeeper" mkdir "services/datakeeper"
	@protoc \
	  --proto_path=protobuf "protobuf/master-tracker/client/client.proto" \
	  --go_out=services \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=services \
	  --go-grpc_opt=paths=source_relative

	@protoc \
	  --proto_path=protobuf "protobuf/master-tracker/datakeeper/datakeeper.proto" \
	  --go_out=services \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=services \
	  --go-grpc_opt=paths=source_relative

	@protoc \
	  --proto_path=protobuf "protobuf/datakeeper/datakeeper.proto" \
	  --go_out=services \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=services \
	  --go-grpc_opt=paths=source_relative


run:
	start cmd /k "go run master-tracker/main.go" 
	start cmd /k "go run data-keeper/main.go datanode-1"
	# start cmd /k "go run data-keeper/main.go datanode-2"
	# start cmd /k "go run data-keeper/main.go datanode-3"
