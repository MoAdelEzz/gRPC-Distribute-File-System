gen:
	@protoc \
	  --proto_path=protobuf "protobuf/master-tracker.proto" \
	  --go_out=services/master-tracker \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=services/master-tracker \
	  --go-grpc_opt=paths=source_relative


run:
	start cmd /k "go run master-tracker/main.go" 
	start cmd /k "go run data-keeper/main.go datanode-1"
	# start cmd /k "go run data-keeper/main.go datanode-2"
	# start cmd /k "go run data-keeper/main.go datanode-3"
