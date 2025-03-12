gen:
	if not exist "services" mkdir "services"
	@protoc \
	  --proto_path=protobuf "protobuf/controller.proto" \
	  --go_out=services \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=services \
	  --go-grpc_opt=paths=source_relative

	@protoc \
	  --proto_path=protobuf "protobuf/datanode.proto" \
	  --go_out=services \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=services \
	  --go-grpc_opt=paths=source_relative

	@protoc \
	  --proto_path=protobuf "protobuf/master.proto" \
	  --go_out=services \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=services \
	  --go-grpc_opt=paths=source_relative

run-master:
	cd master && go build -o ../out/master.exe
	./out/master.exe

run-datanode:
	cd datanode && go build -o ../out/datanode.exe
	./out/datanode.exe

ARGS ?=
run-client:
	cd client && go build -o ../out/client.exe
	./out/client.exe ${ARGS}
