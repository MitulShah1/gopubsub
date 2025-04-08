.PHONY: test
test:
	go test -v ./... -count 1

.PHONY: proto
proto:
	@echo "Generating protobuf code..."
	protoc --go_out=. --go_opt=paths=source_relative \
		internal/util/serializer/message.proto

.PHONY: install-tools
install-tools:
	@echo "Installing protoc-gen-go..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

.PHONY: clean
clean:
	rm -f internal/util/serializer/message.pb.go