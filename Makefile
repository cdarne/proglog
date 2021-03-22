CONFIG_PATH=${HOME}/.proglog

${CONFIG_PATH}:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert: ${CONFIG_PATH}
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: compile
compile:
	protoc api/v1/*.proto \
	--go_out=. --go_opt=paths=source_relative \
	--go-grpc_out=. --go-grpc_opt=paths=source_relative \
	--proto_path=.

${CONFIG_PATH}/model.conf: ${CONFIG_PATH}
	cp test/model.conf ${CONFIG_PATH}/

${CONFIG_PATH}/policy.csv: ${CONFIG_PATH}
	cp test/policy.csv ${CONFIG_PATH}/

.PHONY: test
test: ${CONFIG_PATH}/model.conf ${CONFIG_PATH}/policy.csv gencert
	go test -race ./...