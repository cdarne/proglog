# Proglog

Based on a the book [Distributed Services with Go](https://pragprog.com/titles/tjgo/distributed-services-with-go/), this repo implements a server and a client for a self-contained, persistent event-stream service. This project uses a simple Log as the storage, grpc as the communication layer, serf to discover servers in the cluster and raft for consensus and replication.

This project uses TLS for security, so you'll need to create your own certificates. I'm using `cfssl` to generate them locally:

```
$ go get -u github.com/cloudflare/cfssl/cmd/cfssl
$ go get -u github.com/cloudflare/cfssl/cmd/cfssljson
```

Then you can create your own certificates using the Makefile command:
```
$ make gencert
```

## Tests

```
$ make test
```