package main

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"

   "github.com/spikebike/backups-done-right/src/examples/client-server-tls-proto/sum"
	"google.golang.org/protobuf/proto"
)

func main() {
	cert, err := tls.LoadX509KeyPair("certs/server.pem", "certs/server.key")
	if err != nil {
		log.Fatalf("server: loadkeys: %s", err)
	}
	config := tls.Config{Certificates: []tls.Certificate{cert}}

	listener, err := tls.Listen("tcp", ":4040", &config)
	if err != nil {
		fmt.Printf("Failed to open port: %v", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v", err)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			os.Exit(-1)
		}
	}(conn)

	var buf [4]byte

	_, err := conn.Read(buf[0:])
	if err != nil {
		return
	}

	length := binary.LittleEndian.Uint32(buf[0:])
	data := make([]byte, length)

	_, err = io.ReadFull(conn, data)
	if err != nil {
		return
	}

	nums := &sum.Numbers{}
	if err := proto.Unmarshal(data, nums); err != nil {
		fmt.Printf("Failed to parse message: %v", err)
	}

	result := &sum.Sum{
		Result: nums.A + nums.B,
	}

	out, err := proto.Marshal(result)
	if err != nil {
		fmt.Printf("Failed to encode message: %v", err)
	}

	binary.LittleEndian.PutUint32(buf[0:], uint32(len(out)))
	conn.Write(buf[0:])
	conn.Write(out)
}
