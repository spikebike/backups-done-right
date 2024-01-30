package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"github.com/spikebike/backups-done-right/src/examples/client-server-tls-proto/sum"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
)

func main() {

	cert, err := tls.LoadX509KeyPair("certs/client.pem", "certs/client.key")
	if err != nil {
		log.Fatalf("server: loadkeys: %s", err)
	}

	config := tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
	conn, err := tls.Dial("tcp", "localhost:4040", &config)
	if err != nil {
		fmt.Printf("client: dial: %s", err)
	}

	state := conn.ConnectionState()
	for _, v := range state.PeerCertificates {
		fmt.Println("Client: Server public key is:")
		fmt.Println(x509.MarshalPKIXPublicKey(v.PublicKey))
	}

	defer conn.Close()

	numbers := &sum.Numbers{
		A: 3,
		B: 5,
	}

	out, err := proto.Marshal(numbers)
	if err != nil {
		fmt.Printf("Failed to encode message: %v", err)
	}

	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(out)))
	conn.Write(buf[0:])
	conn.Write(out)

	_, err = conn.Read(buf[0:])
	if err != nil {
		fmt.Printf("Failed to read response: %v", err)
	}

	length := binary.LittleEndian.Uint32(buf[0:])
	data := make([]byte, length)

	_, err = io.ReadFull(conn, data)
	if err != nil {
		fmt.Printf("Failed to read response: %v", err)
	}

	response := &sum.Sum{}
	if err := proto.Unmarshal(data, response); err != nil {
		fmt.Printf("Failed to parse response: %v", err)
	}

	fmt.Printf("The sum is: %d\n", response.Result)
}
