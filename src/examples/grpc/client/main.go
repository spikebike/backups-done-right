package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"echo"
)

type server struct{
	echo.UnimplementedEchoServiceServer
}

func (s *server) Echo(ctx context.Context, in *echo.EchoMessage) (*echo.EchoMessage, error) {
	return in, nil
}

func main() {
	lis, err := net.Listen("tcp", ":4040")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	echo.RegisterEchoServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
