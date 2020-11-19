package main

import (
	"context"
	"log"
	"net"
	"fmt"

	"google.golang.org/grpc"
    "/proto"
	"github.com/confluentinc/confluent-kafka-go/kafka"

)

const (
	port = ":50051"
)


type server struct {
	proto.Server
}

func (s *server) data(ctx context.Context, in *proto.userRequest) (*proto.serveReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &proto.serveReply{Message: "Hello " + in.GetName()}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}


	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()




	topic := "myTopic"
	for _, word := range {s.Serve(lis)} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	p.Flush(15 * 1000)
}
