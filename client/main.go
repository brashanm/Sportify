package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	pb "Sportify/proto" // adjust the import path as needed
)

const grpcAddress = "localhost:50051"

func main() {
	flag.Parse()

	// Connect to the gRPC server.
	conn, err := grpc.Dial(grpcAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewSubscriptionServiceClient(conn)

	// Interactive CLI prompt.
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Welcome to the NBA Subscription CLI")
	fmt.Println("Select subscription type:")
	fmt.Println("1. Player")
	fmt.Println("2. Team")
	fmt.Print("Enter choice (1 or 2): ")
	choiceInput, _ := reader.ReadString('\n')
	choiceInput = strings.TrimSpace(choiceInput)

	var subType pb.SubscriptionType
	switch choiceInput {
	case "1":
		subType = pb.SubscriptionType_PLAYER
	case "2":
		subType = pb.SubscriptionType_TEAM
	default:
		log.Fatalf("Invalid choice")
	}
	fmt.Print("Enter name (e.g., 'Stephen Curry' or 'Toronto Raptors'): ")
	nameInput, _ := reader.ReadString('\n')
	nameInput = strings.TrimSpace(nameInput)

	// Create the subscription request.
	req := &pb.SubscriptionRequest{
		Type: subType,
		Name: nameInput,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Subscribe(ctx, req)
	if err != nil {
		log.Fatalf("Error creating subscription: %v", err)
	}

	log.Println("Subscribed successfully. Waiting for updates... (Press Ctrl+C to exit)")
	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalf("Error receiving update: %v", err)
		}
		updatedAt := resp.GetUpdatedAt().AsTime().Format(time.RFC3339)
		switch update := resp.Update.(type) {
		case *pb.SubscriptionResponse_PlayerStats:
			ps := update.PlayerStats
			fmt.Printf("[%s] Player Stats - %s: Points: %d, Rebounds: %d, Assists: %d, Game Date: %s\n",
				updatedAt, ps.PlayerName, ps.Points, ps.Rebounds, ps.Assists, ps.GameDate.AsTime().Format(time.RFC3339))
		case *pb.SubscriptionResponse_TeamScore:
			ts := update.TeamScore
			liveStatus := "Final"
			if ts.IsLive {
				liveStatus = "Live"
			}
			fmt.Printf("[%s] Team Score - %s: Home: %d, Visitor: %d, Status: %s, Game Date: %s\n",
				updatedAt, ts.TeamName, ts.HomeScore, ts.VisitorScore, liveStatus, ts.GameDate.AsTime().Format(time.RFC3339))
		default:
			fmt.Printf("[%s] Unknown update received\n", updatedAt)
		}
	}
}
