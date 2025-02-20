package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	pb "Sportify/proto" // adjust the import path as needed
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	grpcPort     = ":50051"
	metricsPort  = ":2112"
	pollInterval = 1 * time.Minute
)

var (
	// Prometheus metrics
	balldontlieRequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "balldontlie_request_duration_seconds",
		Help: "Duration of requests to the balldontlie API",
	})
	grpcMessagesSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grpc_messages_sent_total",
		Help: "Total number of gRPC messages sent",
	})
)

func init() {
	prometheus.MustRegister(balldontlieRequestDuration)
	prometheus.MustRegister(grpcMessagesSent)
}

// server implements the SubscriptionService.
type server struct {
	pb.UnimplementedSubscriptionServiceServer
}

// Subscribe implements the gRPC streaming endpoint.
func (s *server) Subscribe(req *pb.SubscriptionRequest, stream pb.SubscriptionService_SubscribeServer) error {
	subType := req.GetType()
	name := req.GetName()
	ctx := stream.Context()

	log.Printf("New subscription: type=%v, name=%s", subType, name)

	switch subType {
	case pb.SubscriptionType_PLAYER:
		// Resolve the player ID (using the search API)
		playerID, playerName, err := fetchPlayerIDAndName(name)
		if err != nil {
			return err
		}
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		// Immediately poll, then at each interval.
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				start := time.Now()
				stats, err := fetchPlayerStats(playerID, playerName)
				duration := time.Since(start).Seconds()
				balldontlieRequestDuration.Observe(duration)
				if err != nil {
					log.Printf("Error fetching player stats: %v", err)
				} else {
					resp := &pb.SubscriptionResponse{
						UpdatedAt: timestamppb.Now(),
						Update: &pb.SubscriptionResponse_PlayerStats{
							PlayerStats: stats,
						},
					}
					if err := stream.Send(resp); err != nil {
						return err
					}
					grpcMessagesSent.Inc()
				}
				select {
				case <-ticker.C:
					continue
				case <-ctx.Done():
					return nil
				}
			}
		}
	case pb.SubscriptionType_TEAM:
		// Resolve the team ID.
		teamID, teamName, err := fetchTeamIDAndName(name)
		if err != nil {
			return err
		}
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				start := time.Now()
				teamScore, err := fetchTeamScore(teamID, teamName)
				duration := time.Since(start).Seconds()
				balldontlieRequestDuration.Observe(duration)
				if err != nil {
					log.Printf("Error fetching team score: %v", err)
				} else {
					resp := &pb.SubscriptionResponse{
						UpdatedAt: timestamppb.Now(),
						Update: &pb.SubscriptionResponse_TeamScore{
							TeamScore: teamScore,
						},
					}
					if err := stream.Send(resp); err != nil {
						return err
					}
					grpcMessagesSent.Inc()
				}
				select {
				case <-ticker.C:
					continue
				case <-ctx.Done():
					return nil
				}
			}
		}
	default:
		return fmt.Errorf("unsupported subscription type")
	}
}

// fetchPlayerIDAndName searches for a player by name using the balldontlie API.
func fetchPlayerIDAndName(name string) (int, string, error) {
	url := fmt.Sprintf("https://www.balldontlie.io/api/v1/players?search=%s", name)
	resp, err := http.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}
	var result struct {
		Data []struct {
			ID        int    `json:"id"`
			FirstName string `json:"first_name"`
			LastName  string `json:"last_name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, "", err
	}
	if len(result.Data) == 0 {
		return 0, "", fmt.Errorf("player not found")
	}
	player := result.Data[0]
	fullName := fmt.Sprintf("%s %s", player.FirstName, player.LastName)
	return player.ID, fullName, nil
}

// fetchPlayerStats retrieves the latest game stats for a given player.
func fetchPlayerStats(playerID int, playerName string) (*pb.PlayerStats, error) {
	url := fmt.Sprintf("https://www.balldontlie.io/api/v1/stats?player_ids[]=%d&per_page=1", playerID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data []struct {
			Points   int `json:"pts"`
			Rebounds int `json:"reb"`
			Assists  int `json:"ast"`
			Game     struct {
				Date string `json:"date"`
			} `json:"game"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, fmt.Errorf("no stats available for player")
	}
	stat := result.Data[0]
	gameTime, err := time.Parse(time.RFC3339, stat.Game.Date)
	if err != nil {
		gameTime = time.Now()
	}
	return &pb.PlayerStats{
		PlayerId:   int32(playerID),
		PlayerName: playerName,
		Points:     int32(stat.Points),
		Rebounds:   int32(stat.Rebounds),
		Assists:    int32(stat.Assists),
		GameDate:   timestamppb.New(gameTime),
	}, nil
}

// fetchTeamIDAndName finds a team by name from the API.
func fetchTeamIDAndName(name string) (int, string, error) {
	resp, err := http.Get("https://www.balldontlie.io/api/v1/teams")
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}
	var result struct {
		Data []struct {
			ID       int    `json:"id"`
			FullName string `json:"full_name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, "", err
	}
	// Match the team name (case-insensitive search)
	for _, team := range result.Data {
		if strings.Contains(strings.ToLower(team.FullName), strings.ToLower(name)) {
			return team.ID, team.FullName, nil
		}
	}
	return 0, "", fmt.Errorf("team not found")
}

// fetchTeamScore retrieves the latest game score for a given team.
func fetchTeamScore(teamID int, teamName string) (*pb.TeamScore, error) {
	url := fmt.Sprintf("https://www.balldontlie.io/api/v1/games?team_ids[]=%d&per_page=1", teamID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data []struct {
			HomeTeam struct {
				ID int `json:"id"`
			} `json:"home_team"`
			VisitorTeam struct {
				ID int `json:"id"`
			} `json:"visitor_team"`
			HomeTeamScore    int    `json:"home_team_score"`
			VisitorTeamScore int    `json:"visitor_team_score"`
			Status           string `json:"status"`
			Date             string `json:"date"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, fmt.Errorf("no games available for team")
	}
	game := result.Data[0]
	gameTime, err := time.Parse(time.RFC3339, game.Date)
	if err != nil {
		gameTime = time.Now()
	}
	// Simulate live status: if status is not "Final", treat as live.
	isLive := strings.ToLower(game.Status) != "final"
	return &pb.TeamScore{
		TeamId:       int32(teamID),
		TeamName:     teamName,
		HomeScore:    int32(game.HomeTeamScore),
		VisitorScore: int32(game.VisitorTeamScore),
		IsLive:       isLive,
		GameDate:     timestamppb.New(gameTime),
	}, nil
}

func main() {
	flag.Parse()

	// Start Prometheus metrics HTTP server.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Starting metrics server on %s", metricsPort)
		if err := http.ListenAndServe(metricsPort, nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	// Start the gRPC server.
	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterSubscriptionServiceServer(s, &server{})
	log.Printf("gRPC server listening on %s", grpcPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
