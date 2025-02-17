# Sportify

Ever since I was a kid, basketball has been more than just a game to me. I grew up dreaming about scoring buzzer-beaters and running into the crowd. Now, as an adult with a newfound curiousity for distributed systems, I decided to merge these two interest into a single project: a real‑time NBA score subscription system.

This project is a CLI‑based application written in Go that leverages gRPC for streaming live updates and uses Prometheus (with Grafana for visualization) to monitor performance and reliability. It pulls data from the [balldontlie API](https://www.balldontlie.io/) so you can keep up with your favourite teams and players in real-time.

## Features

- **Real-Time Updates:** You can subscribe to live score updates and player stats for your favourite teams or players.
- **gRPC Streaming:** Efficiently stream updates directly to you.
- **Distributed System Design:** Leverages Go’s concurrency and modern system design practices to ensure scalability and resilience.
- **Monitoring & Metrics:** Integrated with Prometheus and Grafana to visualize key performance metrics like active subscriptions, request latencies, and error rates.

## Architecture Overview

The system is designed with simplicity and performance in mind:
- **Data Fetcher:** Periodically polls the external NBA API (every minute) to retrieve the latest game and player data.
- **Subscription Manager:** Maintains a registry of active client subscriptions, filtering and releasing updates based on the client's request (by team or player).
- **gRPC Server:** Streams real‑time updates to clients using gRPC.



