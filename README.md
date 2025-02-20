# Sportify

Ever since I was a kid, basketball has been more than just a game to me. I grew up dreaming about scoring buzzer-beaters and running into the crowd. Now, after I got really into concurrency and distributed systems after I class I took in school, I decided to merge these two interests into a single project: a real‑time NBA score subscription system.

This project is a CLI‑based application written in Go that leverages gRPC for streaming live updates and uses Prometheus (with Grafana for visualization) to monitor performance and reliability. It pulls data from the [balldontlie API](https://www.balldontlie.io/) so you can keep up with your favourite teams and players in real-time.

## Features

- **Real-Time Updates:** You can subscribe to live score updates and player stats for your favourite teams or players.
- **gRPC Streaming:** Efficiently stream updates directly to you.
- **Distributed System Design:** Leverages Go’s concurrency and modern system design practices to ensure scalability and resilience.
- **Monitoring & Metrics:** Integrated with Prometheus and Grafana to visualize key performance metrics like active subscriptions, request latencies, and error rates.



