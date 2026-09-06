# Real-time NL OV Streaming Pipeline

Streaming pipeline that consumes real-time Dutch public transport vehicle positions and
arrival predictions (OVapi GTFS-realtime) through Kafka (Redpanda), computes rolling
delay aggregates per line and station in 5-minute tumbling windows, and serves them in a
live dashboard with a map of current delays.

🚧 Work in progress — see [PRD.md](PRD.md) for goals, architecture and Definition of Done.
