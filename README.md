# OTT — Distributed Multimedia Streaming Network

An application-layer overlay network developed in Python for distributed multimedia streaming. The system uses intermediate nodes to forward video streams between servers and clients, aiming to improve scalability and provide alternative routes when network nodes become unavailable.

## Architecture

The network uses two communication planes:

- **Control Plane (TCP):** Handles node registration, topology discovery, route selection and network monitoring.
- **Data Plane (UDP):** Handles video delivery using RTP-style packet structures.

## Key Features

- Dynamic topology discovery through message flooding.
- Route selection based on network metrics, including latency, packet loss and available bandwidth.
- Multiple candidate routes to support fault recovery.
- Video streaming using UDP and RTP packet structures.
- Node monitoring through periodic heartbeat messages.
- Automatic route switching when a node or connection fails.

## Technologies

- **Language:** Python
- **Networking:** TCP, UDP, sockets
- **Protocols:** RTP
- **Concepts:** Overlay networks, distributed routing, fault tolerance and inter-process communication.

## Project Context

Developed as a group project for the Computer Networks course at the University of Minho.
