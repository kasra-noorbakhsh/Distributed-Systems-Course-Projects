# 🌐 Distributed Systems Course Projects

This repository contains implementations for three projects from the Distributed Systems course, focusing on key concepts like distributed processing, reliable storage, and consensus protocols. The projects are implemented in Go, emphasizing fault tolerance, coordination, and replication.

---

## 📌 Project Overview

- **Project 1**: Implements a distributed MapReduce system with a Coordinator process managing tasks and multiple Worker processes executing Map and Reduce functions. Workers communicate via RPC, read inputs from files, write outputs to files, and handle failures by reassigning tasks after a 10-second timeout.
- **Project 2**: Builds a reliable Key/Value (KV) server on a single machine, ensuring operations like Put and Get are linearizable despite network failures (e.g., packet loss, latency). Uses unique IDs for idempotency, internal locks/mutexes for concurrency, and handles retries without duplicating effects.
- **Project 3**: Implements the Raft consensus algorithm as a Go object for replicated state machines. Manages logs for consistency across servers, supports leader election, log replication via RPC, and fault tolerance (crash recovery). Design focuses on the extended Raft version from the paper, ensuring commands are applied in order.

---

## 🧰 Tools Used

- [Go](https://golang.org/) for implementation, concurrency (goroutines), and networking (RPC)
- Standard Go libraries: `net/rpc` for communication, `sync` for synchronization
- Testing frameworks for simulating multi-server environments and failures

---

## 📬 Contact

Made by **Kasra Noorbakhsh**  
📧 Feel free to connect or provide feedback!
