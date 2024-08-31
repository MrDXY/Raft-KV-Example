# Raft-KV-Example: A Storage System Based on ETCD Raft

---

## Introduction

This project is a simplified implementation of a key-value (KV) storage system built on top of the ETCD Raft consensus algorithm. It serves as an educational tool to understand the interaction between the Raft consensus model and the upper-layer KV storage system.

---

## Architecture Overview

The system architecture is divided into several key components, as illustrated in the provided diagram:
![Architecture](/doc/images/architecture.png)
1. **KV Transport**:
    - The KV transport layer handles communication between clients and the Raft model. It is responsible for proposing new entries (key-value pairs) and managing configuration changes in the cluster.

2. **Raft Model**:
    - **Unstable Storage**: This is where entries and snapshots are temporarily stored during the Raft process. Entries are commands proposed by clients, and snapshots are compacted state representations.
    - **Ready**: This component is responsible for processing entries and snapshots, preparing them for commit or communication with peers.
    - **Memory Storage**: This represents the in-memory state of the persisted entries and snapshots. It mirrors the data persisted in the underlying file system, ensuring consistency between the in-memory state and the disk.
    - **Snapshot**: Snapshots capture the state of the Raft state machine at a specific point in time. Along with the Write-Ahead Log (WAL), snapshots allow the system to recover the Raft state machine's state after a failure.

3. **Commit Process**:
    - Once the entries are committed, they are sent to the backend for apply.

4. **Peer Transport**:
    - This layer handles communication between different nodes in the Raft cluster, ensuring consistency and coordination across all participating nodes.

5. **Backend Storage**:
    - **WAL (Write-Ahead Log) File**: The WAL file persists all entries sequentially to ensure data integrity in case of a crash.
    - **Snapshot File**: The snapshot file stores compacted data from the memory storage, providing a way to recover the state of the system without replaying all WAL entries.
    - **Database (.db)**: Optionally, a database (such as BoltDB in ETCD's implementation) may be used for additional disk-based storage. This provides persistent storage for committed key-value pairs and can be used in conjunction with WAL and snapshots.

---

## How It Works

1. **Proposing New Entries**:
    - When a client proposes a new entry (a key-value pair or a config change), the KV transport layer passes it to the Raft model.
    - The entry is first stored in the unstable storage.

2. **Processing Entries**:
    - The Raft model prepares entries and snapshots in the "Ready" component. These are then either committed locally or sent to other peers for consensus.

3. **Commit and Persistence**:
    - Once consensus is reached, the entry is committed and persisted to the backend storage. This includes writing to the WAL file and potentially creating a new snapshot.

4. **Recovery**:
    - In case of a restart, the system loads the WAL and snapshot files to restore the last consistent state of the KV storage.

5. **Memory Store**:
    - The memory store maintains the current state of the system in RAM, mirroring what is stored on disk. This ensures that the system can efficiently access and manipulate the state.

---

## Running the System

To run the server, the following steps will guide you through the setup and execution:


### Three Nodes Raft Cluster

To start all three server nodes simultaneously, use:
```shell
make run-all
```

This will start:
- Node 1 with `PeerPort=2380` and `ClientPort=2379`
- Node 2 with `PeerPort=12380` and `ClientPort=12379`
- Node 3 with `PeerPort=22380` and `ClientPort=22379`

#### Stop a Specific Node in Cluster

To stop a specific node, for instance, Node 2, use:
```shell
make stop-node2
```

#### Restart a Specific Node in Cluster
To restart a specific node, for instance, Node 2, use:
```shell
make start-node2
```

### Single Node Cluster

To start a specific node, for example, Node 1, use:
```shell
make run-single
```

### Send KV Requests

#### Send a `PUT` Request to a Node
```shell
make put-node PORT=2379 DATA="key1=val1"
```

#### Send a `GET` Request to a Node
```shell
make get-node PORT=2379 DATA="key1"
```

---

## Conclusion

This project provides a hands-on experience with the Raft consensus algorithm and its integration into a KV storage system. By studying this implementation, you will gain a deeper understanding of distributed systems, consensus algorithms, and the challenges of building reliable storage systems.

Feel free to explore the code, modify it, and experiment with different configurations to enhance your understanding of the underlying concepts. Happy learning!
