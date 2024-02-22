# Distributed KV Store

- Distributed Key-Value Storage Service
    - KV service is implemented on top of the Raft consensus algorithm.
    - Raft is used for log replication by categorizing the replicas into leader, followers, and candidates.
    - The underlying library follows a replicated state machine approach by holding elections and keeping the log in sync across all servers. It can also handle server crashes, network partitions, and disconnects.
    - Additionally, Raft takes periodic snapshots of the log in order to make it more compact and not have to send unnecessarily large chunks of data over the network.
