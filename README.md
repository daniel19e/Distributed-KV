# Distributed Systems Projects

- Distributed MapReduce
    - An RPC connection is established between the workers and coordinator
    - Fault-tolerant design that can recover from crashed workers
    - It can be used with any custom map/reduce functions such as word-count

- Distributed Key-Value Storage Service
    - Large project implemented on top of the Raft consensus algorithm
    - Raft is used for log replication by categorizing the replicas into leader, followers, and candidates
    - Replicated state machine implementation