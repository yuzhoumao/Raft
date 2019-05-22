# The Raft Consensus Algorithm

> Raft is a distributed consensus algorithm that is designed to be easy to understand. 
> It's equivalent to Paxos in fault-tolerance and performance. 
> The difference is that it's decomposed into relatively independent subproblems, 
> and it cleanly addresses all major pieces needed for practical systems.

Original Raft paper (extended version): https://raft.github.io/raft.pdf

This repository implements:

  - Raft, a replicated state machine protocol
  
  - A fault-tolerant key/value service on top of Raft
  
  - Shard service over multiple replicated state machines for better performance.
  
## Raft, a replicated state machine protocol

Raft manages a service's state replicas, 
and in particular it helps the service sort out what the correct state is after failures. 

Raft implements a replicated state machine. It organizes client requests into a sequence, called the log, 
and ensures that all the replicas agree on the contents of the log. 

Each replica executes the client requests in the log in the order they appear in the log, 
applying those requests to the replica's local copy of the service's state. 
Since all the live replicas see the same log contents, 
they all execute the same requests in the same order, 
and thus continue to have identical service state. 

If a server fails but later recovers, Raft takes care of bringing its log up to date. 
Raft will continue to operate as long as at least a majority of the servers are alive and can talk to each other. 
If there is no such majority, Raft will make no progress, 
but will pick up where it left off as soon as a majority can communicate again.

Raft is implemented as a [Go](https://golang.org/) object type with associated methods, 
meant to be used as a module in a larger service. 
A set of Raft instances talk to each other with RPC to maintain replicated logs. 

The Raft interface supports an indefinite sequence of numbered commands, also called log entries. 
The entries are numbered with index numbers. 
The log entry with a given index will eventually be committed. 
At that point, Raft should send the log entry to the larger service for it to execute.

## A fault-tolerant key/value service on top of Raft

## Shard service over multiple replicated state machines
