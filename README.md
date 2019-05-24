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
The key-value service is structured as a replicated state machine with several key-value servers that coordinate their activities through the Raft log. The key/value service should continue to process client requests as long as a majority of the servers are alive and can communicate, in spite of other failures or network partitions.

The service supports three operations: 
  - Put(key, value)
  - Append(key, arg) 
  - Get(key). 

It maintains a simple database of key/value pairs. 
  - Put(key, value) replaces the value for a particular key in the database
  - Append(key, arg) appends arg to key's value (an Append to a non-existant key should act like Put)
  - Get() fetches the current value for a key

Clients send Put(), Append(), and Get() RPCs to key/value servers (called kvraft servers), who then place those calls into the Raft log and execute them in order. A client can send an RPC to any of the kvraft servers, but if that server is not currently a Raft leader, or if there's a failure, the client should retry by sending to a different server. If the operation is committed to the Raft log (and hence applied to the key/value state machine), its result is reported to the client. If the operation failed to commit (for example, if the leader was replaced), the server reports an error, and the client retries with a different server.

The service provides strong consistency to applications calls to the Clerk Get/Put/Append methods. Here's what we mean by strong consistency: 
> If called one at a time, the Get/Put/Append methods should act as if the system had only one copy of its state, and each call should observe the modifications to the state implied by the preceding sequence of calls. 

> For concurrent calls, the return values and final state must be the same as if the operations had executed one at a time in some order. Calls are concurrent if they overlap in time, for example if client X calls Clerk.Put(), then client Y calls Clerk.Append(), and then client X's call returns. 

> Furthermore, a call must observe the effects of all calls that have completed before the call starts (so we are technically asking for linearizability).

Strong consistency is convenient for applications because it means that, informally, all clients see the same state and they all see the latest state. Providing strong consistency is relatively easy for a single server. It is harder if the service is replicated, since all servers must choose the same execution order for concurrent requests, and must avoid replying to clients using state that isn't up to date. 

## Shard service over multiple replicated state machines
