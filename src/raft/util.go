package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

type RaftServerState int

const (
	follower RaftServerState = iota
	candidate
	leader
)

func getElectionSleepDuration() time.Duration {
	return time.Duration(200) * time.Millisecond
	// r := rand.New(rand.NewSource(666))
	// time.Duration(r.Intn(1)) * time.Microsecond * 100
}

func getHeartbeatSleepDuration() time.Duration {
	return time.Duration(200) * time.Millisecond
	// this should be just a constant period
}

func binarySearchFindFirst(log []*LogEntry, end int, targetTerm int) int {
	start := 1
	for start < end-1 {
		mid := start + (end-start)/2
		if log[mid].TermReceived == targetTerm {
			end = mid
		} else {
			start = mid + 1
		}
	}
	if log[start].TermReceived == targetTerm {
		return start
	}
	return start + 1
}
