package raft

import (
	"log"
	"math"
)

type Entries []LogEntry

type LogEntry struct {
	Term    int
	Command interface{}
}

func minInt(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func maxInt(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

// indexing and terms start from 1
func (rf *Raft) getLogEntryAt(index int) *LogEntry {
	index = index - rf.lastIncludedIndex
	if index < 0 {
		log.Panic("index < 0.\n")
	}
	if index == 0 {
		return &LogEntry{
			Command: nil,
			Term:    rf.lastIncludedTerm,
		}
	}
	if index > len(rf.log) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &rf.log[index-1]
}
func (rf *Raft) lastIndex() int {
	return len(rf.log) + rf.lastIncludedIndex
}
func (rf *Raft) lastTerm() int {
	return rf.getLogEntryAt(len(rf.log) + rf.lastIncludedIndex).Term
}

func (rf *Raft) getTrimmedLogSlice(start int, end int) Entries {
	start = start - rf.lastIncludedIndex
	end = end - rf.lastIncludedIndex
	if start <= 0 {
		log.Panic("slice: start out of range. \n")
	}
	if end > len(rf.log)+1 {
		log.Panic("slice: end out of range.\n")
	}
	if start > end {
		log.Panic("slice: start > end.\n")
	}
	return append(Entries(nil), rf.log[start-1:end-1]...)
}

func (rf *Raft) findTermRange(term int) (int, int) {
	if term == 0 {
		return 0, 0
	} else {
		start, end := math.MaxInt, -1
		for i := rf.lastIncludedIndex + 1; i <= len(rf.log)+rf.lastIncludedIndex; i++ {
			if rf.getLogEntryAt(i).Term == term {
				start = minInt(start, i)
				end = maxInt(end, i)
			}
		}
		if end == -1 {
			return -1, -1
		}
		return start, end
	}
}
