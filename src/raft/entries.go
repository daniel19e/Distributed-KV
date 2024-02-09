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
func (entries Entries) get(index int) *LogEntry {
	if index < 0 {
		log.Panic("index < 0.\n")
	}
	if index == 0 {
		return &LogEntry{
			Command: nil,
			Term:    0,
		}
	}
	if index > len(entries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &entries[index-1]
}
func (entries Entries) lastIndex() int {
	return len(entries)
}
func (entries Entries) lastTerm() int {
	return entries.get(len(entries)).Term
}

func (entries Entries) slice(start int, end int) Entries {
	if start <= 0 {
		log.Panic("slice: start out of range. \n")
	}
	if end > len(entries)+1 {
		log.Panic("slice: end out of range.\n")
	}
	if start > end {
		log.Panic("slice: start > end.\n")
	}
	return entries[start-1 : end-1]
}

func (entries Entries) findTermRange(term int) (int, int) {
	if term == 0 {
		return 0, 0
	} else {
		start, end := math.MaxInt, -1
		for i := 1; i <= len(entries); i++ {
			if entries.get(i).Term == term {
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
