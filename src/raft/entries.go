package raft

type Entries []*LogEntry

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (entries Entries) get(idx int) LogEntry {
	return *entries[idx]
}
func (entries Entries) lastIndex() int {
	return entries[len(entries)-1].Index
}
func (entries Entries) lastTerm() int {
	return entries[len(entries)-1].Term
}

func (entries Entries) slice(start int, end int) Entries {
	if start > end {
		return Entries{}
	}
	if end > len(entries)+1 {
		return Entries{}
	}
	if start <= 0 {
		return Entries{}
	}
	return entries[start:end]
}
