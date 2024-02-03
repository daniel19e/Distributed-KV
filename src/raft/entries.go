package raft

type Entries []LogEntry

type LogEntry struct {
	Term    int
	Command interface{}
}

func (entries Entries) getEntryAt(idx int) LogEntry {
	if idx < 0 || idx > len(entries) {
		BetterDebug(dError, "Trying to get log entry with invalid index.\n")
		return LogEntry{Command: nil, Term: -1}
	} else if idx == 0 { // log index starts at 1
		return LogEntry{Command: nil, Term: 0}
	}
	return entries[idx-1]
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
	return entries[start-1 : end-1]
}
