package constants

const SynchronousLogger = true

const (
	LogRaftStart = iota
	LogElection
	LogStart
	LogAppendEntries
	LogRejectAppendEntries
	LogMatchPreviousEntryAppendEntries
	LogTruncateLogAppendEntries
	LogUpdateAppendEntries
	LogAppendingAppendEntries
	LogUpdateCommitIndexAppendEntries
	LogCommittingEntriesAppendEntries
	LogHeartbeat
	LogSyncLogEntries
)

const (
	LogClerk = iota
	LogServer
)

var RaftLoggingMap = map[int]string{
	LogRaftStart:                       "RaftStartEvent",
	LogElection:                        "ElectionEvent",
	LogStart:                           "StartEvent",
	LogAppendEntries:                   "AppendEntriesEvent",
	LogRejectAppendEntries:             "RejectAppendEntriesEvent",
	LogMatchPreviousEntryAppendEntries: "MatchPreviousEntryAppendEntriesEvent",
	LogTruncateLogAppendEntries:        "TruncateLogAppendEntriesEvent",
	LogUpdateAppendEntries:             "UpdateAppendEntriesEvent",
	LogAppendingAppendEntries:          "AppendingAppendEntriesEvent",
	LogUpdateCommitIndexAppendEntries:  "UpdateCommitIndexAppendEntriesEvent",
	LogCommittingEntriesAppendEntries:  "CommittingEntriesAppendEntriesEvent",
	LogHeartbeat:                       "HeartbeatEvent",
	LogSyncLogEntries:                  "SyncLogEntriesEvent",
}

var ClerkLoggingMap = map[int]string{
	LogClerk:  "ClerkEvent",
	LogServer: "ServerEvent",
	// TODO: Add more log types here
}
