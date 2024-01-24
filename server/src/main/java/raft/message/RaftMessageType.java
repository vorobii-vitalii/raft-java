package raft.message;

public enum RaftMessageType {
	AddLog,
	AppendEntriesErrorMessage,
	AppendEntriesReplyMessage,
	AppendEntriesRequestMessage,
	ElectionTimeout,
	HeartBeatCheck,
	RequestVoteErrorReceived,
	RequestVoteReplyReceived,
	RequestVoteRequestMessage,
	SendHeartBeat,
	Initialize,
	Release
}
