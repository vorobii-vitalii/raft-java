package raft.message;

public sealed interface RaftMessage
		permits AddLog, AppendEntriesErrorMessage, AppendEntriesReplyMessage, AppendEntriesRequestMessage, ElectionTimeout, HeartBeatCheck,
		Initialize, ReleaseResources, RequestVoteErrorReceived, RequestVoteReplyReceived, RequestVoteRequestMessage, SendHeartBeat {

	RaftMessageType getType();

}
