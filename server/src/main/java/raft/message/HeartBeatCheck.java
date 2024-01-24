package raft.message;

public record HeartBeatCheck() implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.HeartBeatCheck;
	}
}
