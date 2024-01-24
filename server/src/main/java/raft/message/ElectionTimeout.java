package raft.message;

public record ElectionTimeout() implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.ElectionTimeout;
	}
}
