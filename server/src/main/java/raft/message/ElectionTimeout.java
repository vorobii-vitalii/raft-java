package raft.message;

public record ElectionTimeout() implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.ELECTION_TIMEOUT;
	}
}
