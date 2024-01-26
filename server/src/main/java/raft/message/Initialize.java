package raft.message;

public record Initialize() implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.INITIALIZE;
	}
}
