package raft.message;

public record ReleaseResources() implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.Release;
	}
}
