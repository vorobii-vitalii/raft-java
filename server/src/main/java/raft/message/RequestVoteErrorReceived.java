package raft.message;

public record RequestVoteErrorReceived(int termId, int serverId) implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.RequestVoteErrorReceived;
	}
}
