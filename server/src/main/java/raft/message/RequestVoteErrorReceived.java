package raft.message;

public record RequestVoteErrorReceived(int termId, int serverId) implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.REQUEST_VOTE_ERROR_RECEIVED;
	}
}
