package raft.message;

import raft.dto.RequestVoteReply;

public record RequestVoteReplyReceived(RequestVoteReply reply, int termId, int serverId) implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.RequestVoteReplyReceived;
	}
}
