package raft.message;

import java.util.function.Consumer;

import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;

public record RequestVoteRequestMessage(RequestVote requestVote, Consumer<RequestVoteReply> replyConsumer) implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.RequestVoteRequestMessage;
	}
}
