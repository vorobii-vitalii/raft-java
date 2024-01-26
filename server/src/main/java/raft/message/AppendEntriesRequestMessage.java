package raft.message;

import java.util.function.Consumer;

import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;

public record AppendEntriesRequestMessage(AppendEntriesRequest request, Consumer<AppendEntriesReply> replyConsumer) implements RaftMessage  {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.APPEND_ENTRIES_REQUEST_MESSAGE;
	}
}
