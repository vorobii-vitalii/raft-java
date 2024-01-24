package raft.message;

import raft.dto.AppendEntriesReply;
import raft.dto.Log;

public record AppendEntriesReplyMessage(AppendEntriesReply reply, Log nextLog, Log previousLog, int serverId) implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.AppendEntriesReplyMessage;
	}
}
