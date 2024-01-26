package raft.message;

import raft.dto.Log;

public record AppendEntriesErrorMessage(int serverId, Log prevLog, Log nextLog) implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.APPEND_ENTRIES_ERROR_MESSAGE;
	}
}
