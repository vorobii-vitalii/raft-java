package raft.message;

import java.util.function.Consumer;

import raft.dto.AppendLogReply;
import raft.dto.AppendLogRequest;

public record AddLog(AppendLogRequest appendLogRequest, Consumer<AppendLogReply> replyConsumer) implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.AddLog;
	}
}
