package raft.state_machine;

import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;

public interface RaftMessageProcessor {
	void process(RaftMessage message, MessageHandler messageHandler);
}
