package raft.state_machine;

import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;

public interface State {
	void onMessage(RaftMessage message, MessageHandler messageHandler);
}
