package raft.state_machine.candidate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;

public class CandidateElectionTimeoutHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateElectionTimeoutHandler.class);

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		LOGGER.info("Election timeout... Cancelling election and becoming follower");
		messageHandler.changeState(NodeState.FOLLOWER);
	}
}
