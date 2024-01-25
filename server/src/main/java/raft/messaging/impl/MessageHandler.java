package raft.messaging.impl;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.message.Initialize;
import raft.message.RaftMessage;
import raft.message.ReleaseResources;
import raft.state_machine.State;
import raft.state_machine.StateFactory;

@NotThreadSafe
public class MessageHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

	private State currentState;
	private final StateFactory stateFactory;

	public MessageHandler(NodeState nodeState, StateFactory stateFactory) {
		this.stateFactory = stateFactory;
		changeState(nodeState);
	}

	public void onMessage(RaftMessage message) {
		LOGGER.info("On message {}", message);
		currentState.onMessage(message, this);
		LOGGER.info("Message processed!");
	}

	public void changeState(NodeState nodeState) {
		LOGGER.info("Changing state to {}", nodeState);
		if (currentState == null) {
			LOGGER.info("No previous state...");
		} else {
			LOGGER.info("Releasing resources of previous state");
			currentState.onMessage(new ReleaseResources(), this);
		}
		currentState = stateFactory.createState(nodeState);
		currentState.onMessage(new Initialize(), this);
	}

}
