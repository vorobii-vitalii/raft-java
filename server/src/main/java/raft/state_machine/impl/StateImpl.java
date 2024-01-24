package raft.state_machine.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.RaftMessage;
import raft.message.RaftMessageType;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.State;
import raft.state_machine.RaftMessageProcessor;

public class StateImpl implements State {
	private static final Logger LOGGER = LoggerFactory.getLogger(StateImpl.class);

	private final Map<RaftMessageType, RaftMessageProcessor> messageProcessorByType;

	public StateImpl(Map<RaftMessageType, RaftMessageProcessor> messageProcessorByType) {
		this.messageProcessorByType = messageProcessorByType;
	}

	@Override
	public void onMessage(RaftMessage message, MessageHandler messageHandler) {
		var messageType = message.getType();
		var messageProcessor = messageProcessorByType.get(messageType);
		if (messageProcessor != null) {
			messageProcessor.process(message, messageHandler);
		} else {
			LOGGER.warn("Ignoring message of type {}", messageType);
		}
	}
}
