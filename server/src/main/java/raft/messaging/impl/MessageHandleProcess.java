package raft.messaging.impl;

import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.RaftMessage;
import raft.messaging.ProcessCondition;

public class MessageHandleProcess implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandleProcess.class);

	private final MessageHandler messageHandler;
	private final ProcessCondition processCondition;
	private final Queue<RaftMessage> raftMessages;

	public MessageHandleProcess(MessageHandler messageHandler, ProcessCondition processCondition, Queue<RaftMessage> raftMessages) {
		this.messageHandler = messageHandler;
		this.processCondition = processCondition;
		this.raftMessages = raftMessages;
	}

	@Override
	public void run() {
		LOGGER.info("Starting message handling process!");
		while (processCondition.shouldContinue()) {
			var message = raftMessages.poll();
			if (message == null) {
				continue;
			}
			LOGGER.info("Read new message from queue {}", message);
			messageHandler.onMessage(message);
		}
		LOGGER.info("Received request to stop message handling...");
	}
}
