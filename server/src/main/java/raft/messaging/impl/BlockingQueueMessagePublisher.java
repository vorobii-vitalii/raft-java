package raft.messaging.impl;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.messaging.MessagePublisher;

public class BlockingQueueMessagePublisher<T> implements MessagePublisher<T> {
	private static final Logger LOGGER = LoggerFactory.getLogger(BlockingQueueMessagePublisher.class);

	private final BlockingQueue<T> blockingQueue;

	public BlockingQueueMessagePublisher(BlockingQueue<T> blockingQueue) {
		this.blockingQueue = blockingQueue;
	}

	@Override
	public void publish(T msg) {
		try {
			blockingQueue.put(msg);
		}
		catch (InterruptedException e) {
			LOGGER.warn("Interrupted on push message", e);
			Thread.currentThread().interrupt();
		}
	}
}
