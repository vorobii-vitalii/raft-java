package raft.messaging.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.jupiter.api.Test;

class TestBlockingQueueMessagePublisher {

	@Test
	void publish() {
		BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(3);
		BlockingQueueMessagePublisher<Integer> messagePublisher = new BlockingQueueMessagePublisher<>(queue);
		messagePublisher.publish(1);
		messagePublisher.publish(2);
		messagePublisher.publish(3);
		assertThat(queue).containsExactly(1, 2, 3);
	}
}
