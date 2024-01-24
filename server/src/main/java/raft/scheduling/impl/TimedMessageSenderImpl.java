package raft.scheduling.impl;

import java.util.function.Supplier;

import raft.messaging.MessagePublisher;
import raft.scheduling.CancellableTask;
import raft.scheduling.TimedMessageSender;
import raft.scheduling.TimedTaskScheduler;

public class TimedMessageSenderImpl<MSG_TYPE> implements TimedMessageSender<MSG_TYPE> {
	private final TimedTaskScheduler taskScheduler;
	private final MessagePublisher<MSG_TYPE> messagePublisher;

	public TimedMessageSenderImpl(TimedTaskScheduler taskScheduler, MessagePublisher<MSG_TYPE> messagePublisher) {
		this.taskScheduler = taskScheduler;
		this.messagePublisher = messagePublisher;
	}

	@Override
	public CancellableTask schedulePeriodical(int delayMs, Supplier<MSG_TYPE> messageSupplier) {
		return taskScheduler.scheduleWithFixedDelay(delayMs, () -> messagePublisher.publish(messageSupplier.get()));
	}

	@Override
	public CancellableTask scheduleOnce(int delay, Supplier<MSG_TYPE> messageSupplier) {
		return taskScheduler.scheduleAfter(delay, () -> messagePublisher.publish(messageSupplier.get()));
	}
}
