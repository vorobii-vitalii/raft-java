package raft.scheduling.impl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import raft.messaging.MessagePublisher;
import raft.scheduling.TimedTaskScheduler;

@SuppressWarnings("unchecked")
class TestTimedMessageSenderImpl {

	public static final int DELAY_MS = 100;
	public static final int MSG = 123;
	MessagePublisher<Integer> messagePublisher = mock(MessagePublisher.class);

	TimedTaskScheduler timedTaskScheduler = mock(TimedTaskScheduler.class);

	TimedMessageSenderImpl<Integer> timedMessageSender = new TimedMessageSenderImpl<>(timedTaskScheduler, messagePublisher);


	@Test
	void schedulePeriodical() {
		ArgumentCaptor<Runnable> actionCaptor = ArgumentCaptor.forClass(Runnable.class);

		timedMessageSender.schedulePeriodical(DELAY_MS, () -> MSG);
		verify(timedTaskScheduler).scheduleWithFixedDelay(eq(DELAY_MS), actionCaptor.capture());
		actionCaptor.getValue().run();
		verify(messagePublisher).publish(MSG);
	}

	@Test
	void scheduleOnce() {
		ArgumentCaptor<Runnable> actionCaptor = ArgumentCaptor.forClass(Runnable.class);
		timedMessageSender.scheduleOnce(DELAY_MS, () -> MSG);
		verify(timedTaskScheduler).scheduleAfter(eq(DELAY_MS), actionCaptor.capture());
		actionCaptor.getValue().run();
		verify(messagePublisher).publish(MSG);
	}
}
