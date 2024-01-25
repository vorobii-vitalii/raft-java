package raft.state_machine.follower;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import raft.message.HeartBeatCheck;
import raft.message.Initialize;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.scheduling.TimedMessageSender;

@SuppressWarnings("unchecked")
class TestFollowerInitializer {
	private static final int HEART_BEAT_CHECK_DELAY = 123456;

	FollowerStateData followerStateData = mock(FollowerStateData.class);
	TimedMessageSender<RaftMessage> raftTimedMessageSender = mock(TimedMessageSender.class);
	FollowerInitializer followerInitializer = new FollowerInitializer(followerStateData, raftTimedMessageSender, HEART_BEAT_CHECK_DELAY);

	MessageHandler messageHandler = mock(MessageHandler.class);

	CancellableTask cancellableTask = mock(CancellableTask.class);

	@Test
	void process() {
		when(raftTimedMessageSender.schedulePeriodical(
				eq(HEART_BEAT_CHECK_DELAY),
				argThat(v -> new HeartBeatCheck().equals(v.get()))
		)).thenReturn(cancellableTask);

		followerInitializer.process(new Initialize(), messageHandler);

		verify(followerStateData).setHeartBeatCheckTask(cancellableTask);
		verify(followerStateData).updateHeartBeat();
	}
}