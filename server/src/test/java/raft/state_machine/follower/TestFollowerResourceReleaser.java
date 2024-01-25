package raft.state_machine.follower;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.message.ReleaseResources;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;

class TestFollowerResourceReleaser {
	FollowerStateData followerStateData = Mockito.mock(FollowerStateData.class);

	FollowerResourceReleaser followerResourceReleaser = new FollowerResourceReleaser(followerStateData);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	CancellableTask cancellableTask = Mockito.mock(CancellableTask.class);

	@Test
	void process() {
		when(followerStateData.getHeartBeatTask()).thenReturn(cancellableTask);
		followerResourceReleaser.process(new ReleaseResources(), messageHandler);
		verify(cancellableTask).cancel();
	}
}