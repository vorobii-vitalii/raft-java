package raft.state_machine.leader;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.message.ReleaseResources;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.state_machine.leader.data.LeaderStateData;
import raft.storage.LogStorage;

class TestLeaderResourceReleaser {

	LeaderStateData leaderStateData = Mockito.mock(LeaderStateData.class);
	LogStorage logStorage = Mockito.mock(LogStorage.class);
	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	LeaderResourceReleaser leaderResourceReleaser = new LeaderResourceReleaser(leaderStateData, logStorage);

	CancellableTask cancellableTask = Mockito.mock(CancellableTask.class);

	@Test
	void releaseResources() throws IOException {
		when(leaderStateData.getHeartBeatTask()).thenReturn(cancellableTask);
		leaderResourceReleaser.process(new ReleaseResources(), messageHandler);
		verifyNoInteractions(messageHandler);
		verify(cancellableTask).cancel();
		verify(logStorage).removeUncommittedChanges();
	}

}
