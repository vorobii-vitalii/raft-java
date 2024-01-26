package raft.state_machine.leader;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.cluster.ClusterConfig;
import raft.dto.Log;
import raft.dto.LogId;
import raft.message.SendHeartBeat;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.state_machine.leader.services.LogAppender;
import raft.storage.LogStorage;

class TestLeaderSendHeartBeatHandler {

	public static final int SERVER_1 = 1;
	public static final int SERVER_2 = 2;
	public static final LogId LAST_LOG_ID = LogId.newBuilder().setTerm(2).setIndex(5).build();
	LogStorage logStorage = Mockito.mock(LogStorage.class);
	ServersReplicationState serversReplicationState = Mockito.mock(ServersReplicationState.class);
	LogAppender logAppender = Mockito.mock(LogAppender.class);
	ClusterConfig clusterConfig = Mockito.mock(ClusterConfig.class);

	LeaderSendHeartBeatHandler leaderSendHeartBeatHandler = new LeaderSendHeartBeatHandler(
			logStorage,
			serversReplicationState,
			logAppender,
			clusterConfig
	);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	@Test
	void sendHeartBeatsGivenLogStorageEmpty() throws IOException {
		when(logStorage.getLastLogId()).thenReturn(Optional.empty());
		when(clusterConfig.getOtherServerIds()).thenReturn(Set.of(SERVER_1, SERVER_2));
		when(serversReplicationState.isCurrentlyReplicating(SERVER_1)).thenReturn(true);
		when(serversReplicationState.isCurrentlyReplicating(SERVER_2)).thenReturn(false);

		leaderSendHeartBeatHandler.process(new SendHeartBeat(), messageHandler);

		verify(serversReplicationState, never()).startReplication(SERVER_1);
		verify(serversReplicationState).startReplication(SERVER_2);
		verify(logAppender).appendLog(SERVER_2, null, null);
	}

	@Test
	void sendHeartBeatsGivenLogStorageNotEmpty() throws IOException {
		when(logStorage.getLastLogId()).thenReturn(Optional.of(LAST_LOG_ID));
		when(logStorage.getById(LAST_LOG_ID)).thenReturn(Log.newBuilder().setId(LAST_LOG_ID).setMsg("msg").build());

		when(clusterConfig.getOtherServerIds()).thenReturn(Set.of(SERVER_1, SERVER_2));
		when(serversReplicationState.isCurrentlyReplicating(SERVER_1)).thenReturn(true);
		when(serversReplicationState.isCurrentlyReplicating(SERVER_2)).thenReturn(false);

		leaderSendHeartBeatHandler.process(new SendHeartBeat(), messageHandler);

		verify(serversReplicationState, never()).startReplication(SERVER_1);
		verify(serversReplicationState).startReplication(SERVER_2);
		verify(logAppender).appendLog(SERVER_2, null, Log.newBuilder().setId(LAST_LOG_ID).setMsg("msg").build());
	}


}