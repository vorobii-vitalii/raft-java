package raft.state_machine.leader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import raft.cluster.ClusterConfig;
import raft.dto.AppendLogReply;
import raft.dto.AppendLogRequest;
import raft.dto.Log;
import raft.dto.LogId;
import raft.message.AddLog;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.state_machine.leader.services.LogAppender;
import raft.state_machine.leader.services.LogIdGenerator;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

@SuppressWarnings("unchecked")
class TestLeaderAddLogMessageHandler {

	public static final int CURRENT_SERVER_ID = 123;
	public static final int CURRENT_TERM = 99;
	public static final LogId NEW_LOG_ID = LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(99).build();
	public static final int SERVER_1 = 1;
	public static final int SERVER_2 = 2;
	public static final String NEW_MESSAGE = "New message!";
	LogStorage logStorage = mock(LogStorage.class);
	ElectionState electionState = mock(ElectionState.class);
	LogAppender logAppender = mock(LogAppender.class);
	LogIdGenerator logIdGenerator = mock(LogIdGenerator.class);
	ClusterConfig clusterConfig = mock(ClusterConfig.class);
	ServersReplicationState serversReplicationState = mock(ServersReplicationState.class);

	LeaderAddLogMessageHandler leaderAddLogMessageHandler = new LeaderAddLogMessageHandler(
			logStorage,
			electionState,
			logAppender,
			logIdGenerator,
			clusterConfig,
			serversReplicationState
	);

	MessageHandler messageHandler = mock(MessageHandler.class);
	Consumer<AppendLogReply> replyConsumer = mock(Consumer.class);

	@Test
	void addLogGivenLogStorageIsCurrentlyEmpty() throws IOException {
		when(logStorage.getLastLogId()).thenReturn(Optional.empty());
		when(clusterConfig.getCurrentServerId()).thenReturn(CURRENT_SERVER_ID);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(clusterConfig.getOtherServerIds()).thenReturn(Set.of(SERVER_1, SERVER_2));
		when(serversReplicationState.isCurrentlyReplicating(SERVER_1)).thenReturn(false);
		when(serversReplicationState.isCurrentlyReplicating(SERVER_2)).thenReturn(true);
		when(logIdGenerator.getNextLogId()).thenReturn(NEW_LOG_ID);

		leaderAddLogMessageHandler.process(
				new AddLog(
						AppendLogRequest.newBuilder()
								.setMessage(NEW_MESSAGE)
								.build(),
						replyConsumer
				),
				messageHandler
		);

		verify(logAppender).appendLog(SERVER_1, Log.newBuilder().setId(NEW_LOG_ID).setMsg(NEW_MESSAGE).build(), null);
		verify(logStorage).addToEnd(Log.newBuilder().setId(NEW_LOG_ID).setMsg(NEW_MESSAGE).build());
	}

	@Test
	void addLogGivenLogStorageIsCurrentlyNotEmpty() throws IOException {
		when(logStorage.getLastLogId()).thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(98).build()));
		when(logStorage.getById(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(98).build()))
				.thenReturn(Log.newBuilder()
						.setMsg("previous")
						.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(98).build())
						.build());
		when(clusterConfig.getCurrentServerId()).thenReturn(CURRENT_SERVER_ID);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(clusterConfig.getOtherServerIds()).thenReturn(Set.of(SERVER_1, SERVER_2));
		when(serversReplicationState.isCurrentlyReplicating(SERVER_1)).thenReturn(false);
		when(serversReplicationState.isCurrentlyReplicating(SERVER_2)).thenReturn(true);
		when(logIdGenerator.getNextLogId()).thenReturn(NEW_LOG_ID);

		leaderAddLogMessageHandler.process(
				new AddLog(
						AppendLogRequest.newBuilder()
								.setMessage(NEW_MESSAGE)
								.build(),
						replyConsumer
				),
				messageHandler
		);

		verify(logAppender).appendLog(
				SERVER_1,
				Log.newBuilder().setId(NEW_LOG_ID).setMsg(NEW_MESSAGE).build(),
				Log.newBuilder().setMsg("previous").setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(98).build()).build());
		verify(logStorage).addToEnd(Log.newBuilder().setId(NEW_LOG_ID).setMsg(NEW_MESSAGE).build());
	}

}