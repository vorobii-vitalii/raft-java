package raft.state_machine.leader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.dto.LogId;
import raft.message.Initialize;
import raft.message.RaftMessage;
import raft.message.SendHeartBeat;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.TimedMessageSender;
import raft.state_machine.leader.data.LeaderStateData;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.storage.LogStorage;

@SuppressWarnings("unchecked")
class TestLeaderInitializer {
	private static final int HEART_BEATS_INTERVAL = 1212;
	public static final LogId LAST_APPLIED_LOG_ID = LogId.newBuilder().build();

	LogStorage logStorage = Mockito.mock(LogStorage.class);
	ServersReplicationState serversReplicationState = Mockito.mock(ServersReplicationState.class);
	MessagePublisher<RaftMessage> messageMessagePublisher = Mockito.mock(MessagePublisher.class);
	TimedMessageSender<RaftMessage> timedMessageSender = Mockito.mock(TimedMessageSender.class);
	LeaderStateData leaderStateData = Mockito.mock(LeaderStateData.class);

	LeaderInitializer leaderInitializer = new LeaderInitializer(
			logStorage,
			serversReplicationState,
			messageMessagePublisher,
			timedMessageSender,
			HEART_BEATS_INTERVAL,
			leaderStateData
	);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	@Test
	void process() throws IOException {
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.of(LAST_APPLIED_LOG_ID));
		leaderInitializer.process(new Initialize(), messageHandler);
		verify(serversReplicationState).initializePreviousLogTable(LAST_APPLIED_LOG_ID);
		verify(messageMessagePublisher).publish(new SendHeartBeat());
	}
}
