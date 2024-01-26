package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.RaftMessage;
import raft.message.SendHeartBeat;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.TimedMessageSender;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.leader.data.LeaderStateData;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.storage.LogStorage;

public class LeaderInitializer implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderInitializer.class);

	private final LogStorage logStorage;
	private final ServersReplicationState serversReplicationState;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;
	private final TimedMessageSender<RaftMessage> timedMessageSender;
	private final int heartBeatsInterval;
	private final LeaderStateData leaderStateData;

	public LeaderInitializer(
			LogStorage logStorage,
			ServersReplicationState serversReplicationState,
			MessagePublisher<RaftMessage> raftMessagePublisher,
			TimedMessageSender<RaftMessage> timedMessageSender,
			int heartBeatsInterval,
			LeaderStateData leaderStateData
	) {
		this.logStorage = logStorage;
		this.serversReplicationState = serversReplicationState;
		this.raftMessagePublisher = raftMessagePublisher;
		this.timedMessageSender = timedMessageSender;
		this.heartBeatsInterval = heartBeatsInterval;
		this.leaderStateData = leaderStateData;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		LOGGER.info("Initializing max previous log id by server id");
		try {
			var maxAppliedLogId = logStorage.getLastAppliedLog().orElse(null);
			LOGGER.info("Max applied log id = {}", maxAppliedLogId);
			serversReplicationState.initializePreviousLogTable(maxAppliedLogId);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		LOGGER.info("I was elected, sending initial heartbeat...");
		raftMessagePublisher.publish(new SendHeartBeat());
		LOGGER.info("Creating periodic task for heart beats...");
		leaderStateData.setHeartBeatTask(timedMessageSender.schedulePeriodical(heartBeatsInterval, SendHeartBeat::new));
	}
}
