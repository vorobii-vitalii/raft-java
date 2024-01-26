package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.leader.data.LeaderStateData;
import raft.storage.LogStorage;

public class LeaderResourceReleaser implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderResourceReleaser.class);

	private final LeaderStateData leaderStateData;
	private final LogStorage logStorage;

	public LeaderResourceReleaser(LeaderStateData leaderStateData, LogStorage logStorage) {
		this.leaderStateData = leaderStateData;
		this.logStorage = logStorage;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		try {
			LOGGER.info("Stopping heart beat task...");
			Optional.ofNullable(leaderStateData.getHeartBeatTask()).ifPresent(CancellableTask::cancel);
			LOGGER.info("Removing all uncommitted changes");
			logStorage.removeUncommittedChanges();
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
