package raft.state_machine.follower;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.state_machine.RaftMessageProcessor;

public class FollowerResourceReleaser implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FollowerResourceReleaser.class);

	private final FollowerStateData followerStateData;

	public FollowerResourceReleaser(FollowerStateData followerStateData) {
		this.followerStateData = followerStateData;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		LOGGER.info("Stopping heart beat check...");
		Optional.ofNullable(followerStateData.getHeartBeatTask())
				.ifPresent(CancellableTask::cancel);
	}
}
