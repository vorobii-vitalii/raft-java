package raft.state_machine.follower;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.HeartBeatCheck;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.TimedMessageSender;
import raft.state_machine.RaftMessageProcessor;

public class FollowerInitializer implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FollowerInitializer.class);

	private final FollowerStateData followerStateData;
	private final TimedMessageSender<RaftMessage> raftTimedMessageSender;
	private final int heartBeatCheckDelay;

	public FollowerInitializer(
			FollowerStateData followerStateData,
			TimedMessageSender<RaftMessage> raftTimedMessageSender,
			int heartBeatCheckDelay
	) {
		this.followerStateData = followerStateData;
		this.raftTimedMessageSender = raftTimedMessageSender;
		this.heartBeatCheckDelay = heartBeatCheckDelay;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		LOGGER.info("Scheduling heart beat check process");
		followerStateData.setHeartBeatCheckTask(
				raftTimedMessageSender.schedulePeriodical(heartBeatCheckDelay, HeartBeatCheck::new));
		followerStateData.updateHeartBeat();
	}

}
