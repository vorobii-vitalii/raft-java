package raft.state_machine.follower;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;

public class FollowerHeartBeatCheckHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FollowerHeartBeatCheckHandler.class);

	private final Duration heartBeatTimeout;
	private final FollowerStateData followerStateData;

	public FollowerHeartBeatCheckHandler(Duration heartBeatTimeout, FollowerStateData followerStateData) {
		this.heartBeatTimeout = heartBeatTimeout;
		this.followerStateData = followerStateData;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var durationSinceLastHeartbeat = followerStateData.getDurationSinceLastHeartBeat();
		if (durationSinceLastHeartbeat.compareTo(heartBeatTimeout) > 0) {
			LOGGER.info("Duration since last heartbeat {} > {}. Becoming candidate 🙂", durationSinceLastHeartbeat, heartBeatTimeout);
			messageHandler.changeState(NodeState.CANDIDATE);
		} else {
			LOGGER.info("Leader sent heart beat in time, keeping follower state!");
		}
	}
}
