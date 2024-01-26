package raft.state_machine.follower;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import raft.domain.NodeState;
import raft.message.HeartBeatCheck;
import raft.messaging.impl.MessageHandler;

class TestFollowerHeartBeatCheckHandler {
	private static final Duration HEART_BEAT_TIMEOUT = Duration.ofSeconds(10);

	FollowerStateData followerStateData = mock(FollowerStateData.class);

	FollowerHeartBeatCheckHandler followerHeartBeatCheckHandler = new FollowerHeartBeatCheckHandler(HEART_BEAT_TIMEOUT, followerStateData);

	MessageHandler messageHandler = mock(MessageHandler.class);

	@Test
	void givenDurationSinceLastHeartBeatIsLowerThanTimeout() {
		when(followerStateData.getDurationSinceLastHeartBeat())
				.thenReturn(HEART_BEAT_TIMEOUT.minusSeconds(2));

		followerHeartBeatCheckHandler.process(new HeartBeatCheck(), messageHandler);

		verifyNoInteractions(messageHandler);
	}

	@Test
	void givenDurationSinceLastHeartBeatIsHigherThanTimeout() {
		when(followerStateData.getDurationSinceLastHeartBeat())
				.thenReturn(HEART_BEAT_TIMEOUT.plusSeconds(1));

		followerHeartBeatCheckHandler.process(new HeartBeatCheck(), messageHandler);

		verify(messageHandler).changeState(NodeState.CANDIDATE);
	}

}