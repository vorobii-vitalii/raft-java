package raft.state_machine.follower;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.scheduling.CancellableTask;

class TestFollowerStateData {

	public static final Instant REF_TIME = Instant.now();
	Clock clock = Mockito.mock(Clock.class);

	public static final int LEADER_ID = 123;
	FollowerStateData followerStateData = new FollowerStateData(clock);

	@Test
	void resetLeader() {
		followerStateData.updateLeader(LEADER_ID);
		followerStateData.resetLeader();
		assertThat(followerStateData.getCurrentLeader()).isEmpty();
	}

	@Test
	void updateLeader() {
		followerStateData.updateLeader(LEADER_ID);
		assertThat(followerStateData.getCurrentLeader()).contains(LEADER_ID);
	}

	@Test
	void getDurationSinceLastHeartBeat() {
		when(clock.instant()).thenReturn(REF_TIME, REF_TIME.plusSeconds(5));
		followerStateData.updateHeartBeat();
		assertThat(followerStateData.getDurationSinceLastHeartBeat()).isEqualByComparingTo(Duration.ofSeconds(5));
	}

	@Test
	void setHeartBeatCheckTask() {
		var cancellableTask = Mockito.mock(CancellableTask.class);
		followerStateData.setHeartBeatCheckTask(cancellableTask);
		assertThat(followerStateData.getHeartBeatTask()).isEqualTo(cancellableTask);
	}

}