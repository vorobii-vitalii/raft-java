package raft.state_machine.follower;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import raft.scheduling.CancellableTask;

@ThreadSafe
public class FollowerStateData {
	public static final int LEADER_NOT_CHOSEN = -1;
	private final AtomicReference<CancellableTask> heartBeatCheckTask = new AtomicReference<>();
	private final AtomicReference<Instant> lastHeartBeat = new AtomicReference<>();
	private final Clock clock;
	private final AtomicInteger currentLeader = new AtomicInteger(LEADER_NOT_CHOSEN);

	public FollowerStateData(Clock clock) {
		this.clock = clock;
		lastHeartBeat.set(clock.instant());
	}

	public void updateLeader(int leaderId) {
		assert leaderId >= 0;
		currentLeader.set(leaderId);
	}

	public Optional<Integer> getCurrentLeader() {
		return Optional.of(currentLeader.get()).filter(v -> v != LEADER_NOT_CHOSEN);
	}

	public Duration getDurationSinceLastHeartBeat() {
		return Duration.between(lastHeartBeat.get(), clock.instant());
	}

	public void updateHeartBeat() {
		lastHeartBeat.set(clock.instant());
	}

	public Instant getLastHeartBeat() {
		return lastHeartBeat.get();
	}

	public void setHeartBeatCheckTask(CancellableTask task) {
		heartBeatCheckTask.set(task);
	}

	public CancellableTask getHeartBeatTask() {
		return heartBeatCheckTask.get();
	}

}
