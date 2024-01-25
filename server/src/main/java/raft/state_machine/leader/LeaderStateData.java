package raft.state_machine.leader;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import raft.scheduling.CancellableTask;

@ThreadSafe
public class LeaderStateData {
	private final AtomicReference<CancellableTask> heartBeatSendTask = new AtomicReference<>();

	public void setHeartBeatTask(CancellableTask cancellableTask) {
		heartBeatSendTask.set(cancellableTask);
	}

	public CancellableTask getHeartBeatTask() {
		return heartBeatSendTask.get();
	}

}
