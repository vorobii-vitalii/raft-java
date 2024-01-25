package raft.state_machine.candidate.domain;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import raft.scheduling.CancellableTask;

@ThreadSafe
public class CandidateStateData {
	private final AtomicReference<CancellableTask> electionTimeoutTaskHolder = new AtomicReference<>();

	public CancellableTask getElectionTimeoutTask() {
		return electionTimeoutTaskHolder.get();
	}

	public void setElectionTimeoutTask(CancellableTask electionTimeoutTask) {
		electionTimeoutTaskHolder.set(electionTimeoutTask);
	}
}
