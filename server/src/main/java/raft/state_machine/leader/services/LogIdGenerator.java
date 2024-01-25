package raft.state_machine.leader.services;

import java.io.IOException;
import java.io.UncheckedIOException;

import raft.dto.LogId;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import raft.utils.LogUtils;

public class LogIdGenerator {
	private final LogStorage logStorage;
	private final ElectionState electionState;

	public LogIdGenerator(LogStorage logStorage, ElectionState electionState) {
		this.logStorage = logStorage;
		this.electionState = electionState;
	}

	public LogId getNextLogId() {
		try {
			var updatedFromLog = logStorage
					.getLastLogId()
					.map(v -> v.toBuilder().setIndex(v.getIndex() + 1).build())
					.orElse(null);
			var currentTerm = electionState.getCurrentTerm();
			return LogUtils.max(updatedFromLog, LogId.newBuilder().setIndex(0).setTerm(currentTerm).build());
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

}
