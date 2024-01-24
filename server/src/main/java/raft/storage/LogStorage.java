package raft.storage;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import raft.dto.Log;
import raft.dto.LogId;

public interface LogStorage {

	/**
	 * Returns true if and only if prevLogID found in "log storage"
	 * @param prevLogID previous log id
	 * @param logs logs to append
	 */
	boolean appendLog(LogId prevLogID, List<Log> logs) throws IOException;

	void addToEnd(Log log) throws IOException;

	Optional<LogId> getLastLogId() throws IOException;

	Log getById(LogId logId) throws IOException;

	/**
	 * Returns previous log
	 * @param logId log id, it must exist in storage
	 * @return Previous log
	 */
	Optional<Log> findPrevious(LogId logId) throws IOException;

	/**
	 * Returns following log
	 * @param logId log id, it must exist in storage
	 * @return Following log
	 */
	Optional<Log> findFollowing(LogId logId) throws IOException;

	Optional<LogId> getLastAppliedLog() throws IOException;

	void removeUncommittedChanges() throws IOException;

	void applyAllChangesUntil(LogId untilLogId) throws IOException;

}
