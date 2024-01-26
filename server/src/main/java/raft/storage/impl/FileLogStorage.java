package raft.storage.impl;

import static raft.utils.LogUtils.LOG_ID_COMPARATOR;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.dto.Log;
import raft.dto.LogId;
import raft.storage.IndexFile;
import raft.storage.LogFile;
import raft.storage.LogStorage;

@NotThreadSafe
public class FileLogStorage implements LogStorage {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileLogStorage.class);

	private final IndexFile<LogId> indexFile;
	private final LogFile logFile;
	private final TreeMap<LogId, Log> uncommittedChanges = new TreeMap<>(LOG_ID_COMPARATOR);

	public FileLogStorage(IndexFile<LogId> indexFile, LogFile logFile) {
		this.indexFile = indexFile;
		this.logFile = logFile;
	}

	@Override
	public boolean appendLog(LogId prevLogID, List<Log> nextLogs) throws IOException {
		LOGGER.info("Appending logs {}, prev = {}", nextLogs, prevLogID);
		if (!isPresent(prevLogID)) {
			return false;
		}
		if (nextLogs.isEmpty()) {
			LOGGER.info("Its just heartbeat");
			return true;
		}
		if (prevLogID == null) {
			LOGGER.info("No previous log id, cleaning whole file");
			logFile.clear();
			indexFile.clear();
		} else {
			var lastOffsetOpt = indexFile.keepUntil(prevLogID);
			if (lastOffsetOpt.isPresent()) {
				LOGGER.info("There are other logs after {}. Removing them!", prevLogID);
				logFile.removeFrom(lastOffsetOpt.get());
			} else {
				LOGGER.info("No other logs after {}", prevLogID);
			}
		}
		LOGGER.info("Appending logs = {} to uncommitted log for now", nextLogs);
		for (var nextLog : nextLogs) {
			uncommittedChanges.put(nextLog.getId(), nextLog);
		}
		return true;
	}

	@Override
	public void addToEnd(Log log) {
		LOGGER.info("Adding log {} to end of uncommitted changes!", log);
		uncommittedChanges.put(log.getId(), log);
	}

	@Override
	public Optional<LogId> getLastLogId() throws IOException {
		LOGGER.info("Getting last log id");
		if (uncommittedChanges.isEmpty()) {
			LOGGER.info("Fetching it from file");
			return getLastAppliedLog();
		}
		LOGGER.info("Fetching from memory structure");
		return Optional.of(uncommittedChanges.lastKey());
	}

	@Override
	public Log getById(LogId logId) throws IOException {
		LOGGER.info("Getting log by id = {}", logId);
		if (uncommittedChanges.containsKey(logId)) {
			LOGGER.info("Getting from memory");
			return uncommittedChanges.get(logId);
		}
		return createLogById(logId);
	}

	@Override
	public Optional<Log> findPrevious(LogId logId) throws IOException {
		LOGGER.info("Finding previous log of {}", logId);
		if (logId == null) {
			return Optional.empty();
		}
		if (uncommittedChanges.containsKey(logId)) {
			LOGGER.info("Log id contains in uncommitted changes");
			var previousEntry = uncommittedChanges.lowerEntry(logId);
			if (previousEntry != null) {
				return Optional.of(previousEntry.getValue());
			}
			LOGGER.info("It is first log in uncommitted changes. Taking last log from file");
			var lastAppliedLog = getLastAppliedLog();
			return lastAppliedLog.isEmpty()
					? Optional.empty()
					: Optional.of(createLogById(lastAppliedLog.get()));
		}
		LOGGER.info("Log id absent in uncommitted changes");
		Optional<LogId> predecessor = indexFile.findPreviousLogId(logId);
		if (predecessor.isEmpty()) {
			LOGGER.info("No previous log id");
			return Optional.empty();
		}
		var previousId = predecessor.get();
		var log = createLogById(previousId);
		LOGGER.info("Previous log = {}", log);
		return Optional.of(log);
	}

	@Override
	public Optional<Log> findFollowing(LogId logId) throws IOException {
		LOGGER.info("Finding previous log of {}", logId);
		if (uncommittedChanges.containsKey(logId)) {
			LOGGER.info("Log id contains in uncommitted changes");
			var nextEntry = uncommittedChanges.higherEntry(logId);
			if (nextEntry != null) {
				return Optional.of(nextEntry.getValue());
			}
			LOGGER.info("It is last log in uncommitted changes. Hence no following log");
			return Optional.empty();
		}
		Optional<LogId> next = indexFile.findNextLogId(logId);
		if (next.isEmpty()) {
			LOGGER.info("No next log id. So returning first one from uncommitted changes");
			return Optional.ofNullable(uncommittedChanges.firstEntry())
					.map(Map.Entry::getValue);
		}
		var nextId = next.get();
		var log = createLogById(nextId);
		LOGGER.info("Next log = {}", log);
		return Optional.of(log);
	}

	@Override
	public Optional<LogId> getLastAppliedLog() throws IOException {
		LOGGER.info("Finding last log");
		return indexFile.findLastLog();
	}

	@Override
	public void removeUncommittedChanges() {
		LOGGER.info("Removing all uncommitted changes ({} changes)", uncommittedChanges.size());
		uncommittedChanges.clear();
	}

	@Override
	public void applyAllChangesUntil(LogId untilLogId) throws IOException {
		LOGGER.info("Applying uncommitted changes until {}", untilLogId);
		for (var entry : uncommittedChanges.entrySet()) {
			var logId = entry.getKey();
			if (LOG_ID_COMPARATOR.compare(logId, untilLogId) > 0) {
				break;
			}
			var log = entry.getValue();
			LOGGER.info("Persisting log {}", log);
			appendToFile(log);
		}
		LOGGER.info("Removing persisted changes from uncommitted changes list");
		uncommittedChanges.headMap(untilLogId, true).clear();
	}

	private boolean isPresent(LogId logId) throws IOException {
		LOGGER.info("Checking whether log {} was applied", logId);
		if (logId == null || isInitial(logId)) {
			LOGGER.info("Log null, its applied");
			return true;
		}
		if (uncommittedChanges.containsKey(logId)) {
			LOGGER.info("Log present in uncommitted changes");
			return true;
		}
		boolean presentInLogFile = indexFile.getOffsetById(logId).isPresent();
		LOGGER.info("Log {} present in file = {}", logId, presentInLogFile);
		return presentInLogFile;
	}

	private boolean isInitial(LogId logId) {
		return logId.getIndex() == 0 && logId.getTerm() == 0;
	}

	private Log createLogById(LogId logId) throws IOException {
		Optional<Long> offset = indexFile.getOffsetById(logId);
		if (offset.isEmpty()) {
			throw new IllegalArgumentException("Log id " + logId + " absent!");
		}
		byte[] log = logFile.getLog(offset.get());
		return Log.newBuilder().setId(logId).setMsg(new String(log, StandardCharsets.UTF_8)).build();
	}

	private void appendToFile(Log nextLog) throws IOException {
		var nextMsgID = nextLog.getId();
		var msgBytes = Objects.requireNonNull(nextLog.getMsg()).getBytes(StandardCharsets.UTF_8);
		long logOffset = logFile.append(msgBytes);
		LOGGER.info("Appended new log by id = {} offset = {}", nextLog, logOffset);
		indexFile.addLast(nextMsgID, logOffset);
	}

	protected TreeMap<LogId, Log> getUncommittedChanges() {
		return uncommittedChanges;
	}
}
