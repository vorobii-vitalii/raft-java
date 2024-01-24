package raft.utils;

import java.util.Comparator;

import raft.dto.LogId;

public final class LogUtils {
	private LogUtils() {
		// Utility classes should not be instantiated
	}

	/**
	 * <b>Exempt from Raft paper</b>:
	 * 	Raft determines which of two logs is more up-to-date by comparing the index and term of the
	 * 	last entries in the logs. If the logs have last entries with different terms, then the log with the latter
	 * 	term is more up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date.
	 */
	public static final Comparator<LogId> LOG_ID_COMPARATOR = (o1, o2) -> {
		if (o1 == null && o2 == null) {
			return 0;
		}
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}
		if (o1.getTerm() != o2.getTerm()) {
			return o1.getTerm() - o2.getTerm();
		}
		return o1.getIndex() - o2.getIndex();
	};

	public static boolean isOtherLogAtLeastAsNew(LogId theLogLastEntry, LogId currentLogLastEntry) {
		return LOG_ID_COMPARATOR.compare(theLogLastEntry, currentLogLastEntry) >= 0;
	}

	public static LogId max(LogId first, LogId second) {
		return LOG_ID_COMPARATOR.compare(first, second) >= 0 ? first : second;
	}

}
