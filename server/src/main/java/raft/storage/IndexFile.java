package raft.storage;

import java.io.IOException;
import java.util.Optional;

public interface IndexFile<ID> {
	void addLast(ID objectID, long offset) throws IOException;
	Optional<Long> keepUntil(ID objectID) throws IOException;
	void clear() throws IOException;
	Optional<Long> getOffsetById(ID objectID) throws IOException;
	Optional<ID> findPreviousLogId(ID objectID) throws IOException;
	Optional<ID> findNextLogId(ID objectID) throws IOException;
	Optional<ID> findLastLog() throws IOException;
}
