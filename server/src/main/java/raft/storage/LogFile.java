package raft.storage;

import java.io.IOException;

public interface LogFile {
	long append(byte[] serializedLog) throws IOException;
	void removeFrom(long offset) throws IOException;
	void clear() throws IOException;
	byte[] getLog(long offset) throws IOException;
}
