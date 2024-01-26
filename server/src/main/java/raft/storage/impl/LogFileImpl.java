package raft.storage.impl;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.storage.LogFile;

public class LogFileImpl implements LogFile {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogFileImpl.class);

	private static final int LOG_LENGTH_BYTES = 4;

	private final RandomAccessFile logFile;

	public LogFileImpl(RandomAccessFile logFile) {
		this.logFile = logFile;
	}

	@Override
	public long append(byte[] serializedLog) throws IOException {
		seekFileToEnd();
		var offset = logFile.length();
		LOGGER.info("Appending log of length = {} file position = {}", serializedLog.length, offset);
		logFile.writeInt(serializedLog.length);
		logFile.write(serializedLog);
		return offset;
	}

	@Override
	public void removeFrom(long offset) throws IOException {
		LOGGER.info("Removing logs from offset = {} (including). length = {}", offset, logFile.length());
		logFile.seek(offset);
		if (offset >= logFile.length()) {
			return;
		}
		var length = logFile.readInt();
		logFile.setLength(offset + LOG_LENGTH_BYTES + length);
	}

	@Override
	public void clear() throws IOException {
		LOGGER.info("Clearing log file completely");
		logFile.seek(0);
		logFile.setLength(0);
	}

	@Override
	public byte[] getLog(long offset) throws IOException {
		LOGGER.info("Reading log, offset = {}", offset);
		logFile.seek(offset);
		var length = logFile.readInt();
		LOGGER.info("Log length = {}", length);
		var log = new byte[length];
		logFile.readFully(log);
		LOGGER.info("Log read.");
		return log;
	}

	private void seekFileToEnd() throws IOException {
		logFile.seek(logFile.length());
	}

}
