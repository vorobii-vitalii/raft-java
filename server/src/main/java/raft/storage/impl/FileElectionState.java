package raft.storage.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;

import raft.storage.ElectionState;

public class FileElectionState implements ElectionState {
	private static final int INT_BYTES = 4;
	private static final int DEFAULT_TERM = 0;
	public static final int NOT_VOTED = 0;

	private final RandomAccessFile randomAccessFile;

	public FileElectionState(RandomAccessFile randomAccessFile) {
		this.randomAccessFile = randomAccessFile;
	}

	@Override
	public void updateTerm(int term) throws IOException {
		randomAccessFile.setLength(0);
		randomAccessFile.writeInt(term);
		randomAccessFile.writeInt(NOT_VOTED);
	}

	@Override
	public void voteFor(int serverId) throws IOException {
		seekBeginning();
		if (randomAccessFile.length() > 0) {
			randomAccessFile.seek(INT_BYTES);
			randomAccessFile.writeInt(serverId);
		} else {
			randomAccessFile.writeInt(DEFAULT_TERM);
			randomAccessFile.writeInt(serverId);
		}
	}

	@Override
	public int getCurrentTerm() throws IOException {
		seekBeginning();
		if (randomAccessFile.length() > 0) {
			return randomAccessFile.readInt();
		}
		return DEFAULT_TERM;
	}

	@Override
	public Optional<Integer> getVotedForInCurrentTerm() throws IOException {
		seekBeginning();
		if (randomAccessFile.length() > 0) {
			randomAccessFile.seek(INT_BYTES);
			return Optional.of(randomAccessFile.readInt()).filter(v -> v != NOT_VOTED);
		}
		return Optional.empty();
	}

	private void seekBeginning() throws IOException {
		randomAccessFile.seek(0);
	}

}
