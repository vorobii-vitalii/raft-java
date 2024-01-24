package raft.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileElectionState {
	private static final String FILE_NAME = "file.txt";
	private static final String RWS_MODE = "rws";

	@Test
	void updateTermGivenFileEmpty(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RWS_MODE)) {
			var state = new FileElectionState(file);
			state.updateTerm(2);
			assertThat(state.getCurrentTerm()).isEqualTo(2);
			assertThat(state.getVotedForInCurrentTerm()).isEmpty();
		}
	}

	@Test
	void updateTermGivenFileNotEmpty(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RWS_MODE)) {
			var state = new FileElectionState(file);
			state.updateTerm(2);
			state.voteFor(4);
			state.updateTerm(3);
			assertThat(state.getCurrentTerm()).isEqualTo(3);
			assertThat(state.getVotedForInCurrentTerm()).isEmpty();
		}
	}

	@Test
	void voteFor(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RWS_MODE)) {
			var state = new FileElectionState(file);
			state.updateTerm(2);
			state.voteFor(4);
			assertThat(state.getCurrentTerm()).isEqualTo(2);
			assertThat(state.getVotedForInCurrentTerm()).contains(4);
		}
	}

	@Test
	void getCurrentTerm(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RWS_MODE)) {
			var state = new FileElectionState(file);
			assertThat(state.getCurrentTerm()).isEqualTo(0);
			state.updateTerm(2);
			assertThat(state.getCurrentTerm()).isEqualTo(2);
		}
	}

	@Test
	void getVotedForInCurrentTerm(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RWS_MODE)) {
			var state = new FileElectionState(file);
			assertThat(state.getCurrentTerm()).isEqualTo(0);
			assertThat(state.getVotedForInCurrentTerm()).isEmpty();
			state.voteFor(1);
			assertThat(state.getCurrentTerm()).isEqualTo(0);
			assertThat(state.getVotedForInCurrentTerm()).contains(1);
			state.updateTerm(2);
			assertThat(state.getCurrentTerm()).isEqualTo(2);
			assertThat(state.getVotedForInCurrentTerm()).isEmpty();
			state.voteFor(4);
			assertThat(state.getCurrentTerm()).isEqualTo(2);
			assertThat(state.getVotedForInCurrentTerm()).contains(4);
		}
	}
}
