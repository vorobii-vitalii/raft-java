package raft.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestLogFileImpl {
	private static final String FILE_NAME = "file.txt";
	private static final String RW_MODE = "rw";

	@Test
	void append(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RW_MODE)) {
			var logFile = new LogFileImpl(file);
			logFile.append(new byte[] {1, 2, 3, 4, 5});
			logFile.append(new byte[] {6, 7, 8, 9});
			assertThat(Files.readAllBytes(path)).isEqualTo(new byte[] {
					0, 0, 0, 5,
					1, 2, 3, 4, 5,
					0, 0, 0, 4,
					6, 7, 8, 9
			});
		}
	}

	@Test
	void removeFromGivenOffsetPointsToLastLog(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RW_MODE)) {
			var logFile = new LogFileImpl(file);
			logFile.append(new byte[] {1, 2, 3, 4, 5});
			logFile.append(new byte[] {6, 7, 8, 9});
			logFile.removeFrom(4 + 5);
			assertThat(Files.readAllBytes(path)).isEqualTo(new byte[] {
					0, 0, 0, 5,
					1, 2, 3, 4, 5,
					0, 0, 0, 4,
					6, 7, 8, 9
			});
		}
	}

	@Test
	void removeFromGivenOffsetPointsToLogInTheMiddle(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RW_MODE)) {
			var logFile = new LogFileImpl(file);
			logFile.append(new byte[] {1, 2, 3, 4, 5});
			logFile.append(new byte[] {6, 7, 8, 9});
			logFile.append(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
			logFile.removeFrom(4 + 5);
			assertThat(Files.readAllBytes(path)).isEqualTo(new byte[] {
					0, 0, 0, 5,
					1, 2, 3, 4, 5,
					0, 0, 0, 4,
					6, 7, 8, 9
			});
		}
	}

	@Test
	void removeFromGivenOffsetPointsToLogInTheBeginning(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RW_MODE)) {
			var logFile = new LogFileImpl(file);
			logFile.append(new byte[] {1, 2, 3, 4, 5});
			logFile.append(new byte[] {6, 7, 8, 9});
			logFile.append(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
			logFile.removeFrom(0);
			assertThat(Files.readAllBytes(path)).isEqualTo(new byte[] {
					0, 0, 0, 5,
					1, 2, 3, 4, 5
			});
		}
	}

	@Test
	void clear(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RW_MODE)) {
			var logFile = new LogFileImpl(file);
			logFile.append(new byte[] {1, 2, 3, 4, 5});
			logFile.append(new byte[] {6, 7, 8, 9});
			logFile.clear();
			assertThat(Files.readAllBytes(path)).isEmpty();
		}
	}

	@Test
	void getLog(@TempDir Path tempDir) throws IOException {
		var path = tempDir.resolve(FILE_NAME);
		try (var file = new RandomAccessFile(path.toFile(), RW_MODE)) {
			var logFile = new LogFileImpl(file);
			logFile.append(new byte[] {1, 2, 3, 4, 5});
			logFile.append(new byte[] {6, 7, 8, 9});
			assertThat(logFile.getLog(0)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
			assertThat(logFile.getLog(9)).isEqualTo(new byte[] {6, 7, 8, 9});
		}
	}
}
