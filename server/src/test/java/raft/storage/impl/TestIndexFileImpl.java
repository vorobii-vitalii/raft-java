package raft.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import raft.serialization.Deserializer;
import raft.serialization.Serializer;

class TestIndexFileImpl {
	private static final String FILE_NAME = "file";
	private static final String RW_MODE = "rw";

	Serializer<Integer> serializer = value -> new byte[] {
			(byte) (value >>> 24),
			(byte) (value >>> 16),
			(byte) (value >>> 8),
			value.byteValue()
	};

	Deserializer<Integer> deserializer =
			bytes -> (((int) bytes[0]) << 24) | ((int) bytes[1] << 16) | ( (int) bytes[2] << 8) | (int) (bytes[3]);

	@Test
	void addLast(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			assertThat(Files.readAllBytes(file)).isEqualTo(new byte[] {
				0, 0, 0, 4,
					0, 0, 0, 1,
					0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 4,
					0, 0, 0, 2,
					0, 0, 0, 0, 0, 0, 0, 1
			});
		}
	}

	@Test
	void keepUntil(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			indexFile.addLast(3, 4);
			indexFile.keepUntil(2);
			assertThat(Files.readAllBytes(file)).isEqualTo(new byte[] {
					0, 0, 0, 4,
					0, 0, 0, 1,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 4,
					0, 0, 0, 2,
					0, 0, 0, 0, 0, 0, 0, 1
			});
		}
	}

	@Test
	void clear(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			indexFile.addLast(3, 4);
			indexFile.clear();
			assertThat(Files.readAllBytes(file)).isEmpty();
		}
	}

	@Test
	void getOffsetById(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			indexFile.addLast(3, 4);
			assertThat(indexFile.getOffsetById(1)).contains(0L);
			assertThat(indexFile.getOffsetById(2)).contains(1L);
			assertThat(indexFile.getOffsetById(3)).contains(4L);
			assertThat(indexFile.getOffsetById(4)).isEmpty();
		}
	}

	@Test
	void findNextLogId(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			indexFile.addLast(3, 4);
			assertThat(indexFile.findNextLogId(1)).contains(2);
			assertThat(indexFile.findNextLogId(2)).contains(3);
		}
	}

	@Test
	void findPredecessorGivenIdExists(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			indexFile.addLast(3, 4);
			assertThat(indexFile.findPreviousLogId(1)).isEmpty();
			assertThat(indexFile.findPreviousLogId(2)).contains(1);
			assertThat(indexFile.findPreviousLogId(3)).contains(2);
		}
	}


	@Test
	void findPredecessorGivenIdNotExists(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			indexFile.addLast(3, 4);
			assertThrows(IllegalArgumentException.class, () -> indexFile.findPreviousLogId(4));
		}
	}

	@Test
	void findLastLogGivenLogFileNotEmpty(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			indexFile.addLast(1, 0);
			indexFile.addLast(2, 1);
			indexFile.addLast(3, 4);
			assertThat(indexFile.findLastLog()).contains(3);
		}
	}

	@Test
	void findLastLogGivenLogFileEmpty(@TempDir Path tempDir) throws IOException {
		var file = tempDir.resolve(FILE_NAME);
		try (var randomAccessFile = new RandomAccessFile(file.toFile(), RW_MODE)) {
			var indexFile = new IndexFileImpl<>(serializer, deserializer, randomAccessFile);
			assertThat(indexFile.findLastLog()).isEmpty();
		}
	}

}
