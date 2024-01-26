package raft.storage.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;

import raft.serialization.Deserializer;
import raft.serialization.Serializer;
import raft.storage.IndexFile;

/**
 * Index file implementation
 * @param <ID> id
 */
@NotThreadSafe
public class IndexFileImpl<ID> implements IndexFile<ID> {
	private static final int OFFSET_BYTES = 8;
	private static final int ID_BYTES = 4;

	private final Serializer<ID> serializer;
	private final Deserializer<ID> deserializer;
	private final RandomAccessFile indexFile;

	public IndexFileImpl(Serializer<ID> serializer, Deserializer<ID> deserializer, RandomAccessFile indexFile) {
		this.serializer = serializer;
		this.deserializer = deserializer;
		this.indexFile = indexFile;
	}

	@Override
	public void addLast(ID objectID, long offset) throws IOException {
		var idInBytes = serializer.serialize(objectID);
		seekFileToEnd();
		indexFile.writeInt(idInBytes.length);
		indexFile.write(idInBytes);
		indexFile.writeLong(offset);
	}

	@Override
	public Optional<Long> keepUntil(ID objectID) throws IOException {
		var idInBytes = serializer.serialize(objectID);
		long fileIndex = indexFile.length();
		seekFileToBeginning();
		for (var currentIndex = 0; currentIndex < fileIndex;) {
			int lengthOfId = indexFile.readInt();
			if (lengthOfId != idInBytes.length) {
				// Skip id + offset
				indexFile.skipBytes(lengthOfId + OFFSET_BYTES);
				currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
			} else {
				var id = new byte[lengthOfId];
				indexFile.readFully(id);
				if (Arrays.equals(id, idInBytes)) {
					var offsetFile = indexFile.readLong();
					currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
					indexFile.setLength(currentIndex);
					return Optional.of(offsetFile);
				} else {
					// Skip id + offset
					indexFile.skipBytes(OFFSET_BYTES);
					currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
				}
			}
		}
		return Optional.empty();
	}

	@Override
	public void clear() throws IOException {
		indexFile.setLength(0);
	}

	@Override
	public Optional<Long> getOffsetById(ID objectID) throws IOException {
		var idInBytes = serializer.serialize(objectID);
		long fileIndex = indexFile.length();
		seekFileToBeginning();
		for (var currentIndex = 0; currentIndex < fileIndex;) {
			int lengthOfId = indexFile.readInt();
			if (lengthOfId != idInBytes.length) {
				// Skip id + offset
				indexFile.skipBytes(lengthOfId + OFFSET_BYTES);
				currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
			} else {
				var id = new byte[lengthOfId];
				indexFile.readFully(id);
				if (Arrays.equals(id, idInBytes)) {
					return Optional.of(indexFile.readLong());
				} else {
					// Skip id + offset
					indexFile.skipBytes(OFFSET_BYTES);
					currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
				}
			}
		}
		return Optional.empty();
	}

	@Override
	public Optional<ID> findPreviousLogId(ID objectID) throws IOException {
		var idInBytes = serializer.serialize(objectID);
		long fileIndex = indexFile.length();
		seekFileToBeginning();
		byte[] prevId = null;
		for (var currentIndex = 0; currentIndex < fileIndex;) {
			int lengthOfId = indexFile.readInt();
			var id = new byte[lengthOfId];
			indexFile.readFully(id);
			if (lengthOfId != idInBytes.length || !Arrays.equals(id, idInBytes)) {
				// Skip id + offset
				indexFile.skipBytes(OFFSET_BYTES);
				currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
				prevId = id;
			} else {
				return Optional.ofNullable(prevId).map(deserializer::deserialize);
			}

		}
		throw new IllegalArgumentException("Object id " + objectID + " should have existed!");
	}

	@Override
	public Optional<ID> findNextLogId(ID objectID) throws IOException {
		var idInBytes = serializer.serialize(objectID);
		long fileIndex = indexFile.length();
		seekFileToBeginning();
		for (var currentIndex = 0; currentIndex < fileIndex;) {
			int lengthOfId = indexFile.readInt();
			var id = new byte[lengthOfId];
			indexFile.readFully(id);
			if (lengthOfId != idInBytes.length || !Arrays.equals(id, idInBytes)) {
				// Skip id + offset
				indexFile.skipBytes(OFFSET_BYTES);
				currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
			} else {
				currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
				indexFile.skipBytes(OFFSET_BYTES);
				if (currentIndex < fileIndex) {
					int lengthOfNextId = indexFile.readInt();
					var nextId = new byte[lengthOfNextId];
					indexFile.readFully(nextId);
					return Optional.of(nextId).map(deserializer::deserialize);
				}
				return Optional.empty();
			}
		}
		throw new IllegalArgumentException("Object id " + objectID + " should have existed!");
	}

	// Optimize later
	@Override
	public Optional<ID> findLastLog() throws IOException {
		long fileIndex = indexFile.length();
		seekFileToBeginning();
		byte[] prevId = null;
		for (var currentIndex = 0; currentIndex < fileIndex;) {
			int lengthOfId = indexFile.readInt();
			var id = new byte[lengthOfId];
			indexFile.readFully(id);
			// Skip id + offset
			indexFile.skipBytes(OFFSET_BYTES);
			currentIndex += ID_BYTES + lengthOfId + OFFSET_BYTES;
			prevId = id;
		}
		return Optional.ofNullable(prevId).map(deserializer::deserialize);
	}

	private void seekFileToEnd() throws IOException {
		indexFile.seek(indexFile.length());
	}

	private void seekFileToBeginning() throws IOException {
		indexFile.seek(0);
	}

}
