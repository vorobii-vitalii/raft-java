package raft.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.dto.Log;
import raft.dto.LogId;
import raft.storage.IndexFile;
import raft.storage.LogFile;

@SuppressWarnings("unchecked")
class TestFileLogStorage {

	IndexFile<LogId> indexFile = Mockito.mock(IndexFile.class);
	LogFile logFile = Mockito.mock(LogFile.class);

	FileLogStorage storage = new FileLogStorage(indexFile, logFile);

	@Test
	void appendLogGivenLogIdNull() throws IOException {
		assertThat(storage.appendLog(null, List.of(
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
						.setMsg("log 1")
						.build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		))).isTrue();
		assertThat(storage.getUncommittedChanges()).containsExactlyInAnyOrderEntriesOf(Map.of(
				LogId.newBuilder().setTerm(1).setIndex(2).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
						.setMsg("log 1")
						.build(),
				LogId.newBuilder().setTerm(1).setIndex(3).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		));
		verify(indexFile).clear();
		verify(logFile).clear();
	}

	@Test
	void appendLogGivenLogIdAbsent() throws IOException {
		var logId = LogId.newBuilder().setTerm(1).setIndex(2).build();
		when(indexFile.getOffsetById(logId)).thenReturn(Optional.empty());
		assertThat(storage.appendLog(logId, List.of())).isFalse();
		verifyNoInteractions(logFile);
	}

	@Test
	void appendLogGivenLogIdPresentAndThereAreOtherLogsFollowingIt() throws IOException {
		var prevLogId = LogId.newBuilder().setTerm(1).setIndex(1).build();
		when(indexFile.getOffsetById(prevLogId)).thenReturn(Optional.of(0L));
		when(indexFile.keepUntil(prevLogId)).thenReturn(Optional.of(420L));
		assertThat(storage.appendLog(
				prevLogId,
				List.of(
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
								.setMsg("log 1")
								.build(),
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
								.setMsg("log 2")
								.build()
				))
		).isTrue();
		assertThat(storage.getUncommittedChanges()).containsExactlyInAnyOrderEntriesOf(Map.of(
				LogId.newBuilder().setTerm(1).setIndex(2).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
						.setMsg("log 1")
						.build(),
				LogId.newBuilder().setTerm(1).setIndex(3).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		));
		verify(logFile).removeFrom(420L);
	}

	@Test
	void appendLogGivenLogIdPresentAndThereAreNoOtherLogsFollowingIt() throws IOException {
		var prevLogId = LogId.newBuilder().setTerm(1).setIndex(1).build();
		when(indexFile.getOffsetById(prevLogId)).thenReturn(Optional.of(0L));
		when(indexFile.keepUntil(prevLogId)).thenReturn(Optional.empty());
		assertThat(storage.appendLog(
				prevLogId,
				List.of(
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
								.setMsg("log 1")
								.build(),
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
								.setMsg("log 2")
								.build()
				))
		).isTrue();
		assertThat(storage.getUncommittedChanges()).containsExactlyInAnyOrderEntriesOf(Map.of(
				LogId.newBuilder().setTerm(1).setIndex(2).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
						.setMsg("log 1")
						.build(),
				LogId.newBuilder().setTerm(1).setIndex(3).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		));
		verifyNoInteractions(logFile);
	}

	@Test
	void appendLogGivenLogIdPresentAndNoLogsWereRequestedToBeAdded() throws IOException {
		var prevLogId = LogId.newBuilder().setTerm(1).setIndex(1).build();
		when(indexFile.getOffsetById(prevLogId)).thenReturn(Optional.of(0L));
		assertThat(storage.appendLog(prevLogId, List.of())).isTrue();
		verifyNoInteractions(logFile);
	}

	@Test
	void appendLogGivenLogIdPresentInCommittedChanges() throws IOException {
		var prevLogId = LogId.newBuilder().setTerm(1).setIndex(1).build();
		when(indexFile.getOffsetById(prevLogId)).thenReturn(Optional.of(0L));
		when(indexFile.keepUntil(prevLogId)).thenReturn(Optional.empty());

		// Add first log
		assertThat(storage.appendLog(
				prevLogId,
				List.of(
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
								.setMsg("log 1")
								.build()
				))
		).isTrue();

		assertThat(storage.appendLog(
				LogId.newBuilder().setTerm(1).setIndex(2).build(),
				List.of(
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
								.setMsg("log 2")
								.build()
				))
		).isTrue();

		assertThat(storage.getUncommittedChanges()).containsExactlyInAnyOrderEntriesOf(Map.of(
				LogId.newBuilder().setTerm(1).setIndex(2).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
						.setMsg("log 1")
						.build(),
				LogId.newBuilder().setTerm(1).setIndex(3).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		));
		verifyNoInteractions(logFile);
	}

	@Test
	void addToEnd() {
		storage.addToEnd(Log.newBuilder().setId(LogId.newBuilder().setTerm(1).setIndex(2).build()).setMsg("log 1").build());
		storage.addToEnd(Log.newBuilder().setId(LogId.newBuilder().setTerm(1).setIndex(3).build()).setMsg("log 2").build());

		assertThat(storage.getUncommittedChanges()).containsExactlyInAnyOrderEntriesOf(Map.of(
				LogId.newBuilder().setTerm(1).setIndex(2).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
						.setMsg("log 1")
						.build(),
				LogId.newBuilder().setTerm(1).setIndex(3).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		));
	}

	@Test
	void applyAllChangesUntil() throws IOException {
		storage.addToEnd(Log.newBuilder().setId(LogId.newBuilder().setTerm(1).setIndex(2).build()).setMsg("log 1").build());
		storage.addToEnd(Log.newBuilder().setId(LogId.newBuilder().setTerm(1).setIndex(3).build()).setMsg("log 2").build());

		assertThat(storage.getUncommittedChanges()).containsExactlyInAnyOrderEntriesOf(Map.of(
				LogId.newBuilder().setTerm(1).setIndex(2).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
						.setMsg("log 1")
						.build(),
				LogId.newBuilder().setTerm(1).setIndex(3).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		));

		// Applies that don't commit any logs
		storage.applyAllChangesUntil(null);
		storage.applyAllChangesUntil(LogId.newBuilder().setTerm(0).setIndex(12).build());

		// Second apply

		when(logFile.append("log 1".getBytes(StandardCharsets.UTF_8))).thenReturn(123L);

		storage.applyAllChangesUntil(LogId.newBuilder().setTerm(1).setIndex(2).build());
		assertThat(storage.getUncommittedChanges()).containsExactlyInAnyOrderEntriesOf(Map.of(
				LogId.newBuilder().setTerm(1).setIndex(3).build(),
				Log.newBuilder()
						.setId(LogId.newBuilder().setTerm(1).setIndex(3).build())
						.setMsg("log 2")
						.build()
		));
		verify(indexFile).addLast(LogId.newBuilder().setTerm(1).setIndex(2).build(), 123L);

		// Third apply

		when(logFile.append("log 2".getBytes(StandardCharsets.UTF_8))).thenReturn(525L);

		storage.applyAllChangesUntil(LogId.newBuilder().setTerm(1).setIndex(4).build());
		assertThat(storage.getUncommittedChanges()).isEmpty();
		verify(indexFile).addLast(LogId.newBuilder().setTerm(1).setIndex(3).build(), 525L);
	}

	@Test
	void getLastLogIdGivenNoUncommittedChanges() throws IOException {
		when(indexFile.findLastLog()).thenReturn(Optional.of(LogId.newBuilder().setTerm(1).setIndex(2).build()));
		assertThat(storage.getLastLogId()).contains(LogId.newBuilder().setTerm(1).setIndex(2).build());
	}

	@Test
	void getLastLogIdGivenUncommittedChangesPresent() throws IOException {
		storage.addToEnd(Log.newBuilder()
				.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
				.setMsg("log 1")
				.build());
		assertThat(storage.getLastLogId()).contains(LogId.newBuilder().setTerm(1).setIndex(2).build());
		verifyNoInteractions(indexFile);
	}

	@Test
	void getByIdGivenLogPresentInMemory() throws IOException {
		storage.addToEnd(Log.newBuilder()
				.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
				.setMsg("log 1")
				.build());

		assertThat(storage.getById(LogId.newBuilder().setTerm(1).setIndex(2).build()))
				.isEqualTo(
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
								.setMsg("log 1")
								.build()
				);
	}

	@Test
	void getByIdGivenLogPresentInFile() throws IOException {
		when(indexFile.getOffsetById(LogId.newBuilder().setTerm(1).setIndex(2).build()))
				.thenReturn(Optional.of(123L));
		when(logFile.getLog(123L)).thenReturn("log 1".getBytes(StandardCharsets.UTF_8));
		assertThat(storage.getById(LogId.newBuilder().setTerm(1).setIndex(2).build()))
				.isEqualTo(
						Log.newBuilder()
								.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
								.setMsg("log 1")
								.build()
				);
	}

	@Test
	void getByIdGivenLogAbsentEverywhere() throws IOException {
		when(indexFile.getOffsetById(LogId.newBuilder().setTerm(1).setIndex(2).build()))
				.thenReturn(Optional.empty());
		assertThrows(IllegalArgumentException.class, () -> storage.getById(LogId.newBuilder().setTerm(1).setIndex(2).build()));
	}

	@Test
	void removeUncommittedChanges() {
		storage.addToEnd(Log.newBuilder()
				.setId(LogId.newBuilder().setTerm(1).setIndex(2).build())
				.setMsg("log 1")
				.build());
		assertThat(storage.getUncommittedChanges()).isNotEmpty();
		storage.removeUncommittedChanges();
		assertThat(storage.getUncommittedChanges()).isEmpty();
	}

	@Test
	void findPreviousGiveNullLogId() throws IOException {
		assertThat(storage.findPrevious(null)).isEmpty();
	}



}
