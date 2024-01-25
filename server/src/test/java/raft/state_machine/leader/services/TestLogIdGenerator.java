package raft.state_machine.leader.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.dto.LogId;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

class TestLogIdGenerator {
	private static final int CURRENT_TERM = 10;

	LogStorage logStorage = Mockito.mock(LogStorage.class);
	ElectionState electionState = Mockito.mock(ElectionState.class);

	LogIdGenerator logIdGenerator = new LogIdGenerator(logStorage, electionState);

	@Test
	void getNextLogIdGivenLogsAreEmpty() throws IOException {
		when(logStorage.getLastLogId()).thenReturn(Optional.empty());
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		assertThat(logIdGenerator.getNextLogId()).isEqualTo(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build());
	}

	@Test
	void getNextLogIdGivenLastLogHasLowerTerm() throws IOException {
		when(logStorage.getLastLogId()).thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM - 1).setIndex(22).build()));
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		assertThat(logIdGenerator.getNextLogId()).isEqualTo(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build());
	}

	@Test
	void getNextLogIdGivenLastLogHasSameTerm() throws IOException {
		when(logStorage.getLastLogId()).thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(22).build()));
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		assertThat(logIdGenerator.getNextLogId()).isEqualTo(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(23).build());
	}

}
