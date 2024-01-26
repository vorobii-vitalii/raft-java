package raft.state_machine.candidate;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.message.ReleaseResources;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.state_machine.candidate.domain.CandidateStateData;

class TestCandidateResourceReleaser {

	CandidateStateData candidateStateData = Mockito.mock(CandidateStateData.class);

	CandidateResourceReleaser candidateResourceReleaser = new CandidateResourceReleaser(candidateStateData);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	CancellableTask cancellableTask = Mockito.mock(CancellableTask.class);

	@Test
	void process() {
		when(candidateStateData.getElectionTimeoutTask()).thenReturn(cancellableTask);
		candidateResourceReleaser.process(new ReleaseResources(), messageHandler);
		verify(cancellableTask).cancel();
	}
}