package raft.state_machine.candidate;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.domain.NodeState;
import raft.message.RequestVoteErrorReceived;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.candidate.domain.ElectionStats;
import raft.state_machine.candidate.domain.ElectionStatus;

class TestCandidateRequestVoteErrorReceived {
	private static final int TERM_ID = 12;
	private static final int SERVER_ID = 123;

	ElectionStats electionStats = Mockito.mock(ElectionStats.class);

	CandidateRequestVoteErrorReceived candidateRequestVoteErrorReceived = new CandidateRequestVoteErrorReceived(electionStats);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	@Test
	void processGivenElectionAfterTheRejectionIsLost() {
		when(electionStats.getStatus()).thenReturn(ElectionStatus.LOST);
		var requestVoteErrorReceived = new RequestVoteErrorReceived(TERM_ID, SERVER_ID);
		candidateRequestVoteErrorReceived.process(requestVoteErrorReceived, messageHandler);
		verify(electionStats).receiveRejection(SERVER_ID);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
	}

	@Test
	void processGivenElectionResultAfterTheRejectionIsStillNotDecided() {
		when(electionStats.getStatus()).thenReturn(ElectionStatus.NOT_DECIDED);
		var requestVoteErrorReceived = new RequestVoteErrorReceived(TERM_ID, SERVER_ID);
		candidateRequestVoteErrorReceived.process(requestVoteErrorReceived, messageHandler);
		verify(electionStats).receiveRejection(SERVER_ID);
		verifyNoInteractions(messageHandler);
	}

}
