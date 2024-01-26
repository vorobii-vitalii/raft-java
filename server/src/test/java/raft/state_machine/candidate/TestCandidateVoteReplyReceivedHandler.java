package raft.state_machine.candidate;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.domain.NodeState;
import raft.dto.RequestVoteReply;
import raft.message.RequestVoteReplyReceived;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.candidate.domain.ElectionStats;
import raft.state_machine.candidate.domain.ElectionStatus;

class TestCandidateVoteReplyReceivedHandler {
	private static final int TERM_ID = 123;
	private static final int SERVER_ID = 1942;

	ElectionStats electionState = Mockito.mock(ElectionStats.class);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	CandidateVoteReplyReceivedHandler candidateVoteReplyReceivedHandler = new CandidateVoteReplyReceivedHandler(electionState);

	@Test
	void givenVoteWasGrantedAndElectionStillNotWon() {
		var requestVoteReplyReceived = new RequestVoteReplyReceived(
				RequestVoteReply.newBuilder().setVoteGranted(true).build(),
				TERM_ID,
				SERVER_ID
		);
		when(electionState.getStatus()).thenReturn(ElectionStatus.NOT_DECIDED);
		candidateVoteReplyReceivedHandler.process(requestVoteReplyReceived, messageHandler);
		verify(electionState).receiveVote(SERVER_ID);
		verifyNoInteractions(messageHandler);
	}

	@Test
	void givenVoteWasGrantedAndElectionWon() {
		var requestVoteReplyReceived = new RequestVoteReplyReceived(
				RequestVoteReply.newBuilder().setVoteGranted(true).build(),
				TERM_ID,
				SERVER_ID
		);
		when(electionState.getStatus()).thenReturn(ElectionStatus.WON);
		candidateVoteReplyReceivedHandler.process(requestVoteReplyReceived, messageHandler);
		verify(electionState).receiveVote(SERVER_ID);
		verify(messageHandler).changeState(NodeState.LEADER);
	}

	@Test
	void givenVoteNotGrantedAndTermOfOtherServerIsHigher() {
		var requestVoteReplyReceived = new RequestVoteReplyReceived(
				RequestVoteReply.newBuilder().setVoteGranted(false).setTerm(TERM_ID + 1).build(),
				TERM_ID,
				SERVER_ID
		);
		candidateVoteReplyReceivedHandler.process(requestVoteReplyReceived, messageHandler);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
	}

	@Test
	void givenVoteNotGrantedAndTermOfOtherServerIsSameAndElectionStillNotLost() {
		var requestVoteReplyReceived = new RequestVoteReplyReceived(
				RequestVoteReply.newBuilder().setVoteGranted(false).setTerm(TERM_ID).build(),
				TERM_ID,
				SERVER_ID
		);
		when(electionState.getStatus()).thenReturn(ElectionStatus.NOT_DECIDED);
		candidateVoteReplyReceivedHandler.process(requestVoteReplyReceived, messageHandler);
		verify(electionState).receiveRejection(SERVER_ID);
		verifyNoInteractions(messageHandler);
	}

	@Test
	void givenVoteNotGrantedAndTermOfOtherServerIsSameAndElectionLost() {
		var requestVoteReplyReceived = new RequestVoteReplyReceived(
				RequestVoteReply.newBuilder().setVoteGranted(false).setTerm(TERM_ID).build(),
				TERM_ID,
				SERVER_ID
		);
		when(electionState.getStatus()).thenReturn(ElectionStatus.LOST);
		candidateVoteReplyReceivedHandler.process(requestVoteReplyReceived, messageHandler);
		verify(electionState).receiveRejection(SERVER_ID);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
	}

}
