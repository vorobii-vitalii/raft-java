package raft.state_machine.candidate;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.domain.NodeState;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import raft.message.RequestVoteRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;

@SuppressWarnings("unchecked")
class TestCandidateRequestVoteHandler {
	private static final int CURRENT_TERM = 123;
	public static final int CANDIDATE_ID = 12;

	ElectionState electionState = Mockito.mock(ElectionState.class);
	CandidateRequestVoteHandler candidateRequestVoteHandler = new CandidateRequestVoteHandler(electionState);
	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);
	Consumer<RequestVoteReply> requestVoteReplyConsumer = Mockito.mock(Consumer.class);

	@Test
	void givenTermOfOtherServerIsLower() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder().setCandidateId(CANDIDATE_ID).setCandidateTerm(CURRENT_TERM - 1).build(),
				requestVoteReplyConsumer
		);
		candidateRequestVoteHandler.process(requestVoteRequestMessage, messageHandler);
		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM).setVoteGranted(false).build());
	}

	@Test
	void givenTermOfOtherServerIsHigher() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder().setCandidateId(CANDIDATE_ID).setCandidateTerm(CURRENT_TERM + 1).build(),
				requestVoteReplyConsumer
		);
		candidateRequestVoteHandler.process(requestVoteRequestMessage, messageHandler);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
		verify(messageHandler).onMessage(requestVoteRequestMessage);
		verifyNoInteractions(requestVoteReplyConsumer);
	}

	@Test
	void givenTermOfOtherServerIsSame() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder().setCandidateId(CANDIDATE_ID).setCandidateTerm(CURRENT_TERM).build(),
				requestVoteReplyConsumer
		);
		candidateRequestVoteHandler.process(requestVoteRequestMessage, messageHandler);
		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM).setVoteGranted(false).build());
	}

}
