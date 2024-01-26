package raft.state_machine.leader;

import static org.junit.jupiter.api.Assertions.*;
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
import raft.message.RequestVoteReplyReceived;
import raft.message.RequestVoteRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;

@SuppressWarnings("unchecked")
class TestLeaderRequestVoteRequestMessageHandler {

	public static final int CURRENT_TERM = 12;
	public static final int CANDIDATE_ID = 9;
	ElectionState electionState = Mockito.mock(ElectionState.class);

	LeaderRequestVoteRequestMessageHandler leaderRequestVoteRequestMessageHandler = new LeaderRequestVoteRequestMessageHandler(electionState);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	Consumer<RequestVoteReply> replyConsumer = Mockito.mock(Consumer.class);

	@Test
	void givenTermOfCallerIsLower() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var message = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateTerm(CURRENT_TERM - 1)
						.setCandidateId(CANDIDATE_ID)
						.build(),
				replyConsumer
		);
		leaderRequestVoteRequestMessageHandler.process(message, messageHandler);
		verify(replyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM).setVoteGranted(false).build());
		verifyNoInteractions(messageHandler);
	}

	@Test
	void givenTermOfCallerIsSame() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var message = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateTerm(CURRENT_TERM)
						.setCandidateId(CANDIDATE_ID)
						.build(),
				replyConsumer
		);
		leaderRequestVoteRequestMessageHandler.process(message, messageHandler);
		verify(replyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM).setVoteGranted(false).build());
		verifyNoInteractions(messageHandler);
	}

	@Test
	void givenTermOfCallersIsHigher() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var message = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateTerm(CURRENT_TERM + 1)
						.setCandidateId(CANDIDATE_ID)
						.build(),
				replyConsumer
		);
		leaderRequestVoteRequestMessageHandler.process(message, messageHandler);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
		verify(messageHandler).onMessage(message);
	}

}
