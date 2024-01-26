package raft.state_machine.candidate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.message.AppendEntriesRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;

@SuppressWarnings("unchecked")
class TestCandidateAppendEntriesHandler {
	private static final int CURRENT_TERM = 123;
	private static final int LEADER_ID = 12;

	ElectionState electionState = mock(ElectionState.class);

	CandidateAppendEntriesHandler candidateAppendEntriesHandler = new CandidateAppendEntriesHandler(electionState);

	Consumer<AppendEntriesReply> appendEntriesReplyConsumer = mock(Consumer.class);

	MessageHandler messageHandler = mock(MessageHandler.class);

	@Test
	void givenSenderTermIsLower() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var appendEntriesRequestMessage = new AppendEntriesRequestMessage(
				AppendEntriesRequest.newBuilder()
						.setLeaderId(LEADER_ID)
						.setTerm(CURRENT_TERM - 1)
						.build(),
				appendEntriesReplyConsumer
		);
		candidateAppendEntriesHandler.process(appendEntriesRequestMessage, messageHandler);
	}

	@ValueSource(ints = {CURRENT_TERM, CURRENT_TERM + 1, CURRENT_TERM + 5})
	@ParameterizedTest
	void givenSenderTermIsGreaterOrEqualCurrentTerm(int senderTerm) throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var appendEntriesRequestMessage = new AppendEntriesRequestMessage(
				AppendEntriesRequest.newBuilder()
						.setLeaderId(LEADER_ID)
						.setTerm(senderTerm)
						.build(),
				appendEntriesReplyConsumer
		);
		candidateAppendEntriesHandler.process(appendEntriesRequestMessage, messageHandler);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
		verify(messageHandler).onMessage(appendEntriesRequestMessage);
	}

}
