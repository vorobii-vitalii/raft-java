package raft.state_machine.leader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.message.AppendEntriesRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;

@SuppressWarnings("unchecked")
class TestLeaderAppendEntriesHandler {

	public static final int CURRENT_TERM = 123;
	ElectionState electionState = mock(ElectionState.class);

	LeaderAppendEntriesHandler leaderAppendEntriesHandler = new LeaderAppendEntriesHandler(electionState);

	Consumer<AppendEntriesReply> consumer = mock(Consumer.class);

	MessageHandler messageHandler = mock(MessageHandler.class);

	@Test
	void givenTermOfOtherServerIsLower() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var message = new AppendEntriesRequestMessage(AppendEntriesRequest.newBuilder().setTerm(CURRENT_TERM - 1).build(), consumer);
		leaderAppendEntriesHandler.process(message, messageHandler);
		verify(consumer).accept(AppendEntriesReply.newBuilder().setSuccess(false).setTerm(CURRENT_TERM).build());
		verifyNoInteractions(messageHandler);
	}

	@Test
	void givenTermOfOtherServerIsHigher() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var message = new AppendEntriesRequestMessage(AppendEntriesRequest.newBuilder().setTerm(CURRENT_TERM + 1).build(), consumer);
		leaderAppendEntriesHandler.process(message, messageHandler);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
		verify(messageHandler).onMessage(message);
	}

}
