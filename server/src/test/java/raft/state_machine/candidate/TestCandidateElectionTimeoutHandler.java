package raft.state_machine.candidate;

import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.domain.NodeState;
import raft.message.ElectionTimeout;
import raft.messaging.impl.MessageHandler;

class TestCandidateElectionTimeoutHandler {

	CandidateElectionTimeoutHandler candidateElectionTimeoutHandler = new CandidateElectionTimeoutHandler();

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	@Test
	void expectStateChangedToFollowerIfElectionTimedOut() {
		candidateElectionTimeoutHandler.process(new ElectionTimeout(), messageHandler);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
	}
}