package raft.messaging.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import raft.domain.NodeState;
import raft.message.Initialize;
import raft.message.ReleaseResources;
import raft.message.SendHeartBeat;
import raft.state_machine.State;
import raft.state_machine.StateFactory;

class TestMessageHandler {

	State followerState = mock(State.class);
	State candidateState = mock(State.class);

	private StateFactory getStateFactory() {
		var factory = mock(StateFactory.class);
		when(factory.createState(NodeState.CANDIDATE)).thenReturn(candidateState);
		when(factory.createState(NodeState.FOLLOWER)).thenReturn(followerState);
		return factory;
	}

	MessageHandler messageHandler = new MessageHandler(NodeState.FOLLOWER, getStateFactory());

	@Test
	void onMessage() {
		var message = new SendHeartBeat();
		messageHandler.changeState(NodeState.FOLLOWER);
		messageHandler.onMessage(message);
		verify(followerState).onMessage(message, messageHandler);
	}

	@Test
	void changeState() {
		messageHandler.changeState(NodeState.CANDIDATE);
		verify(followerState).onMessage(new ReleaseResources(), messageHandler);
		verify(candidateState).onMessage(new Initialize(), messageHandler);
	}


}