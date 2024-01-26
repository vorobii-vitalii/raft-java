package raft.state_machine.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import raft.domain.NodeState;
import raft.state_machine.State;

class TestStateFactoryImpl {

	State leaderState = mock(State.class);
	State follower = mock(State.class);
	State candidate = mock(State.class);

	Map<NodeState, Supplier<State>> stateCreatorByNodeState = Map.of(
		NodeState.LEADER, () -> leaderState,
		NodeState.CANDIDATE, () -> candidate,
		NodeState.FOLLOWER, () -> follower
	);

	StateFactoryImpl stateFactory = new StateFactoryImpl(stateCreatorByNodeState);

	@Test
	void createState() {
		stateCreatorByNodeState.forEach(((nodeState, stateSupplier) ->
				assertThat(stateFactory.createState(nodeState)).isEqualTo(stateSupplier.get())));
	}
}