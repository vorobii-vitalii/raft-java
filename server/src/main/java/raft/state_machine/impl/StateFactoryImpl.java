package raft.state_machine.impl;

import java.util.Map;
import java.util.function.Supplier;

import raft.domain.NodeState;
import raft.state_machine.StateFactory;

public class StateFactoryImpl implements StateFactory {
	private final Map<NodeState, Supplier<State>> stateCreatorByNodeState;

	public StateFactoryImpl(Map<NodeState, Supplier<State>> stateCreatorByNodeState) {
		this.stateCreatorByNodeState = stateCreatorByNodeState;
	}

	@Override
	public State createState(NodeState nodeState) {
		return stateCreatorByNodeState.get(nodeState).get();
	}
}
