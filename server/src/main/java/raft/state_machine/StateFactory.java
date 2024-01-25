package raft.state_machine;

import raft.domain.NodeState;

public interface StateFactory {
	State createState(NodeState nodeState);
}
