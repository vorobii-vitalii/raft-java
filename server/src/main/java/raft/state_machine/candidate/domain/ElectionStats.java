package raft.state_machine.candidate.domain;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import raft.utils.RaftUtils;

public class ElectionStats {
	private final Set<Integer> votedForMe = Collections.synchronizedSet(new HashSet<>());
	private final Set<Integer> notVotedForMe = Collections.synchronizedSet(new HashSet<>());
	private final Set<Integer> allServerIds;

	public ElectionStats(Set<Integer> allServerIds) {
		this.allServerIds = allServerIds;
	}

	public void receiveVote(int serverId) {
		votedForMe.add(serverId);
	}

	public void receiveRejection(int serverId) {
		notVotedForMe.add(serverId);
	}

	public ElectionStatus getStatus() {
		if (RaftUtils.isQuorum(allServerIds, votedForMe)) {
			return ElectionStatus.WON;
		} else if (RaftUtils.isQuorum(allServerIds, notVotedForMe)) {
			return ElectionStatus.LOST;
		}
		return ElectionStatus.NOT_DECIDED;
	}

}
