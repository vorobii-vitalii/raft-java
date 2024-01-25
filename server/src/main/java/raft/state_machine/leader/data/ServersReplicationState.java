package raft.state_machine.leader.data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import raft.cluster.ClusterConfig;
import raft.dto.LogId;

@ThreadSafe
public class ServersReplicationState {
	private final Set<Integer> currentlyReplicating = Collections.synchronizedSet(new HashSet<>());
	private final Map<Integer, LogId> previousLogIdByServerId = new ConcurrentHashMap<>();
	private final Map<Integer, LogId> maxReplicatedLogByServerId = new ConcurrentHashMap<>();

	private final ClusterConfig clusterConfig;

	public ServersReplicationState(ClusterConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
	}

	public void initializePreviousLogTable(@Nullable LogId maxCommitedLogId) {
		if (maxCommitedLogId != null) {
			for (var serverId : clusterConfig.getOtherServerIds()) {
				previousLogIdByServerId.put(serverId, maxCommitedLogId);
			}
		}
	}

	public boolean isCurrentlyReplicating(int serverId) {
		return currentlyReplicating.contains(serverId);
	}

	public void stopReplication(int serverId) {
		currentlyReplicating.remove(serverId);
	}

	public void startReplication(int serverId) {
		currentlyReplicating.add(serverId);
	}

}
