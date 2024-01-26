package raft.cluster;

import java.util.Set;

import io.grpc.Channel;

public interface ClusterConfig {
	Channel getChannelByServerId(int serverId);
	Set<Integer> getOtherServerIds();
	int getCurrentServerId();
}
