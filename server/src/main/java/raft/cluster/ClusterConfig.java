package raft.cluster;

import io.grpc.Channel;

public interface ClusterConfig {
	Channel getChannelByServerId(int serverId);
}
