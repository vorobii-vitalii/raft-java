package raft.cluster.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import raft.cluster.ClusterConfig;

@ThreadSafe
public class InMemoryClusterConfig implements ClusterConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryClusterConfig.class);

	private final Map<Integer, String> addressByServerId;
	private final ConcurrentHashMap<Integer, Channel> channelByAddressId = new ConcurrentHashMap<>();

	public InMemoryClusterConfig(Map<Integer, String> addressByServerId) {
		this.addressByServerId = addressByServerId;
	}

	@Override
	public Channel getChannelByServerId(int serverId) {
		return channelByAddressId.computeIfAbsent(
				serverId,
				v -> {
					var serverAddress = addressByServerId.get(serverId).trim();
					var host = serverAddress.split(":")[0];
					var port = Integer.parseInt(serverAddress.split(":")[1]);
					LOGGER.info("Creating channel for server {}. Address = {}", serverId, serverAddress);
					return Grpc.newChannelBuilderForAddress(host, port, InsecureChannelCredentials.create())
							.build();
				});
	}
}
