package raft.cluster.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
	public static final String ADDRESS_DELIMITER = ":";

	private final Map<Integer, String> addressByServerId;
	private final ConcurrentHashMap<Integer, Channel> channelByAddressId = new ConcurrentHashMap<>();
	private final int currentServerId;
	private final Set<Integer> otherServerIds;

	public InMemoryClusterConfig(Map<Integer, String> addressByServerId, int currentServerId) {
		this.addressByServerId = addressByServerId;
		this.otherServerIds = addressByServerId.keySet().stream().filter(v -> v != currentServerId).collect(Collectors.toUnmodifiableSet());
		this.currentServerId = currentServerId;
	}

	@Override
	public Channel getChannelByServerId(int serverId) {
		return channelByAddressId.computeIfAbsent(
				serverId,
				v -> {
					var serverAddress = addressByServerId.get(serverId).trim();
					var host = serverAddress.split(ADDRESS_DELIMITER)[0];
					var port = Integer.parseInt(serverAddress.split(ADDRESS_DELIMITER)[1]);
					LOGGER.info("Creating channel for server {}. Address = {}", serverId, serverAddress);
					return Grpc.newChannelBuilderForAddress(host, port, InsecureChannelCredentials.create())
							.enableRetry()
							.build();
				});
	}

	@Override
	public Set<Integer> getOtherServerIds() {
		return otherServerIds;
	}

	@Override
	public int getCurrentServerId() {
		return currentServerId;
	}
}
