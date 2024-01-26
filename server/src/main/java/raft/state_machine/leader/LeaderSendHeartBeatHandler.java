package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterConfig;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.state_machine.leader.services.LogAppender;
import raft.storage.LogStorage;

public class LeaderSendHeartBeatHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderSendHeartBeatHandler.class);

	private final LogStorage logStorage;
	private final ServersReplicationState serversReplicationState;
	private final LogAppender logAppender;
	private final ClusterConfig clusterConfig;

	public LeaderSendHeartBeatHandler(LogStorage logStorage, ServersReplicationState serversReplicationState,
			LogAppender logAppender, ClusterConfig clusterConfig) {
		this.logStorage = logStorage;
		this.serversReplicationState = serversReplicationState;
		this.logAppender = logAppender;
		this.clusterConfig = clusterConfig;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		LOGGER.info("Sending heartbeats to other nodes");
		try {
			var lastLogId = logStorage.getLastLogId().orElse(null);
			var lastLog = lastLogId != null ? logStorage.getById(lastLogId) : null;
			for (var serverId : clusterConfig.getOtherServerIds()) {
				if (serversReplicationState.isCurrentlyReplicating(serverId)) {
					LOGGER.info("Some logs are already being replicated to server {}. No sense to send heart beat", serverId);
				} else {
					LOGGER.info("Sending heart beat to {} and starting its replication", serverId);
					serversReplicationState.startReplication(serverId);
					logAppender.appendLog(serverId, null, lastLog);
				}
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

}
