package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterConfig;
import raft.dto.AppendLogReply;
import raft.dto.Log;
import raft.message.AddLog;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.state_machine.leader.services.LogAppender;
import raft.state_machine.leader.services.LogIdGenerator;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

public class LeaderAddLogMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAddLogMessageHandler.class);

	private final LogStorage logStorage;
	private final ElectionState electionState;
	private final LogAppender logAppender;
	private final LogIdGenerator logIdGenerator;
	private final ClusterConfig clusterConfig;
	private final ServersReplicationState serversReplicationState;

	public LeaderAddLogMessageHandler(
			LogStorage logStorage,
			ElectionState electionState,
			LogAppender logAppender,
			LogIdGenerator logIdGenerator,
			ClusterConfig clusterConfig,
			ServersReplicationState serversReplicationState
	) {
		this.logStorage = logStorage;
		this.electionState = electionState;
		this.logAppender = logAppender;
		this.logIdGenerator = logIdGenerator;
		this.clusterConfig = clusterConfig;
		this.serversReplicationState = serversReplicationState;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var addLog = (AddLog) message;
		var logRequest = addLog.appendLogRequest();
		LOGGER.info("Writing the log in memory {}", addLog.appendLogRequest().getMessage());
		try {
			var lastLogId = logStorage.getLastLogId().orElse(null);
			var lastLog = lastLogId != null ? logStorage.getById(lastLogId) : null;
			var nextLog = Log.newBuilder().setId(logIdGenerator.getNextLogId()).setMsg(logRequest.getMessage()).build();
			int currentServerId = clusterConfig.getCurrentServerId();
			logStorage.addToEnd(nextLog);
			LOGGER.info("Added new log to in memory data structure {}", nextLog);
			int currentTerm = electionState.getCurrentTerm();
			for (var serverId : clusterConfig.getOtherServerIds()) {
				if (serversReplicationState.isCurrentlyReplicating(serverId)) {
					LOGGER.info("Server {} is currently replicating some logs. This log will be written to it eventually", serverId);
				} else {
					LOGGER.info("Will immediately replicate the log to {} and start its replication", serverId);
					serversReplicationState.startReplication(serverId);
					logAppender.appendLog(serverId, nextLog, lastLog);
				}
			}
			LOGGER.info("Sending reply to RPC!");
			addLog.replyConsumer()
					.accept(AppendLogReply.newBuilder().setSuccess(true).setTerm(currentTerm).setLeaderId(currentServerId).build());
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

}
