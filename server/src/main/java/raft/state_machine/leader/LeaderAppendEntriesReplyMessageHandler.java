package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.dto.Log;
import raft.message.AppendEntriesReplyMessage;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.state_machine.leader.services.LogAppender;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

public class LeaderAppendEntriesReplyMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAppendEntriesHandler.class);

	private final ElectionState electionState;
	private final LogStorage logStorage;
	private final ServersReplicationState serversReplicationState;
	private final LogAppender logAppender;

	public LeaderAppendEntriesReplyMessageHandler(
			ElectionState electionState,
			LogStorage logStorage,
			ServersReplicationState serversReplicationState,
			LogAppender logAppender
	) {
		this.electionState = electionState;
		this.logStorage = logStorage;
		this.serversReplicationState = serversReplicationState;
		this.logAppender = logAppender;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var appendEntriesReplyMessage = (AppendEntriesReplyMessage) message;
		var appendEntriesReply = appendEntriesReplyMessage.reply();
		var serverId = appendEntriesReplyMessage.serverId();
		var nextLog = appendEntriesReplyMessage.nextLog();
		var previousLog = appendEntriesReplyMessage.previousLog();
		try {
			int currentTerm = electionState.getCurrentTerm();
			if (appendEntriesReply.getSuccess()) {
				onLogReplicationSuccess(nextLog, serverId);
			} else {
				LOGGER.info("My current term = {}", currentTerm);
				if (appendEntriesReply.getTerm() > currentTerm) {
					LOGGER.info("Looks like another node started new election. His term = {}, mine = {}. "
									+ "Will give up leader role and become follower 😓",
							appendEntriesReply.getTerm(),
							currentTerm);
					messageHandler.changeState(NodeState.FOLLOWER);
				} else {
					LOGGER.info("It must mean server {} doesn't have {} in logs", serverId, nextLog);
					var newPrevious = logStorage.findPrevious(previousLog.getId()).orElse(null);
					LOGGER.info("Appending another log to {}: prev {} next {}", serverId, newPrevious, previousLog);
					logAppender.appendLog(serverId, previousLog, newPrevious);
				}
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	private void onLogReplicationSuccess(Log nextLog, int serverId) throws IOException {
		if (nextLog != null) {
			LOGGER.info("Change {} was successfully applied by {}. Advancing pointer 😀", nextLog, serverId);
			serversReplicationState.updateMaxReplicatedLog(serverId, nextLog.getId());
			tryToAdvanceCommitIndex();
			var following = logStorage.findFollowing(nextLog.getId());
			if (following.isPresent()) {
				var nextLogToAdd = following.get();
				LOGGER.info("Replicating one more log to server {} ({})", serverId, nextLogToAdd);
				logAppender.appendLog(
						serverId,
						nextLogToAdd,
						nextLog
				);
			} else {
				LOGGER.info("All logs were replicated to {} so far 😃. Stopping replication for now 😀", serverId);
				serversReplicationState.stopReplication(serverId);
			}
		} else {
			LOGGER.info("Heartbeat was accepted by {} 😀. Stopping replication for now", serverId);
			serversReplicationState.stopReplication(serverId);
		}
	}

	private void tryToAdvanceCommitIndex() throws IOException {
		var maxReplicatedByQuorumIndex = serversReplicationState.getMaxReplicatedByQuorumIndex();
		LOGGER.info("Max replicated log by quorum = {}", maxReplicatedByQuorumIndex);
		logStorage.applyAllChangesUntil(maxReplicatedByQuorumIndex);
	}
}
