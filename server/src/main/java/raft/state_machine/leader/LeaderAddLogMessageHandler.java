package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterRPCService;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.AppendLogReply;
import raft.dto.Log;
import raft.dto.LogId;
import raft.message.AddLog;
import raft.message.AppendEntriesErrorMessage;
import raft.message.AppendEntriesReplyMessage;
import raft.message.RaftMessage;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import raft.utils.LogUtils;
import reactor.core.CoreSubscriber;

public class LeaderAddLogMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAddLogMessageHandler.class);

	private final LogStorage logStorage;
	private final ElectionState electionState;
	private final Set<Integer> allServerIds;
	private final int currentServerId;
	private final Map<Integer, LogId> maxReplicatedLogByServerId;
	private final ClusterRPCService clusterRPCService;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;

	public LeaderAddLogMessageHandler(LogStorage logStorage, ElectionState electionState, Set<Integer> allServerIds, int currentServerId,
			Map<Integer, LogId> maxReplicatedLogByServerId, ClusterRPCService clusterRPCService,
			MessagePublisher<RaftMessage> raftMessagePublisher) {
		this.logStorage = logStorage;
		this.electionState = electionState;
		this.allServerIds = allServerIds;
		this.currentServerId = currentServerId;
		this.maxReplicatedLogByServerId = maxReplicatedLogByServerId;
		this.clusterRPCService = clusterRPCService;
		this.raftMessagePublisher = raftMessagePublisher;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var addLog = (AddLog) message;

		var logRequest = addLog.appendLogRequest();
		LOGGER.info("Writing the log in memory {}", addLog.appendLogRequest().getMessage());
		try {
			var lastLogId = logStorage.getLastLogId().orElse(null);
			var lastLog = lastLogId != null ? logStorage.getById(lastLogId) : null;
			var lastAppliedLog = logStorage.getLastAppliedLog().orElse(null);
			var nextLogId = nextLogId();
			var nextLog = Log.newBuilder().setId(nextLogId).setMsg(logRequest.getMessage()).build();
			logStorage.addToEnd(nextLog);
			LOGGER.info("Added new log to in memory data structure {}", nextLog);
			int currentTerm = electionState.getCurrentTerm();
			for (var serverId : allServerIds) {
				if (serverId == currentServerId) {
					LOGGER.info("Skipping myself");
					continue;
				}
				var maxReplicated = maxReplicatedLogByServerId.get(serverId);
				if (Objects.equals(maxReplicated, lastLogId)) {
					LOGGER.info("Will immediately replicate the log to {}", serverId);
					appendLog(
							serverId,
							currentServerId,
							nextLog,
							lastLog,
							lastAppliedLog,
							currentTerm
					);
				} else {
					LOGGER.info("Server {} is lagging...", serverId);
				}
			}
			LOGGER.info("Sending reply to RPC!");
			addLog.replyConsumer().accept(AppendLogReply.newBuilder()
					.setSuccess(true)
					.setTerm(currentTerm)
					.setLeaderId(currentServerId)
					.build());
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private LogId nextLogId() throws IOException {
		var updatedFromLog = logStorage
				.getLastLogId()
				.map(v -> v.toBuilder().setIndex(v.getIndex() + 1).build())
				.orElse(null);
		var currentTerm = electionState.getCurrentTerm();
		return LogUtils.max(updatedFromLog, LogId.newBuilder().setIndex(0).setTerm(currentTerm).build());
	}

	private void appendLog(
			Integer serverId,
			int currentServerId,
			@Nullable Log nextLog,
			@Nullable Log previousLog,
			@Nullable LogId lastAppliedLog,
			int currentTerm
	) {
		var builder = AppendEntriesRequest.newBuilder()
				.setLeaderId(currentServerId)
				.setTerm(currentTerm)
				.addAllEntries(Optional.ofNullable(nextLog).map(List::of).orElse(Collections.emptyList()));

		Optional.ofNullable(previousLog).map(Log::getId).ifPresent(builder::setPreviousLog);
		if (lastAppliedLog != null) {
			builder.setLeaderCurrentLogId(lastAppliedLog);
		}

		var appendEntriesRequest = builder.build();
		clusterRPCService.appendLog(serverId, appendEntriesRequest)
				.subscribe(new CoreSubscriber<>() {
					@Override
					public void onSubscribe(Subscription subscription) {
						subscription.request(1);
					}

					@Override
					public void onNext(AppendEntriesReply appendEntriesReply) {
						raftMessagePublisher.publish(new AppendEntriesReplyMessage(
								appendEntriesReply,
								nextLog,
								previousLog,
								serverId
						));
					}

					@Override
					public void onError(Throwable error) {
						LOGGER.error("Error occurred on replication to server {}", serverId, error);
						raftMessagePublisher
								.publish(new AppendEntriesErrorMessage(serverId, previousLog, nextLog));
					}

					@Override
					public void onComplete() {

					}
				});
	}

}
