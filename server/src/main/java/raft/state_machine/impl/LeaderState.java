package raft.state_machine.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterRPCService;
import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.AppendLogReply;
import raft.dto.Log;
import raft.dto.LogId;
import raft.dto.RequestVoteReply;
import raft.message.AddLog;
import raft.message.AppendEntriesErrorMessage;
import raft.message.AppendEntriesReplyMessage;
import raft.message.AppendEntriesRequestMessage;
import raft.message.RaftMessage;
import raft.message.RequestVoteErrorReceived;
import raft.message.RequestVoteReplyReceived;
import raft.message.RequestVoteRequestMessage;
import raft.message.SendHeartBeat;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.scheduling.TimedMessageSender;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import raft.utils.LogUtils;
import reactor.core.CoreSubscriber;

/**
 * Leader implementation
 */
@NotThreadSafe
public class LeaderState extends AbstractState {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderState.class);

	private final LogStorage logStorage;
	private final TimedMessageSender<RaftMessage> timedMessageSender;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;
	private final ElectionState electionState;
	private final ClusterRPCService clusterRPCService;
	private final Set<Integer> allServerIds;
	private final int heartBeatsInterval;
	private final int currentServerId;

	private CancellableTask heartBeatTask;
	private final Map<Integer, LogId> previousLogIdByServerId = new HashMap<>();
	private final Map<Integer, LogId> maxReplicatedLogByServerId = new HashMap<>();

	public LeaderState(
			LogStorage logStorage,
			TimedMessageSender<RaftMessage> timedMessageSender,
			int heartBeatsInterval,
			MessagePublisher<RaftMessage> raftMessagePublisher,
			ElectionState electionState,
			ClusterRPCService clusterRPCService,
			Set<Integer> allServerIds,
			int currentServerId
	) {
		this.logStorage = logStorage;
		this.timedMessageSender = timedMessageSender;
		this.heartBeatsInterval = heartBeatsInterval;
		this.raftMessagePublisher = raftMessagePublisher;
		this.electionState = electionState;
		this.clusterRPCService = clusterRPCService;
		this.allServerIds = allServerIds;
		this.currentServerId = currentServerId;
	}

	@Override
	public void onRequestVote(RequestVoteRequestMessage requestVoteRequestMessage) {
		try {
			var currentTerm = electionState.getCurrentTerm();
			var requestVote = requestVoteRequestMessage.requestVote();
			var replyConsumer = requestVoteRequestMessage.replyConsumer();
			int candidateTerm = requestVote.getCandidateTerm();
			if (candidateTerm < currentTerm) {
				LOGGER.warn("Candidate term {} lower than current term {}", candidateTerm, currentTerm);
				replyConsumer.accept(RequestVoteReply.newBuilder().setTerm(currentTerm).setVoteGranted(false).build());
			} else {
				if (requestVote.getCandidateTerm() > currentTerm) {
					LOGGER.info("Candidate term higher ({} > {})! It means I am not leader 😩", requestVote.getCandidateTerm(), currentTerm);
					LOGGER.info("Becoming follower...");
					this.changeState(NodeState.FOLLOWER);
					LOGGER.info("Reprocessing message!");
					this.reprocessMessage(requestVoteRequestMessage);
				} else {
					LOGGER.info("Election on term = {} was won by me. Rejecting request...", currentTerm);
					replyConsumer.accept(RequestVoteReply.newBuilder().setTerm(currentTerm).setVoteGranted(false).build());
				}
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	@Override
	public void onRequestVoteReply(RequestVoteReplyReceived requestVoteReplyReceived) {
		LOGGER.info("I was already elected, ignored request vote reply {}", requestVoteReplyReceived);
	}

	@Override
	public void onRequestVoteErrorReceived(RequestVoteErrorReceived requestVoteErrorReceived) {
		LOGGER.info("I was already elected, ignored request vote error {}", requestVoteErrorReceived);
	}

	@Override
	public void onSendHeartBeat() {
		LOGGER.info("Sending heartbeats to other nodes");
		try {
			var lastAppliedLogIdMaster = logStorage.getLastAppliedLog().orElse(null);
			for (var serverId : allServerIds) {
				if (serverId == currentServerId) {
					continue;
				}
				var prevLogId = previousLogIdByServerId.get(serverId);
				var prevLog = prevLogId != null ? logStorage.getById(prevLogId) : null;
				int currentTerm = electionState.getCurrentTerm();
				LOGGER.info("Sending heartbeat to server {} prevLog = {} term = {}", serverId, prevLog, currentTerm);
				appendLog(
						serverId,
						currentServerId,
						null,
						prevLog,
						lastAppliedLogIdMaster,
						currentTerm
				);
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
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

	@Override
	public void onAddLog(AddLog addLog) {
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

	@Override
	public void releaseResources() {
		try {
			LOGGER.info("Stopping heart beat task...");
			heartBeatTask.cancel();
			LOGGER.info("Removing all uncommitted changes");
			logStorage.removeUncommittedChanges();
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public Map<Integer, LogId> getPreviousLogIdByServerId() {
		return previousLogIdByServerId;
	}

	public Map<Integer, LogId> getMaxReplicatedLogByServerId() {
		return maxReplicatedLogByServerId;
	}

}
