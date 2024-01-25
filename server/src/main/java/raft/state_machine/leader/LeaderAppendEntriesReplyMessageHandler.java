package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterRPCService;
import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.Log;
import raft.dto.LogId;
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

public class LeaderAppendEntriesReplyMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAppendEntriesHandler.class);

	private final ElectionState electionState;
	private final Map<Integer, LogId> maxReplicatedLogByServerId;
	private final Map<Integer, LogId> previousLogIdByServerId;
	private final Set<Integer> allServerIds;
	private final LogStorage logStorage;
	private final int currentServerId;
	private final ClusterRPCService clusterRPCService;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;

	public LeaderAppendEntriesReplyMessageHandler(ElectionState electionState, Map<Integer, LogId> maxReplicatedLogByServerId,
			Map<Integer, LogId> previousLogIdByServerId, Set<Integer> allServerIds, LogStorage logStorage, int currentServerId,
			ClusterRPCService clusterRPCService, MessagePublisher<RaftMessage> raftMessagePublisher) {
		this.electionState = electionState;
		this.maxReplicatedLogByServerId = maxReplicatedLogByServerId;
		this.previousLogIdByServerId = previousLogIdByServerId;
		this.allServerIds = allServerIds;
		this.logStorage = logStorage;
		this.currentServerId = currentServerId;
		this.clusterRPCService = clusterRPCService;
		this.raftMessagePublisher = raftMessagePublisher;
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
				if (nextLog != null) {
					LOGGER.info("Change {} was successfully applied by {}. Advancing pointer 😀", nextLog, serverId);
					maxReplicatedLogByServerId.put(serverId, nextLog.getId());
					previousLogIdByServerId.put(serverId, nextLog.getId());
					tryToAdvanceCommitIndex();
					var maxAppliedLogId = logStorage.getLastAppliedLog().orElse(null);
					logStorage
							.findFollowing(nextLog.getId())
							.ifPresentOrElse(
									nextLogToAdd -> {
										LOGGER.info("Replicating one more log to server {} ({})", serverId, nextLogToAdd);
										appendLog(
												serverId,
												currentServerId,
												nextLogToAdd,
												nextLog,
												maxAppliedLogId,
												currentTerm
										);
									},
									() -> LOGGER.info("All logs were replicated to {} so far 😃", serverId));
				} else {
					LOGGER.info("Heartbeat was accepted by {} 😀", serverId);
				}
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
					var maxAppliedLogId = logStorage.getLastAppliedLog().orElse(null);
					var newPrevious = logStorage.findPrevious(previousLog.getId()).orElse(null);
					LOGGER.info("Appending another log to {}: prev {} next {}", serverId, newPrevious, previousLog);
					appendLog(
							serverId,
							currentServerId,
							previousLog,
							newPrevious,
							maxAppliedLogId,
							currentTerm
					);
				}
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	private void tryToAdvanceCommitIndex() throws IOException {
		int n = allServerIds.size() / 2;
		LOGGER.info("Trying to advance commit index. Min servers to replicate = {}", n);
		PriorityQueue<LogId> priorityQueue = new PriorityQueue<>(LogUtils.LOG_ID_COMPARATOR);
		maxReplicatedLogByServerId.forEach((serverId, maxLogId) -> {
			if (maxLogId == null) {
				return;
			}
			priorityQueue.add(maxLogId);
			if (priorityQueue.size() > n) {
				priorityQueue.poll();
			}
		});
		if (priorityQueue.isEmpty()) {
			LOGGER.info("No replications 😒... Cannot advance commit index");
		} else {
			LogId replicatedByMajority = priorityQueue.poll();
			LOGGER.info("{} was replicated by quorum!", replicatedByMajority);
			logStorage.applyAllChangesUntil(replicatedByMajority);
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
}
