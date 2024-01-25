package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterRPCService;
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
import reactor.core.CoreSubscriber;

public class LeaderSendHeartBeatHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderSendHeartBeatHandler.class);

	private final LogStorage logStorage;
	private final Set<Integer> allServerIds;
	private final int currentServerId;
	private final Map<Integer, LogId> previousLogIdByServerId;
	private final ElectionState electionState;
	private final ClusterRPCService clusterRPCService;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;

	public LeaderSendHeartBeatHandler(LogStorage logStorage, Set<Integer> allServerIds, int currentServerId,
			Map<Integer, LogId> previousLogIdByServerId, ElectionState electionState, ClusterRPCService clusterRPCService,
			MessagePublisher<RaftMessage> raftMessagePublisher) {
		this.logStorage = logStorage;
		this.allServerIds = allServerIds;
		this.currentServerId = currentServerId;
		this.previousLogIdByServerId = previousLogIdByServerId;
		this.electionState = electionState;
		this.clusterRPCService = clusterRPCService;
		this.raftMessagePublisher = raftMessagePublisher;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
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
