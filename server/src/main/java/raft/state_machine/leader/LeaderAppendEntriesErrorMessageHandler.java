package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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

public class LeaderAppendEntriesErrorMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAppendEntriesErrorMessageHandler.class);

	private final LogStorage logStorage;
	private final ElectionState electionState;
	private final int currentServerId;
	private final ClusterRPCService clusterRPCService;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;

	public LeaderAppendEntriesErrorMessageHandler(LogStorage logStorage, ElectionState electionState, int currentServerId,
			ClusterRPCService clusterRPCService, MessagePublisher<RaftMessage> raftMessagePublisher) {
		this.logStorage = logStorage;
		this.electionState = electionState;
		this.currentServerId = currentServerId;
		this.clusterRPCService = clusterRPCService;
		this.raftMessagePublisher = raftMessagePublisher;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var appendEntriesError = (AppendEntriesErrorMessage) message;
		LOGGER.warn("Error occurred on append entries to {}. Going to retry send of same request", appendEntriesError.serverId());
		try {
			var nextLog = appendEntriesError.nextLog();
			var prevLog = appendEntriesError.prevLog();
			var serverId = appendEntriesError.serverId();
			var maxAppliedLogId = logStorage.getLastAppliedLog().orElse(null);
			int currentTerm = electionState.getCurrentTerm();
			appendLog(
					serverId,
					currentServerId,
					nextLog,
					prevLog,
					maxAppliedLogId,
					currentTerm
			);
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
