package raft.state_machine.leader.services;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterConfig;
import raft.cluster.ClusterRPCService;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.Log;
import raft.message.AppendEntriesErrorMessage;
import raft.message.AppendEntriesReplyMessage;
import raft.message.RaftMessage;
import raft.messaging.MessagePublisher;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import reactor.core.CoreSubscriber;

public class LogAppender {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogAppender.class);

	private final ClusterConfig clusterConfig;
	private final LogStorage logStorage;
	private final ElectionState electionState;
	private final ClusterRPCService clusterRPCService;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;

	public LogAppender(
			ClusterConfig clusterConfig,
			LogStorage logStorage,
			ElectionState electionState,
			ClusterRPCService clusterRPCService,
			MessagePublisher<RaftMessage> raftMessagePublisher
	) {
		this.clusterConfig = clusterConfig;
		this.logStorage = logStorage;
		this.electionState = electionState;
		this.clusterRPCService = clusterRPCService;
		this.raftMessagePublisher = raftMessagePublisher;
	}

	public void appendLog(
			int receiverServerId,
			@Nullable Log nextLog,
			@Nullable Log previousLog
	) throws IOException {
		var currentServerId = clusterConfig.getCurrentServerId();
		var currentTerm = electionState.getCurrentTerm();
		var builder = AppendEntriesRequest.newBuilder()
				.setLeaderId(currentServerId)
				.setTerm(currentTerm)
				.addAllEntries(Optional.ofNullable(nextLog).map(List::of).orElse(Collections.emptyList()));

		Optional.ofNullable(previousLog).map(Log::getId).ifPresent(builder::setPreviousLog);
		logStorage.getLastAppliedLog().ifPresent(builder::setLeaderCurrentLogId);

		var appendEntriesRequest = builder.build();
		clusterRPCService.appendLog(receiverServerId, appendEntriesRequest)
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
								receiverServerId
						));
					}

					@Override
					public void onError(Throwable error) {
						LOGGER.error("Error occurred on replication to server {}", receiverServerId, error);
						raftMessagePublisher
								.publish(new AppendEntriesErrorMessage(receiverServerId, previousLog, nextLog));
					}

					@Override
					public void onComplete() {
					}
				});
	}

}
