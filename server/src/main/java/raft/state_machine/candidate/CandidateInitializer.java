package raft.state_machine.candidate;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterConfig;
import raft.cluster.ClusterRPCService;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import raft.message.ElectionTimeout;
import raft.message.RaftMessage;
import raft.message.RequestVoteErrorReceived;
import raft.message.RequestVoteReplyReceived;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.TimedMessageSender;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.candidate.domain.CandidateStateData;
import raft.state_machine.candidate.domain.ElectionStats;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import reactor.core.CoreSubscriber;

public class CandidateInitializer implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateInitializer.class);

	private final ElectionState electionState;
	private final ElectionStats electionStats;
	private final ClusterConfig clusterConfig;
	private final ClusterRPCService clusterRPCService;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;
	private final LogStorage logStorage;
	private final TimedMessageSender<RaftMessage> timedMessageSender;
	private final int electionTimeout;
	private final CandidateStateData candidateState;

	public CandidateInitializer(
			ElectionState electionState,
			ElectionStats electionStats,
			ClusterConfig clusterConfig,
			ClusterRPCService clusterRPCService,
			MessagePublisher<RaftMessage> raftMessagePublisher,
			LogStorage logStorage,
			TimedMessageSender<RaftMessage> timedMessageSender,
			int electionTimeout,
			CandidateStateData candidateState
	) {
		this.electionState = electionState;
		this.electionStats = electionStats;
		this.clusterConfig = clusterConfig;
		this.clusterRPCService = clusterRPCService;
		this.raftMessagePublisher = raftMessagePublisher;
		this.logStorage = logStorage;
		this.timedMessageSender = timedMessageSender;
		this.electionTimeout = electionTimeout;
		this.candidateState = candidateState;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		try {
			var currentServerId = clusterConfig.getCurrentServerId();
			LOGGER.info("Incrementing current term and voting for myself ({})", currentServerId);
			electionStats.receiveVote(currentServerId);
			var prevTerm = electionState.getCurrentTerm();
			LOGGER.info("Previous term was {}", prevTerm);
			var newTerm = prevTerm + 1;
			electionState.updateTerm(newTerm);
			electionState.voteFor(currentServerId);
			LOGGER.info("Requesting votes from {}", clusterConfig.getOtherServerIds());
			var requestVote = createRequestVote(newTerm);
			for (var serverId : clusterConfig.getOtherServerIds()) {
				requestVote(serverId, requestVote, newTerm);
			}
			LOGGER.info("Scheduling election timeout!");
			candidateState.setElectionTimeoutTask(timedMessageSender.scheduleOnce(electionTimeout, ElectionTimeout::new));
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	private void requestVote(Integer serverId, RequestVote requestVote, int currentTerm) throws IOException {
		LOGGER.info("Requesting vote from {}", serverId);
		clusterRPCService.requestVote(serverId, requestVote)
				.subscribe(new CoreSubscriber<>() {
					@Override
					public void onSubscribe(Subscription subscription) {
						subscription.request(1);
					}

					@Override
					public void onNext(RequestVoteReply requestVoteReply) {
						LOGGER.info("Reply from {} on request vote received {}", serverId, requestVote);
						raftMessagePublisher
								.publish(new RequestVoteReplyReceived(requestVoteReply, currentTerm, serverId));
					}

					@Override
					public void onError(Throwable error) {
						LOGGER.error("Server {} failed to reply on vote request", serverId, error);
						raftMessagePublisher.publish(new RequestVoteErrorReceived(currentTerm, serverId));
					}

					@Override
					public void onComplete() {
					}
				});
	}

	private RequestVote createRequestVote(int currentTerm) throws IOException {
		var builder = RequestVote.newBuilder().setCandidateTerm(currentTerm).setCandidateId(clusterConfig.getCurrentServerId());
		logStorage.getLastAppliedLog().ifPresent(builder::setLastLog);
		return builder.build();
	}

}
