package raft.state_machine.candidate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.cluster.ClusterRPCService;
import raft.dto.LogId;
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
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import reactor.core.CoreSubscriber;

public class CandidateInitializer implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateInitializer.class);

	private final Set<Integer> votedForMe;
	private final ElectionState electionState;
	private final int currentServerId;
	private final Set<Integer> allServerIds;
	private final ClusterRPCService clusterRPCService;
	private final MessagePublisher<RaftMessage> raftMessagePublisher;
	private final LogStorage logStorage;
	private final TimedMessageSender<RaftMessage> timedMessageSender;
	private final int electionTimeout;
	private final CandidateStateData candidateState;

	public CandidateInitializer(
			Set<Integer> votedForMe,
			ElectionState electionState,
			int currentServerId,
			Set<Integer> allServerIds,
			ClusterRPCService clusterRPCService,
			MessagePublisher<RaftMessage> raftMessagePublisher,
			LogStorage logStorage,
			TimedMessageSender<RaftMessage> timedMessageSender,
			int electionTimeout,
			CandidateStateData candidateState
	) {
		this.votedForMe = votedForMe;
		this.electionState = electionState;
		this.currentServerId = currentServerId;
		this.allServerIds = allServerIds;
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
			LOGGER.info("Incrementing current term and voting for myself ({})", currentServerId);
			votedForMe.add(currentServerId);
			var prevTerm = electionState.getCurrentTerm();
			LOGGER.info("Previous term was {}", prevTerm);
			var currentTerm = prevTerm + 1;
			electionState.updateTerm(currentTerm);
			electionState.voteFor(currentServerId);
			LOGGER.info("Requesting votes from all servers {} except {}", allServerIds, currentServerId);
			for (var serverId : allServerIds) {
				if (serverId == currentServerId) {
					LOGGER.info("Skipping myself");
					continue;
				}
				requestVote(serverId, currentTerm);
			}
			LOGGER.info("Scheduling election timeout!");
			candidateState.setElectionTimeoutTask(timedMessageSender.scheduleOnce(electionTimeout, ElectionTimeout::new));
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	private void requestVote(Integer serverId, int currentTerm) throws IOException {
		var requestVote = createRequestVote(currentTerm);
		LOGGER.info("Requesting vote from {}", serverId);
		clusterRPCService
				.requestVote(serverId, requestVote)
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
		LogId logId = logStorage.getLastAppliedLog().orElse(null);
		var builder = RequestVote.newBuilder()
				.setCandidateTerm(currentTerm)
				.setCandidateId(currentServerId);
		if (logId != null) {
			builder.setLastLog(logId);
		}
		return builder.build();
	}

}
