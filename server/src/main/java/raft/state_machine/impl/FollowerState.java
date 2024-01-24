package raft.state_machine.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.AppendLogReply;
import raft.dto.LogId;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import raft.message.AddLog;
import raft.message.AppendEntriesErrorMessage;
import raft.message.AppendEntriesReplyMessage;
import raft.message.AppendEntriesRequestMessage;
import raft.message.HeartBeatCheck;
import raft.message.RaftMessage;
import raft.message.RequestVoteErrorReceived;
import raft.message.RequestVoteReplyReceived;
import raft.message.RequestVoteRequestMessage;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.scheduling.TimedTaskScheduler;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import raft.utils.LogUtils;

public class FollowerState extends AbstractState {
	private static final Logger LOGGER = LoggerFactory.getLogger(FollowerState.class);
	public static final int NO_LEADER = 0;

	private final Clock clock;
	private final Duration timeout;
	private final ElectionState electionState;
	private final LogStorage logStorage;
	private Instant lastHeartBeat;
	private final TimedTaskScheduler timedTaskScheduler;
	private final int heartBeatCheckTimeout;
	private final MessagePublisher<RaftMessage> raftMessageMessagePublisher;
	private CancellableTask heartBeatCheck;
	private int leaderId;

	public FollowerState(
			Clock clock,
			Duration timeout,
			ElectionState electionState,
			LogStorage logStorage, TimedTaskScheduler timedTaskScheduler, int heartBeatCheckTimeout,
			MessagePublisher<RaftMessage> raftMessageMessagePublisher
	) {
		this.clock = clock;
		this.timeout = timeout;
		this.electionState = electionState;
		this.logStorage = logStorage;
		this.timedTaskScheduler = timedTaskScheduler;
		this.heartBeatCheckTimeout = heartBeatCheckTimeout;
		this.raftMessageMessagePublisher = raftMessageMessagePublisher;
	}

	@Override
	public void initialize(MessageHandler messageHandler) {
		setMessageHandler(messageHandler);
		lastHeartBeat = clock.instant();
		LOGGER.info("Scheduling heart beat check process");
		heartBeatCheck = timedTaskScheduler.scheduleWithFixedDelay(
				heartBeatCheckTimeout,
				() -> raftMessageMessagePublisher.publish(new HeartBeatCheck())
		);
	}

	@Override
	public void onAppendEntries(AppendEntriesRequestMessage appendEntriesRequestMessage) {
		var appendEntriesRequest = appendEntriesRequestMessage.request();
		LOGGER.info("Received append entries request {}", appendEntriesRequest);
		var replyConsumer = appendEntriesRequestMessage.replyConsumer();
		try {
			var currentTerm = electionState.getCurrentTerm();
			var leaderTerm = appendEntriesRequest.getTerm();
			if (leaderTerm < currentTerm) {
				LOGGER.info("Leader term lower (current node term = {}), letting him know...", currentTerm);
				replyConsumer.accept(
						AppendEntriesReply.newBuilder()
								.setSuccess(false)
								.setTerm(currentTerm)
								.build());
			} else {
				if (leaderTerm > currentTerm) {
					LOGGER.info("Leader term higher ({} > {})! Going to change my term and vote for him", leaderTerm, currentTerm);
					electionState.updateTerm(leaderTerm);
					electionState.voteFor(appendEntriesRequest.getLeaderId());
					logStorage.removeUncommittedChanges();
				}
				try {
					boolean success = logStorage.appendLog(getPreviousLog(appendEntriesRequest), appendEntriesRequest.getEntriesList());
					LOGGER.info("Logs added = {} 😄", success);
					replyConsumer.accept(AppendEntriesReply.newBuilder()
							.setSuccess(success)
							.setTerm(leaderTerm)
							.build());
					resetLastHeartBeat();
					logStorage.applyAllChangesUntil(getLeaderCurrentLogId(appendEntriesRequest));
				}
				catch (IOException e) {
					LOGGER.error("Error occurred when appending new logs", e);
					throw new UncheckedIOException(e);
				}
				leaderId = appendEntriesRequest.getLeaderId();
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	private LogId getLeaderCurrentLogId(AppendEntriesRequest appendEntriesRequest) {
		return appendEntriesRequest.hasLeaderCurrentLogId() ? appendEntriesRequest.getLeaderCurrentLogId() : null;
	}

	private LogId getPreviousLog(AppendEntriesRequest appendEntriesRequest) {
		return appendEntriesRequest.hasPreviousLog() ?  appendEntriesRequest.getPreviousLog() : null;
	}

	@Override
	public void onAppendEntriesReply(AppendEntriesReplyMessage appendEntriesReplyMessage) {

	}

	@Override
	public void onAppendEntriesError(AppendEntriesErrorMessage appendEntriesError) {

	}

	@Override
	public void onElectionTimeout() {
		LOGGER.info("Follower doesn't participate in election, skipping...");
	}

	@Override
	public void onHeartbeatCheck() {
		var durationSinceLastHeartbeat = Duration.between(lastHeartBeat, clock.instant());
		if (durationSinceLastHeartbeat.compareTo(timeout) > 0) {
			LOGGER.info("Duration since last heartbeat {} > {}. Becoming candidate 🙂", durationSinceLastHeartbeat, timeout);
			changeState(NodeState.CANDIDATE);
		} else {
			LOGGER.info("Leader sent heart beat in time, keeping follower state!");
		}
	}

	@Override
	public void onRequestVote(RequestVoteRequestMessage requestVoteRequestMessage) {
		try {
			var currentTerm = electionState.getCurrentTerm();
			var requestVote = requestVoteRequestMessage.requestVote();
			var replyConsumer = requestVoteRequestMessage.replyConsumer();
			int candidateTerm = requestVote.getCandidateTerm();
			if (candidateTerm < currentTerm) {
				LOGGER.warn("Candidate term {} lower than current term {} 🙂", candidateTerm, currentTerm);
				replyConsumer.accept(RequestVoteReply.newBuilder().setTerm(currentTerm).setVoteGranted(false).build());
			} else {
				if (requestVote.getCandidateTerm() > currentTerm) {
					LOGGER.info("Candidate term higher ({} > {})! Going to change my term", requestVote.getCandidateTerm(), currentTerm);
					electionState.updateTerm(requestVote.getCandidateTerm());
					leaderId = NO_LEADER;
					resetLastHeartBeat();
				}
				int term = Math.max(requestVote.getCandidateTerm(), currentTerm);
				electionState.getVotedForInCurrentTerm()
						.ifPresentOrElse(
								votedFor -> {
									LOGGER.info("Already voted for {} in term {}", votedFor, term);
									replyConsumer.accept(RequestVoteReply.newBuilder().setTerm(currentTerm).setVoteGranted(false).build());
								},
								() -> {
									LOGGER.info("Voted for no-one in term {}", term);
									try {
										logStorage.getLastAppliedLog()
												.ifPresentOrElse(
														lastLog -> {
															try {
																LOGGER.info("My last log = {}, his last log = {}", lastLog,
																		getLastLog(requestVote));
																if (LogUtils.isOtherLogAtLeastAsNew(getLastLog(requestVote), lastLog)) {
																	LOGGER.info("Accepting request vote request");
																	resetLastHeartBeat();
																	electionState.voteFor(requestVote.getCandidateId());
																	replyConsumer.accept(RequestVoteReply.newBuilder()
																			.setTerm(currentTerm)
																			.setVoteGranted(true)
																			.build());
																} else {
																	LOGGER.info("Rejecting request vote request since my log is newer!");
																	replyConsumer.accept(RequestVoteReply.newBuilder()
																			.setTerm(currentTerm)
																			.setVoteGranted(false)
																			.build());
																}
															}
															catch (IOException error) {
																throw new UncheckedIOException(error);
															}
														},
														() -> {
															try {
																LOGGER.info("My log is empty, his log must be more up to date");
																LOGGER.info("Accepting request vote request");
																resetLastHeartBeat();
																electionState.voteFor(requestVote.getCandidateId());
																replyConsumer.accept(RequestVoteReply.newBuilder()
																		.setTerm(currentTerm)
																		.setVoteGranted(true)
																		.build());
															}
															catch (IOException error) {
																throw new UncheckedIOException(error);
															}
														});
									}
									catch (IOException e) {
										LOGGER.error("IO error occurred...", e);
										throw new UncheckedIOException(e);
									}
								}
						);
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	private static LogId getLastLog(RequestVote requestVote) {
		return requestVote.hasLastLog() ? requestVote.getLastLog() : null;
	}

	@Override
	public void onRequestVoteReply(RequestVoteReplyReceived requestVoteReplyReceived) {

	}

	@Override
	public void onRequestVoteErrorReceived(RequestVoteErrorReceived requestVoteErrorReceived) {

	}

	@Override
	public void onSendHeartBeat() {
		LOGGER.info("Follower doesn't send heartbeats...");
	}

	@Override
	public void onAddLog(AddLog addLog) {
		LOGGER.info("Got add log request, but I am not leader...");
		LOGGER.info("Rejecting request and letting know who is leader (if any)");
		try {
			var consumer = addLog.replyConsumer();
			consumer.accept(AppendLogReply.newBuilder()
					.setSuccess(false)
					.setTerm(electionState.getCurrentTerm())
					.setLeaderId(leaderId)
					.build());
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

	@Override
	public void releaseResources() {
		LOGGER.info("Stopping heart beat check...");
		heartBeatCheck.cancel();
	}

	private void resetLastHeartBeat() {
		LOGGER.info("Resetting heartbeat!");
		lastHeartBeat = clock.instant();
	}

}
