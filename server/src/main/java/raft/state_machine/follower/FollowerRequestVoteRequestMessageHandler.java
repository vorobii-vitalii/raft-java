package raft.state_machine.follower;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.dto.LogId;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import raft.message.RaftMessage;
import raft.message.RequestVoteRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import raft.utils.LogUtils;

public class FollowerRequestVoteRequestMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FollowerRequestVoteRequestMessageHandler.class);

	private final ElectionState electionState;
	private final FollowerStateData followerStateData;
	private final LogStorage logStorage;

	public FollowerRequestVoteRequestMessageHandler(ElectionState electionState, FollowerStateData followerStateData, LogStorage logStorage) {
		this.electionState = electionState;
		this.followerStateData = followerStateData;
		this.logStorage = logStorage;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var requestVoteRequestMessage = (RequestVoteRequestMessage) message;
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
					followerStateData.updateHeartBeat();
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
																	followerStateData.updateHeartBeat();
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
																followerStateData.updateHeartBeat();
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

}
