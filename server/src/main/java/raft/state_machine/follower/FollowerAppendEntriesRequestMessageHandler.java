package raft.state_machine.follower;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.LogId;
import raft.message.AppendEntriesRequestMessage;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

public class FollowerAppendEntriesRequestMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FollowerAppendEntriesRequestMessageHandler.class);

	private final ElectionState electionState;
	private final LogStorage logStorage;
	private final FollowerStateData followerStateData;

	public FollowerAppendEntriesRequestMessageHandler(ElectionState electionState, LogStorage logStorage, FollowerStateData followerStateData) {
		this.electionState = electionState;
		this.logStorage = logStorage;
		this.followerStateData = followerStateData;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var appendEntriesRequestMessage = (AppendEntriesRequestMessage) message;
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
					followerStateData.updateHeartBeat();
					logStorage.applyAllChangesUntil(getLeaderCurrentLogId(appendEntriesRequest));
				}
				catch (IOException e) {
					LOGGER.error("Error occurred when appending new logs", e);
					throw new UncheckedIOException(e);
				}
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

}
