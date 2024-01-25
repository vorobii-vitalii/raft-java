package raft.state_machine.candidate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.message.RaftMessage;
import raft.message.RequestVoteReplyReceived;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.candidate.domain.ElectionStats;

public class CandidateVoteReplyReceivedHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRequestVoteHandler.class);
	private final ElectionStats electionStats;

	public CandidateVoteReplyReceivedHandler(ElectionStats electionStats) {
		this.electionStats = electionStats;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var requestVoteReplyReceived = (RequestVoteReplyReceived) message;
		var termId = requestVoteReplyReceived.termId();
		int serverId = requestVoteReplyReceived.serverId();
		if (requestVoteReplyReceived.reply().getVoteGranted()) {
			LOGGER.info("Server {} voted for me! term = {}", serverId, termId);
			electionStats.receiveVote(serverId);
		} else {
			int replyTermId = requestVoteReplyReceived.reply().getTerm();
			if (replyTermId == termId) {
				LOGGER.info("Reply term matches {}. But server {} didnt vote for me", replyTermId, serverId);
				electionStats.receiveRejection(serverId);
			} else {
				LOGGER.info("Reply term is bigger {}. Becoming follower...", replyTermId);
				messageHandler.changeState(NodeState.FOLLOWER);
				return;
			}
		}
		switch (electionStats.getStatus()) {
			case WON -> {
				LOGGER.info("Majority voted for me. I am leader 😃");
				messageHandler.changeState(NodeState.LEADER);
			}
			case LOST -> {
				LOGGER.info("Majority not voted for me. I am follower 😩");
				messageHandler.changeState(NodeState.FOLLOWER);
			}
			case NOT_DECIDED ->  {
				LOGGER.info("Election result is not yet decided!");
			}
		}
	}
}
