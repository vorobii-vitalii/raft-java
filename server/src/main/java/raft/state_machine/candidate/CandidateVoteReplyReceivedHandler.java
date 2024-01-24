package raft.state_machine.candidate;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.message.RaftMessage;
import raft.message.RequestVoteReplyReceived;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.utils.RaftUtils;

public class CandidateVoteReplyReceivedHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRequestVoteHandler.class);

	private final Set<Integer> allServerIds;
	private final Set<Integer> votedForMe;
	private final Set<Integer> notVotedForMe;

	public CandidateVoteReplyReceivedHandler(Set<Integer> allServerIds, Set<Integer> votedForMe, Set<Integer> notVotedForMe) {
		this.allServerIds = allServerIds;
		this.votedForMe = votedForMe;
		this.notVotedForMe = notVotedForMe;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var requestVoteReplyReceived = (RequestVoteReplyReceived) message;
		var termId = requestVoteReplyReceived.termId();
		int serverId = requestVoteReplyReceived.serverId();
		if (requestVoteReplyReceived.reply().getVoteGranted()) {
			LOGGER.info("Server {} voted for me! term = {}", serverId, termId);
			votedForMe.add(serverId);
		} else {
			int replyTermId = requestVoteReplyReceived.reply().getTerm();
			if (replyTermId == termId) {
				LOGGER.info("Reply term matches {}. But server {} didnt vote for me", replyTermId, serverId);
				notVotedForMe.add(serverId);
			} else {
				LOGGER.info("Reply term is bigger {}. Becoming follower...", replyTermId);
				messageHandler.changeState(NodeState.FOLLOWER);
				return;
			}
		}
		changeStateIfElectionAlreadyWonOrLost(messageHandler);
	}

	private void changeStateIfElectionAlreadyWonOrLost(MessageHandler messageHandler) {
		if (RaftUtils.isQuorum(allServerIds, votedForMe)) {
			LOGGER.info("Majority voted for me {} / {}. I am leader 😃", votedForMe, allServerIds);
			messageHandler.changeState(NodeState.LEADER);
		} else if (RaftUtils.isQuorum(allServerIds, notVotedForMe)) {
			LOGGER.info("Majority not voted for me {} / {}. I am follower 😩", notVotedForMe, allServerIds);
			messageHandler.changeState(NodeState.FOLLOWER);
		}
	}
}
