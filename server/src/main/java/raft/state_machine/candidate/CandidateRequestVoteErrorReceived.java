package raft.state_machine.candidate;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.message.RaftMessage;
import raft.message.RequestVoteErrorReceived;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.utils.RaftUtils;

public class CandidateRequestVoteErrorReceived implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRequestVoteErrorReceived.class);

	private final Set<Integer> notVotedForMe;
	private final Set<Integer> allServerIds;

	public CandidateRequestVoteErrorReceived(Set<Integer> notVotedForMe, Set<Integer> allServerIds) {
		this.notVotedForMe = notVotedForMe;
		this.allServerIds = allServerIds;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var requestVoteErrorReceived = (RequestVoteErrorReceived) message;
		notVotedForMe.add(requestVoteErrorReceived.serverId());
		if (RaftUtils.isQuorum(allServerIds, notVotedForMe)) {
			LOGGER.info("Majority not voted for me {} / {}. I am follower 😩", notVotedForMe, allServerIds);
			messageHandler.changeState(NodeState.FOLLOWER);
		}
	}
}
