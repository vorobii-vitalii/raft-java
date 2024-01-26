package raft.state_machine.candidate;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.message.RaftMessage;
import raft.message.RequestVoteErrorReceived;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.candidate.domain.ElectionStats;
import raft.state_machine.candidate.domain.ElectionStatus;
import raft.storage.ElectionState;
import raft.utils.RaftUtils;

public class CandidateRequestVoteErrorReceived implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRequestVoteErrorReceived.class);
	private final ElectionStats electionStats;

	public CandidateRequestVoteErrorReceived(ElectionStats electionStats) {
		this.electionStats = electionStats;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var requestVoteErrorReceived = (RequestVoteErrorReceived) message;
		var serverId = requestVoteErrorReceived.serverId();
		electionStats.receiveRejection(serverId);
		var electionStatus = electionStats.getStatus();
		LOGGER.info("Server {} failed to respond on request vote. Election status = {}", serverId, electionStatus);
		if (electionStatus == ElectionStatus.LOST) {
			LOGGER.info("Majority not voted for me. I am follower 😩");
			messageHandler.changeState(NodeState.FOLLOWER);
		} else {
			LOGGER.info("Election result not yet decided!");
		}
	}
}
