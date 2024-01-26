package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.dto.RequestVoteReply;
import raft.message.RaftMessage;
import raft.message.RequestVoteRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.storage.ElectionState;

public class LeaderRequestVoteRequestMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAppendEntriesHandler.class);

	private final ElectionState electionState;

	public LeaderRequestVoteRequestMessageHandler(ElectionState electionState) {
		this.electionState = electionState;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var requestVoteRequestMessage = (RequestVoteRequestMessage) message;
		try {
			var currentTerm = electionState.getCurrentTerm();
			var requestVote = requestVoteRequestMessage.requestVote();
			var replyConsumer = requestVoteRequestMessage.replyConsumer();
			int candidateTerm = requestVote.getCandidateTerm();
			if (candidateTerm <= currentTerm) {
				LOGGER.warn("Candidate term {} lower or equal than current term {}", candidateTerm, currentTerm);
				replyConsumer.accept(RequestVoteReply.newBuilder().setTerm(currentTerm).setVoteGranted(false).build());
			} else {
				LOGGER.info("Candidate term higher ({} > {})! It means I am not leader 😩", requestVote.getCandidateTerm(), currentTerm);
				LOGGER.info("Becoming follower...");
				messageHandler.changeState(NodeState.FOLLOWER);
				LOGGER.info("Reprocessing message!");
				messageHandler.onMessage(requestVoteRequestMessage);
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

}
