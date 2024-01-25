package raft.state_machine.candidate;

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

public class CandidateRequestVoteHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRequestVoteHandler.class);

	private final ElectionState electionState;

	public CandidateRequestVoteHandler(ElectionState electionState) {
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
			if (candidateTerm < currentTerm) {
				LOGGER.warn("Candidate term {} lower than current term {}", candidateTerm, currentTerm);
				replyConsumer.accept(RequestVoteReply.newBuilder().setTerm(currentTerm).setVoteGranted(false).build());
			} else {
				if (requestVote.getCandidateTerm() > currentTerm) {
					LOGGER.info("Candidate term higher ({} > {})! It means I lost election 😩", requestVote.getCandidateTerm(), currentTerm);
					LOGGER.info("Becoming follower...");
					messageHandler.changeState(NodeState.FOLLOWER);
					LOGGER.info("Reprocessing message!");
					messageHandler.onMessage(requestVoteRequestMessage);
				} else {
					LOGGER.info("I am currently participating on election on term = {}. Will not vote for others", currentTerm);
					replyConsumer.accept(RequestVoteReply.newBuilder().setTerm(currentTerm).setVoteGranted(false).build());
				}
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}
}
