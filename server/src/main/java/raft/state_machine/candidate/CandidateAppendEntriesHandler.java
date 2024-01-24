package raft.state_machine.candidate;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.message.AppendEntriesRequestMessage;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.storage.ElectionState;

public class CandidateAppendEntriesHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateAppendEntriesHandler.class);

	private final ElectionState electionState;

	public CandidateAppendEntriesHandler(ElectionState electionState) {
		this.electionState = electionState;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		AppendEntriesRequestMessage appendEntriesRequestMessage = (AppendEntriesRequestMessage) message;
 		try {
			var appendEntriesRequest = appendEntriesRequestMessage.request();
			LOGGER.info("Received append entries request {}", appendEntriesRequest);
			var replyConsumer = appendEntriesRequestMessage.replyConsumer();
			var currentTerm = electionState.getCurrentTerm();
			var leaderTerm = appendEntriesRequest.getTerm();
			if (leaderTerm < currentTerm) {
				LOGGER.info("Leader term lower (current node term = {}), letting him know...", currentTerm);
				replyConsumer.accept(AppendEntriesReply.newBuilder().setSuccess(false).setTerm(currentTerm).build());
			} else {
				LOGGER.info(
						"It must mean another leader was already elected 😩. So becoming follower and reprocessing the message as follower...");
				messageHandler.changeState(NodeState.FOLLOWER);
				messageHandler.onMessage(appendEntriesRequestMessage);
			}
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}
}
