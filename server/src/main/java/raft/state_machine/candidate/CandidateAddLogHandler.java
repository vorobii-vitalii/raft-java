package raft.state_machine.candidate;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.dto.AppendLogReply;
import raft.message.AddLog;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.storage.ElectionState;

public class CandidateAddLogHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateAddLogHandler.class);

	private final ElectionState electionState;

	public CandidateAddLogHandler(ElectionState electionState) {
		this.electionState = electionState;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		LOGGER.warn("Leader not yet chosen. Going to reject add log request 😓");
		var addLog = (AddLog) message;
		try {
			var appendLogReply = AppendLogReply.newBuilder()
					.setTerm(electionState.getCurrentTerm())
					.setSuccess(false)
					.build();
			addLog.replyConsumer().accept(appendLogReply);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
