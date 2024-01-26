package raft.state_machine.leader;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.AppendEntriesErrorMessage;
import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.leader.services.LogAppender;

public class LeaderAppendEntriesErrorMessageHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAppendEntriesErrorMessageHandler.class);

	private final LogAppender logAppender;

	public LeaderAppendEntriesErrorMessageHandler(LogAppender logAppender) {
		this.logAppender = logAppender;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var appendEntriesError = (AppendEntriesErrorMessage) message;
		LOGGER.warn("Error occurred on append entries to {}. Going to retry send of same request", appendEntriesError.serverId());
		try {
			var nextLog = appendEntriesError.nextLog();
			var prevLog = appendEntriesError.prevLog();
			var serverId = appendEntriesError.serverId();
			logAppender.appendLog(serverId, nextLog, prevLog);
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}

}
