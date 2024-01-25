package raft.state_machine.follower;

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

public class FollowerAddLogHandler implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FollowerAddLogHandler.class);

	private final FollowerStateData followerStateData;
	private final ElectionState electionState;

	public FollowerAddLogHandler(FollowerStateData followerStateData, ElectionState electionState) {
		this.followerStateData = followerStateData;
		this.electionState = electionState;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		var addLog = (AddLog) message;
		LOGGER.info("Got add log request, but I am not leader...");
		LOGGER.info("Rejecting request and letting know who is leader (if any)");
		try {
			var consumer = addLog.replyConsumer();
			var builder = AppendLogReply.newBuilder()
					.setSuccess(false)
					.setTerm(electionState.getCurrentTerm());
			followerStateData.getCurrentLeader().ifPresent(builder::setLeaderId);
			consumer.accept(builder.build());
		}
		catch (IOException error) {
			throw new UncheckedIOException(error);
		}
	}
}
