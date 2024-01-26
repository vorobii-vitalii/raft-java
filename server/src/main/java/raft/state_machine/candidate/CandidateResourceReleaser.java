package raft.state_machine.candidate;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import raft.message.RaftMessage;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.state_machine.RaftMessageProcessor;
import raft.state_machine.candidate.domain.CandidateStateData;

public class CandidateResourceReleaser implements RaftMessageProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CandidateResourceReleaser.class);

	private final CandidateStateData candidateStateData;

	public CandidateResourceReleaser(CandidateStateData candidateStateData) {
		this.candidateStateData = candidateStateData;
	}

	@Override
	public void process(RaftMessage message, MessageHandler messageHandler) {
		LOGGER.info("Cancelling election timeout task");
		Optional.ofNullable(candidateStateData.getElectionTimeoutTask())
				.ifPresent(CancellableTask::cancel);
	}
}
