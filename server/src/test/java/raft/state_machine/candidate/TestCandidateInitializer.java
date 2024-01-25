package raft.state_machine.candidate;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.cluster.ClusterConfig;
import raft.cluster.ClusterRPCService;
import raft.dto.LogId;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import raft.message.ElectionTimeout;
import raft.message.Initialize;
import raft.message.RaftMessage;
import raft.message.RequestVoteErrorReceived;
import raft.message.RequestVoteReplyReceived;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.CancellableTask;
import raft.scheduling.TimedMessageSender;
import raft.state_machine.candidate.domain.CandidateStateData;
import raft.state_machine.candidate.domain.ElectionStats;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
class TestCandidateInitializer {
	private static final int ELECTION_TIMEOUT = 1000;
	public static final int CURRENT_SERVER_ID = 100;
	public static final int CURRENT_TERM = 12;
	public static final LogId LAST_LOG_ID = LogId.newBuilder().setIndex(2).setTerm(CURRENT_TERM).build();
	public static final int SERVER_1 = 1;
	public static final int SERVER_2 = 2;

	ElectionState electionState = mock(ElectionState.class);
	ElectionStats electionStats = mock(ElectionStats.class);
	ClusterConfig clusterConfig = mock(ClusterConfig.class);
	ClusterRPCService clusterRPCService = mock(ClusterRPCService.class);
	MessagePublisher<RaftMessage> raftMessagePublisher = mock(MessagePublisher.class);
	LogStorage logStorage = mock(LogStorage.class);
	TimedMessageSender<RaftMessage> timedMessageSender = mock(TimedMessageSender.class);
	CandidateStateData candidateState = mock(CandidateStateData.class);

	CandidateInitializer candidateInitializer = new CandidateInitializer(
			electionState,
			electionStats,
			clusterConfig,
			clusterRPCService,
			raftMessagePublisher,
			logStorage,
			timedMessageSender,
			ELECTION_TIMEOUT,
			candidateState
	);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	CancellableTask heartbeatTask = mock(CancellableTask.class);

	@Test
	void processGivenNoLastAppliedLog() throws IOException {
		when(clusterConfig.getCurrentServerId()).thenReturn(CURRENT_SERVER_ID);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(clusterConfig.getOtherServerIds()).thenReturn(Set.of(SERVER_1, SERVER_2));
		when(timedMessageSender.scheduleOnce(eq(ELECTION_TIMEOUT), argThat(v -> new ElectionTimeout().equals(v.get()))))
				.thenReturn(heartbeatTask);
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.empty());
		var requestVote = RequestVote.newBuilder().setCandidateTerm(CURRENT_TERM + 1).setCandidateId(CURRENT_SERVER_ID).build();
		when(clusterRPCService.requestVote(SERVER_1, requestVote))
				.thenReturn(Mono.just(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM + 1).setVoteGranted(true).build()));
		when(clusterRPCService.requestVote(SERVER_2, requestVote))
				.thenReturn(Mono.error(new RuntimeException()));
		candidateInitializer.process(new Initialize(), messageHandler);
		verify(candidateState).setElectionTimeoutTask(heartbeatTask);
		verify(electionState).updateTerm(CURRENT_TERM + 1);
		verify(electionState).voteFor(CURRENT_SERVER_ID);
		verify(raftMessagePublisher)
				.publish(new RequestVoteReplyReceived(
						RequestVoteReply.newBuilder().setTerm(CURRENT_TERM + 1).setVoteGranted(true).build(),
						CURRENT_TERM + 1,
						SERVER_1
				));
		verify(raftMessagePublisher)
				.publish(new RequestVoteErrorReceived(CURRENT_TERM + 1, SERVER_2));
	}

	@Test
	void processGivenLastAppliedLogPresent() throws IOException {
		when(clusterConfig.getCurrentServerId()).thenReturn(CURRENT_SERVER_ID);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(clusterConfig.getOtherServerIds()).thenReturn(Set.of(SERVER_1, SERVER_2));
		when(timedMessageSender.scheduleOnce(eq(ELECTION_TIMEOUT), argThat(v -> new ElectionTimeout().equals(v.get()))))
				.thenReturn(heartbeatTask);
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.of(LAST_LOG_ID));
		var requestVote = RequestVote.newBuilder()
				.setCandidateTerm(CURRENT_TERM + 1)
				.setCandidateId(CURRENT_SERVER_ID)
				.setLastLog(LAST_LOG_ID)
				.build();
		when(clusterRPCService.requestVote(SERVER_1, requestVote))
				.thenReturn(Mono.just(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM + 1).setVoteGranted(true).build()));
		when(clusterRPCService.requestVote(SERVER_2, requestVote))
				.thenReturn(Mono.error(new RuntimeException()));
		candidateInitializer.process(new Initialize(), messageHandler);
		verify(candidateState).setElectionTimeoutTask(heartbeatTask);
		verify(electionState).updateTerm(CURRENT_TERM + 1);
		verify(electionState).voteFor(CURRENT_SERVER_ID);
		verify(raftMessagePublisher)
				.publish(new RequestVoteReplyReceived(
						RequestVoteReply.newBuilder().setTerm(CURRENT_TERM + 1).setVoteGranted(true).build(),
						CURRENT_TERM + 1,
						SERVER_1
				));
		verify(raftMessagePublisher)
				.publish(new RequestVoteErrorReceived(CURRENT_TERM + 1, SERVER_2));
	}

}
