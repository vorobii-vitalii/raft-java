package raft.state_machine.leader.services;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.cluster.ClusterConfig;
import raft.cluster.ClusterRPCService;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.LogId;
import raft.message.RaftMessage;
import raft.messaging.MessagePublisher;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
class TestLogAppender {

	public static final int CURRENT_SERVER_ID = 12;
	public static final int CURRENT_TERM = 922;
	public static final LogId PREV_LOG = LogId.newBuilder().setIndex(1).setTerm(CURRENT_TERM).build();
	public static final LogId NEXT_LOG = LogId.newBuilder().setIndex(2).setTerm(CURRENT_TERM).build();
	public static final int RECEIVER_SERVER = 922;
	ClusterConfig clusterConfig = Mockito.mock(ClusterConfig.class);
	LogStorage logStorage = Mockito.mock(LogStorage.class);
	ElectionState electionState = Mockito.mock(ElectionState.class);
	ClusterRPCService clusterRPCService = Mockito.mock(ClusterRPCService.class);
	MessagePublisher<RaftMessage> raftMessagePublisher = Mockito.mock(MessagePublisher.class);

	LogAppender logAppender = new LogAppender(clusterConfig, logStorage, electionState, clusterRPCService, raftMessagePublisher);

	@Test
	void appendLogGivenLogStorageEmptyHappyPath() throws IOException {
		when(clusterConfig.getCurrentServerId()).thenReturn(CURRENT_SERVER_ID);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.empty());
		when(clusterRPCService.appendLog(RECEIVER_SERVER, AppendEntriesRequest.newBuilder()
				.setLeaderId(CURRENT_SERVER_ID)
				.setTerm(CURRENT_TERM)
				.setPreviousLog(PREV_LOG)
				.build()
		)).thenReturn(Mono.just(AppendEntriesReply.newBuilder().setSuccess(true).build()));

		// TODO
		logAppender.appendLog(RECEIVER_SERVER, NEXT_LOG, PREV_LOG);

	}
}