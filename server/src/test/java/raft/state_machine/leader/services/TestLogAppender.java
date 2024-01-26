package raft.state_machine.leader.services;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.cluster.ClusterConfig;
import raft.cluster.ClusterRPCService;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.Log;
import raft.dto.LogId;
import raft.message.AppendEntriesErrorMessage;
import raft.message.AppendEntriesReplyMessage;
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
				.addEntries(Log.newBuilder().setId(NEXT_LOG).setMsg("next").build())
				.build()
		)).thenReturn(Mono.just(AppendEntriesReply.newBuilder().setSuccess(true).build()));

		logAppender.appendLog(
				RECEIVER_SERVER,
				Log.newBuilder().setId(NEXT_LOG).setMsg("next").build(),
				Log.newBuilder().setId(PREV_LOG).setMsg("prev").build());
		verify(raftMessagePublisher).publish(
				new AppendEntriesReplyMessage(
						AppendEntriesReply.newBuilder().setSuccess(true).build(),
						Log.newBuilder().setId(NEXT_LOG).setMsg("next").build(),
						Log.newBuilder().setId(PREV_LOG).setMsg("prev").build(),
						RECEIVER_SERVER
				));
	}

	@Test
	void appendLogGivenLogStorageEmptyFailureCase() throws IOException {
		when(clusterConfig.getCurrentServerId()).thenReturn(CURRENT_SERVER_ID);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.empty());
		when(clusterRPCService.appendLog(RECEIVER_SERVER, AppendEntriesRequest.newBuilder()
				.setLeaderId(CURRENT_SERVER_ID)
				.setTerm(CURRENT_TERM)
				.setPreviousLog(PREV_LOG)
				.addEntries(Log.newBuilder().setId(NEXT_LOG).setMsg("next").build())
				.build()
		)).thenReturn(Mono.error(new RuntimeException()));

		logAppender.appendLog(
				RECEIVER_SERVER,
				Log.newBuilder().setId(NEXT_LOG).setMsg("next").build(),
				Log.newBuilder().setId(PREV_LOG).setMsg("prev").build());
		verify(raftMessagePublisher).publish(
				new AppendEntriesErrorMessage(
						RECEIVER_SERVER,
						Log.newBuilder().setId(PREV_LOG).setMsg("prev").build(),
						Log.newBuilder().setId(NEXT_LOG).setMsg("next").build()
				));
	}

	@Test
	void appendLogGivenLogStorageNotEmptyHappyPath() throws IOException {
		when(clusterConfig.getCurrentServerId()).thenReturn(CURRENT_SERVER_ID);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(logStorage.getLastAppliedLog())
				.thenReturn(Optional.of(LogId.newBuilder().setIndex(0).setTerm(CURRENT_TERM).build()));
		when(clusterRPCService.appendLog(RECEIVER_SERVER, AppendEntriesRequest.newBuilder()
				.setLeaderId(CURRENT_SERVER_ID)
				.setTerm(CURRENT_TERM)
				.setPreviousLog(PREV_LOG)
				.setLeaderCurrentLogId(LogId.newBuilder().setIndex(0).setTerm(CURRENT_TERM).build())
				.addEntries(Log.newBuilder().setId(NEXT_LOG).setMsg("next").build())
				.build()
		)).thenReturn(Mono.just(AppendEntriesReply.newBuilder().setSuccess(true).build()));

		logAppender.appendLog(
				RECEIVER_SERVER,
				Log.newBuilder().setId(NEXT_LOG).setMsg("next").build(),
				Log.newBuilder().setId(PREV_LOG).setMsg("prev").build());
		verify(raftMessagePublisher).publish(
				new AppendEntriesReplyMessage(
						AppendEntriesReply.newBuilder().setSuccess(true).build(),
						Log.newBuilder().setId(NEXT_LOG).setMsg("next").build(),
						Log.newBuilder().setId(PREV_LOG).setMsg("prev").build(),
						RECEIVER_SERVER
				));
	}

}