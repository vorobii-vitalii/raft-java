package raft.state_machine.leader;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.dto.Log;
import raft.dto.LogId;
import raft.message.AppendEntriesReplyMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.state_machine.leader.services.LogAppender;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

class TestLeaderAppendEntriesReplyMessageHandler {
	private static final int RECEIVER_SERVER_ID = 1245;
	private static final int TERM = 5;

	ElectionState electionState = Mockito.mock(ElectionState.class);
	LogStorage logStorage = Mockito.mock(LogStorage.class);
	ServersReplicationState serversReplicationState = Mockito.mock(ServersReplicationState.class);
	LogAppender logAppender = Mockito.mock(LogAppender.class);

	LeaderAppendEntriesReplyMessageHandler appendEntriesReplyMessageHandler =
			new LeaderAppendEntriesReplyMessageHandler(electionState, logStorage, serversReplicationState, logAppender);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	@Test
	void onHeartBeatReceiveSuccess() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(TERM);
		var message = new AppendEntriesReplyMessage(
				AppendEntriesReply.newBuilder()
						.setSuccess(true)
						.setTerm(TERM)
						.build(),
				null,
				Log.newBuilder().setId(LogId.newBuilder().setIndex(2).setTerm(TERM).build()).setMsg("msg").build(),
				RECEIVER_SERVER_ID
		);
		appendEntriesReplyMessageHandler.process(message, messageHandler);
		verify(serversReplicationState).stopReplication(RECEIVER_SERVER_ID);
		verifyNoInteractions(messageHandler);
	}

	@Test
	void onLastLogReplicationSuccess() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(TERM);
		var message = new AppendEntriesReplyMessage(
				AppendEntriesReply.newBuilder()
						.setSuccess(true)
						.setTerm(TERM)
						.build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(3).setTerm(TERM).build()).setMsg("new log").build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(2).setTerm(TERM).build()).setMsg("msg").build(),
				RECEIVER_SERVER_ID
		);
		when(logStorage.findFollowing(LogId.newBuilder().setIndex(3).setTerm(TERM).build())).thenReturn(Optional.empty());

		appendEntriesReplyMessageHandler.process(message, messageHandler);

		verify(serversReplicationState).stopReplication(RECEIVER_SERVER_ID);
		verify(serversReplicationState).updateMaxReplicatedLog(RECEIVER_SERVER_ID, LogId.newBuilder().setIndex(3).setTerm(TERM).build());
		verifyNoInteractions(messageHandler);
	}

	@Test
	void onNotLastLogReplicationSuccess() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(TERM);
		when(serversReplicationState.getMaxReplicatedByQuorumIndex())
				.thenReturn(LogId.newBuilder().setIndex(3).setTerm(TERM).build());

		var message = new AppendEntriesReplyMessage(
				AppendEntriesReply.newBuilder()
						.setSuccess(true)
						.setTerm(TERM)
						.build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(3).setTerm(TERM).build()).setMsg("new log").build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(2).setTerm(TERM).build()).setMsg("msg").build(),
				RECEIVER_SERVER_ID
		);
		when(logStorage.findFollowing(LogId.newBuilder().setIndex(3).setTerm(TERM).build()))
				.thenReturn(Optional.of(
						Log.newBuilder().setId(LogId.newBuilder().setIndex(4).setTerm(TERM).build()).setMsg("new log 2").build()
				));

		appendEntriesReplyMessageHandler.process(message, messageHandler);

		verify(logAppender)
				.appendLog(
						RECEIVER_SERVER_ID,
						Log.newBuilder().setId(LogId.newBuilder().setIndex(4).setTerm(TERM).build()).setMsg("new log 2").build(),
						Log.newBuilder().setId(LogId.newBuilder().setIndex(3).setTerm(TERM).build()).setMsg("new log").build()
				);
		verify(serversReplicationState, never()).stopReplication(RECEIVER_SERVER_ID);
		verify(serversReplicationState).updateMaxReplicatedLog(RECEIVER_SERVER_ID, LogId.newBuilder().setIndex(3).setTerm(TERM).build());
		verify(logStorage).applyAllChangesUntil(LogId.newBuilder().setIndex(3).setTerm(TERM).build());
		verifyNoInteractions(messageHandler);
	}

	@Test
	void onLastLogReplicationFailureBecauseIAmNotLeaderAnymore() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(TERM);
		var message = new AppendEntriesReplyMessage(
				AppendEntriesReply.newBuilder()
						.setSuccess(false)
						.setTerm(TERM + 1)
						.build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(3).setTerm(TERM).build()).setMsg("new log").build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(2).setTerm(TERM).build()).setMsg("msg").build(),
				RECEIVER_SERVER_ID
		);
		appendEntriesReplyMessageHandler.process(message, messageHandler);
		verify(messageHandler).changeState(NodeState.FOLLOWER);
	}

	@Test
	void onLastLogReplicationFailureBecausePrevLogWasNotFound() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(TERM);
		when(logStorage.findPrevious(LogId.newBuilder().setIndex(2).setTerm(TERM).build()))
				.thenReturn(Optional.of(
						Log.newBuilder()
								.setId(LogId.newBuilder().setIndex(1).setTerm(TERM).build())
								.setMsg("prev")
								.build()
				));
		var message = new AppendEntriesReplyMessage(
				AppendEntriesReply.newBuilder()
						.setSuccess(false)
						.setTerm(TERM)
						.build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(3).setTerm(TERM).build()).setMsg("new log").build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(2).setTerm(TERM).build()).setMsg("msg").build(),
				RECEIVER_SERVER_ID
		);
		appendEntriesReplyMessageHandler.process(message, messageHandler);

		verify(logAppender).appendLog(
				RECEIVER_SERVER_ID,
				Log.newBuilder().setId(LogId.newBuilder().setIndex(2).setTerm(TERM).build()).setMsg("msg").build(),
				Log.newBuilder().setId(LogId.newBuilder().setIndex(1).setTerm(TERM).build()).setMsg("prev").build());
		verify(serversReplicationState, never()).stopReplication(RECEIVER_SERVER_ID);
		verifyNoInteractions(messageHandler);
	}


}
