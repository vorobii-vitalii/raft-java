package raft.state_machine.follower;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.Log;
import raft.dto.LogId;
import raft.message.AppendEntriesRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

@SuppressWarnings("unchecked")
class TestFollowerAppendEntriesRequestMessageHandler {

	public static final int CURRENT_TERM = 123;
	public static final int LEADER_ID = 924;
	ElectionState electionState = mock(ElectionState.class);
	LogStorage logStorage = mock(LogStorage.class);
	FollowerStateData followerStateData = mock(FollowerStateData.class);
	FollowerAppendEntriesRequestMessageHandler followerAppendEntriesRequestMessageHandler = new FollowerAppendEntriesRequestMessageHandler(
			electionState,
			logStorage,
			followerStateData
	);
	Consumer<AppendEntriesReply> appendEntriesReplyConsumer = mock(Consumer.class);
	MessageHandler messageHandler = mock(MessageHandler.class);

	@Test
	void givenOtherServerTermIsLower() throws IOException {
		var appendEntriesRequestMessage = new AppendEntriesRequestMessage(
				AppendEntriesRequest.newBuilder()
						.setTerm(CURRENT_TERM - 1)
						.build(),
				appendEntriesReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		followerAppendEntriesRequestMessageHandler.process(appendEntriesRequestMessage, messageHandler);
		verify(appendEntriesReplyConsumer).accept(
				AppendEntriesReply.newBuilder()
						.setSuccess(false)
						.setTerm(CURRENT_TERM)
						.build());
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void givenOtherServerTermIsHigher(boolean isChangeCommitted) throws IOException {
		var appendEntriesRequestMessage = new AppendEntriesRequestMessage(
				AppendEntriesRequest.newBuilder()
						.setLeaderId(LEADER_ID)
						.setTerm(CURRENT_TERM + 1)
						.setPreviousLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build())
						.addEntries(Log.newBuilder().setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()).setMsg("New log").build())
						.build(),
				appendEntriesReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(logStorage.appendLog(
				LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build(),
				List.of(
						Log.newBuilder().setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()).setMsg("New log").build()
				)
		)).thenReturn(isChangeCommitted);

		followerAppendEntriesRequestMessageHandler.process(appendEntriesRequestMessage, messageHandler);

		verify(electionState).updateTerm(CURRENT_TERM + 1);
		verify(electionState).voteFor(LEADER_ID);
		verify(followerStateData).updateLeader(LEADER_ID);
		verify(followerStateData).updateHeartBeat();

		verify(logStorage).removeUncommittedChanges();
		verify(appendEntriesReplyConsumer)
				.accept(AppendEntriesReply.newBuilder().setSuccess(isChangeCommitted).setTerm(CURRENT_TERM + 1).build());
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void givenOtherServerTermIsEqualToCurrentOne(boolean isChangeCommitted) throws IOException {
		var appendEntriesRequestMessage = new AppendEntriesRequestMessage(
				AppendEntriesRequest.newBuilder()
						.setLeaderId(LEADER_ID)
						.setTerm(CURRENT_TERM)
						.setPreviousLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build())
						.addEntries(Log.newBuilder().setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()).setMsg("New log").build())
						.build(),
				appendEntriesReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(logStorage.appendLog(
				LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build(),
				List.of(
						Log.newBuilder().setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()).setMsg("New log").build()
				)
		)).thenReturn(isChangeCommitted);

		followerAppendEntriesRequestMessageHandler.process(appendEntriesRequestMessage, messageHandler);

		verify(electionState).voteFor(LEADER_ID);
		verify(followerStateData).updateLeader(LEADER_ID);
		verify(followerStateData).updateHeartBeat();

		verify(logStorage, never()).removeUncommittedChanges();
		verify(electionState, never()).updateTerm(anyInt());
		verify(appendEntriesReplyConsumer)
				.accept(AppendEntriesReply.newBuilder().setSuccess(isChangeCommitted).setTerm(CURRENT_TERM).build());
	}

}
