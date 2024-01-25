package raft.state_machine.follower;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import raft.dto.AppendLogReply;
import raft.dto.AppendLogRequest;
import raft.message.AddLog;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;

@SuppressWarnings("unchecked")
class TestFollowerAddLogHandler {

	public static final int TERM = 12;
	public static final int LEADER_ID = 123;
	FollowerStateData followerStateData = mock(FollowerStateData.class);
	ElectionState electionState = mock(ElectionState.class);
	FollowerAddLogHandler followerAddLogHandler = new FollowerAddLogHandler(followerStateData, electionState);

	Consumer<AppendLogReply> appendLogReplyConsumer = mock(Consumer.class);
	MessageHandler messageHandler = mock(MessageHandler.class);

	@Test
	void processGivenLeaderIsUnknown() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(TERM);
		when(followerStateData.getCurrentLeader()).thenReturn(Optional.empty());

		var addLog = new AddLog(AppendLogRequest.newBuilder().build(), appendLogReplyConsumer);

		followerAddLogHandler.process(addLog, messageHandler);
		verify(appendLogReplyConsumer).accept(AppendLogReply.newBuilder()
				.setSuccess(false)
				.setTerm(TERM)
				.build());
		verifyNoInteractions(messageHandler);
	}

	@Test
	void processGivenLeaderIsKnown() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(TERM);
		when(followerStateData.getCurrentLeader()).thenReturn(Optional.of(LEADER_ID));

		var addLog = new AddLog(AppendLogRequest.newBuilder().build(), appendLogReplyConsumer);

		followerAddLogHandler.process(addLog, messageHandler);
		verify(appendLogReplyConsumer).accept(AppendLogReply.newBuilder()
				.setSuccess(false)
				.setTerm(TERM)
				.setLeaderId(LEADER_ID)
				.build());
		verifyNoInteractions(messageHandler);
	}

}
