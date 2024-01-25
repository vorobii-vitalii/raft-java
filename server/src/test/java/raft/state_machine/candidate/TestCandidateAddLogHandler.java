package raft.state_machine.candidate;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.dto.AppendLogReply;
import raft.dto.AppendLogRequest;
import raft.message.AddLog;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;

@SuppressWarnings("unchecked")
class TestCandidateAddLogHandler {

	public static final int CURRENT_TERM = 123;
	ElectionState electionState = Mockito.mock(ElectionState.class);

	CandidateAddLogHandler candidateAddLogHandler = new CandidateAddLogHandler(electionState);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	Consumer<AppendLogReply> appendLogReplyConsumer = Mockito.mock(Consumer.class);

	@Test
	void process() throws IOException {
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		var addLog = new AddLog(AppendLogRequest.newBuilder().build(), appendLogReplyConsumer);
		candidateAddLogHandler.process(addLog, messageHandler);
		verify(appendLogReplyConsumer).accept(
				AppendLogReply.newBuilder()
						.setTerm(CURRENT_TERM)
						.setSuccess(false)
						.build());
	}
}
