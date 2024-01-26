package raft.state_machine.leader;

import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import raft.dto.Log;
import raft.dto.LogId;
import raft.message.AppendEntriesErrorMessage;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.leader.services.LogAppender;

class TestLeaderAppendEntriesErrorMessageHandler {

	public static final int RECEIVER_ID = 12;
	public static final Log PREV_LOG = Log.newBuilder().setId(LogId.newBuilder().setTerm(1).setIndex(2).build()).build();
	public static final Log NEXT_LOG = Log.newBuilder().setId(LogId.newBuilder().setTerm(1).setIndex(1).build()).build();
	LogAppender logAppender = Mockito.mock(LogAppender.class);

	LeaderAppendEntriesErrorMessageHandler appendEntriesErrorMessageHandler =
			new LeaderAppendEntriesErrorMessageHandler(logAppender);

	MessageHandler messageHandler = Mockito.mock(MessageHandler.class);

	@Test
	void expectLogWriteIsRetried() throws IOException {
		var message = new AppendEntriesErrorMessage(RECEIVER_ID, PREV_LOG, NEXT_LOG);
		appendEntriesErrorMessageHandler.process(message, messageHandler);
		verify(logAppender).appendLog(RECEIVER_ID, NEXT_LOG, PREV_LOG);
	}

}