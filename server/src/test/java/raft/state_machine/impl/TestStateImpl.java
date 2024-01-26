package raft.state_machine.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.jupiter.api.Test;

import raft.dto.AppendLogRequest;
import raft.message.AddLog;
import raft.message.Initialize;
import raft.message.RaftMessageType;
import raft.messaging.impl.MessageHandler;
import raft.state_machine.RaftMessageProcessor;

class TestStateImpl {

	RaftMessageProcessor addLogProcessor = mock(RaftMessageProcessor.class);

	Map<RaftMessageType, RaftMessageProcessor> messageProcessorByType = Map.of(
			RaftMessageType.ADD_LOG, addLogProcessor
	);

	StateImpl state = new StateImpl(messageProcessorByType);

	MessageHandler messageHandler = mock(MessageHandler.class);

	@Test
	void onMessageGivenHandlerForMessageTypeDefined() {
		var addLog = new AddLog(AppendLogRequest.newBuilder().build(), v -> {
		});
		state.onMessage(addLog, messageHandler);
		verify(addLogProcessor).process(addLog, messageHandler);
	}

	@Test
	void onMessageGivenHandlerForMessageTypeNotDefined() {
		assertDoesNotThrow(() -> state.onMessage(new Initialize(), messageHandler));
	}

}