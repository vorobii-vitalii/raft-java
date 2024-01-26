package raft.state_machine.follower;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import raft.dto.LogId;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import raft.message.RequestVoteRequestMessage;
import raft.messaging.impl.MessageHandler;
import raft.storage.ElectionState;
import raft.storage.LogStorage;

@SuppressWarnings("unchecked")
class TestFollowerRequestVoteRequestMessageHandler {

	public static final int CURRENT_TERM = 123;
	public static final int CANDIDATE_ID = 11;
	ElectionState electionState = mock(ElectionState.class);
	FollowerStateData followerStateData = mock(FollowerStateData.class);
	LogStorage logStorage = mock(LogStorage.class);

	FollowerRequestVoteRequestMessageHandler requestVoteRequestMessageHandler =
			new FollowerRequestVoteRequestMessageHandler(electionState, followerStateData, logStorage);

	Consumer<RequestVoteReply> requestVoteReplyConsumer = mock(Consumer.class);
	MessageHandler messageHandler = mock(MessageHandler.class);

	@Test
	void givenOtherServerTermIsLower() throws IOException {
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateId(CANDIDATE_ID)
						.setCandidateTerm(CURRENT_TERM - 1)
						.build(),
				requestVoteReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		requestVoteRequestMessageHandler.process(requestVoteRequestMessage, messageHandler);
		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM).setVoteGranted(false).build());
	}

	@Test
	void givenOtherServerTermIsHigherButItsLogIsOutdated() throws IOException {
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateId(CANDIDATE_ID)
						.setCandidateTerm(CURRENT_TERM + 1)
						.setLastLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
						.build(),
				requestVoteReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(electionState.getVotedForInCurrentTerm()).thenReturn(Optional.empty());
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build()));

		requestVoteRequestMessageHandler.process(requestVoteRequestMessage, messageHandler);

		verify(electionState).updateTerm(CURRENT_TERM + 1);
		verify(followerStateData).resetLeader();
		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM + 1).setVoteGranted(false).build());

	}

	@Test
	void givenOtherServerTermIsHigherButItsLogIsNewer() throws IOException {
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateId(CANDIDATE_ID)
						.setCandidateTerm(CURRENT_TERM + 1)
						.setLastLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(3).build())
						.build(),
				requestVoteReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(electionState.getVotedForInCurrentTerm()).thenReturn(Optional.empty());
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build()));

		requestVoteRequestMessageHandler.process(requestVoteRequestMessage, messageHandler);

		verify(electionState).updateTerm(CURRENT_TERM + 1);
		verify(followerStateData).updateHeartBeat();
		verify(followerStateData).resetLeader();
		verify(followerStateData).updateHeartBeat();
		verify(electionState).voteFor(CANDIDATE_ID);

		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM + 1).setVoteGranted(true).build());
	}

	@Test
	void givenOtherServerTermIsHigherButItsLogIsSame() throws IOException {
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateId(CANDIDATE_ID)
						.setCandidateTerm(CURRENT_TERM + 1)
						.setLastLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
						.build(),
				requestVoteReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(electionState.getVotedForInCurrentTerm()).thenReturn(Optional.empty());
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build()));

		requestVoteRequestMessageHandler.process(requestVoteRequestMessage, messageHandler);

		verify(electionState).updateTerm(CURRENT_TERM + 1);
		verify(followerStateData).updateHeartBeat();
		verify(followerStateData).resetLeader();
		verify(followerStateData).updateHeartBeat();
		verify(electionState).voteFor(CANDIDATE_ID);

		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM + 1).setVoteGranted(true).build());
	}

	@Test
	void givenOtherServerTermIsSameAndThisServerAlreadyVotedForAnother() throws IOException {
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateId(CANDIDATE_ID)
						.setCandidateTerm(CURRENT_TERM)
						.setLastLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
						.build(),
				requestVoteReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(electionState.getVotedForInCurrentTerm()).thenReturn(Optional.of(123));

		requestVoteRequestMessageHandler.process(requestVoteRequestMessage, messageHandler);

		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM).setVoteGranted(false).build());
	}

	@Test
	void givenOtherServerTermIsSameAndThisServerNotAlreadyVotedForAnotherAndLogIsNewEnough() throws IOException {
		var requestVoteRequestMessage = new RequestVoteRequestMessage(
				RequestVote.newBuilder()
						.setCandidateId(CANDIDATE_ID)
						.setCandidateTerm(CURRENT_TERM)
						.setLastLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
						.build(),
				requestVoteReplyConsumer
		);
		when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
		when(electionState.getVotedForInCurrentTerm()).thenReturn(Optional.empty());
		when(logStorage.getLastAppliedLog()).thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build()));

		requestVoteRequestMessageHandler.process(requestVoteRequestMessage, messageHandler);

		verify(requestVoteReplyConsumer).accept(RequestVoteReply.newBuilder().setTerm(CURRENT_TERM).setVoteGranted(true).build());
		verify(followerStateData).updateHeartBeat();
		verify(electionState).voteFor(CANDIDATE_ID);
	}

}
