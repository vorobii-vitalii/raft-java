package raft.state_machine.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import raft.cluster.ClusterRPCService;
import raft.domain.NodeState;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.Log;
import raft.dto.LogId;
import raft.message.AppendEntriesErrorMessage;
import raft.message.AppendEntriesReplyMessage;
import raft.message.AppendEntriesRequestMessage;
import raft.message.RaftMessage;
import raft.message.SendHeartBeat;
import raft.messaging.MessagePublisher;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.TimedMessageSender;
import raft.storage.ElectionState;
import raft.storage.LogStorage;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
class TestLeaderState {
//	private static final int CURRENT_SERVER_ID = 2;
//	private static final int SLAVE_1 = 1;
//	private static final int SLAVE_2 = 3;
//	private static final Set<Integer> ALL_SERVERS = Set.of(SLAVE_1, CURRENT_SERVER_ID, SLAVE_2);
//	private static final int HEART_BEATS_INTERVAL = 100;
//	private static final LogId LAST_APPLIED_LOG = LogId.newBuilder().setIndex(1).setTerm(2).build();
//	private static final int CURRENT_TERM = 123;
//
//	LogStorage logStorage = mock(LogStorage.class);
//	TimedMessageSender<RaftMessage> timedMessageSender = mock(TimedMessageSender.class);
//	MessagePublisher<RaftMessage> raftMessagePublisher = mock(MessagePublisher.class);
//	ElectionState electionState = mock(ElectionState.class);
//	ClusterRPCService clusterRPCService = mock(ClusterRPCService.class);
//	State followerState = mock(State.class);
//
//	LeaderState leaderState = new LeaderState(
//			logStorage,
//			timedMessageSender,
//			HEART_BEATS_INTERVAL,
//			raftMessagePublisher,
//			electionState,
//			clusterRPCService,
//			ALL_SERVERS,
//			CURRENT_SERVER_ID
//	);
//
//	MessageHandler messageHandler = mock(MessageHandler.class);
//	Consumer<AppendEntriesReply> appendEntriesReplyConsumer = mock(Consumer.class);
//
//	@Nested
//	class InitializeTest {
//		@Test
//		void initializeGivenLogEmpty() throws IOException {
//			when(logStorage.getLastAppliedLog()).thenReturn(Optional.empty());
//			leaderState.initialize(messageHandler);
//			assertThat(leaderState.getPreviousLogIdByServerId()).isEmpty();
//			verify(raftMessagePublisher).publish(new SendHeartBeat());
//			verify(timedMessageSender).schedulePeriodical(eq(HEART_BEATS_INTERVAL), any());
//		}
//
//		@Test
//		void initializeGivenLogNotEmpty() throws IOException {
//			when(logStorage.getLastAppliedLog()).thenReturn(Optional.of(LAST_APPLIED_LOG));
//			leaderState.initialize(messageHandler);
//			assertThat(leaderState.getPreviousLogIdByServerId()).containsExactlyInAnyOrderEntriesOf(Map.of(
//					SLAVE_1, LAST_APPLIED_LOG,
//					SLAVE_2, LAST_APPLIED_LOG
//			));
//			verify(raftMessagePublisher).publish(new SendHeartBeat());
//			verify(timedMessageSender).schedulePeriodical(eq(HEART_BEATS_INTERVAL), any());
//		}
//	}
//
//	@Nested
//	class OnAppendEntriesTest {
//		@Test
//		void onAppendEntriesGivenTermIsLower() throws IOException {
//			when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
//
//			leaderState.onAppendEntries(new AppendEntriesRequestMessage(
//					AppendEntriesRequest.newBuilder()
//							.setLeaderId(SLAVE_1)
//							.setTerm(CURRENT_TERM - 1)
//							.build(),
//					appendEntriesReplyConsumer
//			));
//			verify(appendEntriesReplyConsumer).accept(AppendEntriesReply.newBuilder()
//					.setSuccess(false)
//					.setTerm(CURRENT_TERM)
//					.build());
//		}
//
//		@Test
//		void onAppendEntriesGivenTermIsHigher() throws IOException {
//			leaderState.setMessageHandler(messageHandler);
//
//			when(electionState.getCurrentTerm()).thenReturn(CURRENT_TERM);
//
//			var appendEntriesRequestMessage = new AppendEntriesRequestMessage(
//					AppendEntriesRequest.newBuilder()
//							.setLeaderId(SLAVE_1)
//							.setTerm(CURRENT_TERM + 1)
//							.build(),
//					appendEntriesReplyConsumer
//			);
//			leaderState.onAppendEntries(appendEntriesRequestMessage);
//
//			verify(messageHandler).changeState(NodeState.FOLLOWER);
//			verify(messageHandler).onMessage(appendEntriesRequestMessage);
//		}
//	}
//
//	@Nested
//	class AppendEntriesReplyTest {
//
//		@Test
//		void onAppendEntriesReplyGivenHeartBeatWasAppliedSuccessfully() {
//			var appendEntriesReplyMessage = new AppendEntriesReplyMessage(
//					AppendEntriesReply.newBuilder()
//							.setSuccess(true)
//							.setTerm(CURRENT_TERM)
//							.build(),
//					null,
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build())
//							.build(),
//					SLAVE_1
//			);
//			leaderState.onAppendEntriesReply(appendEntriesReplyMessage);
//			assertThat(leaderState.getMaxReplicatedLogByServerId()).isEmpty();
//			verifyNoInteractions(clusterRPCService);
//		}
//
//		@Test
//		void onAppendEntriesReplyGivenChangeWasAppliedSuccessfullyAndNoOtherLogsNeedToBeReplicated() throws IOException {
//			var appendEntriesReplyMessage = new AppendEntriesReplyMessage(
//					AppendEntriesReply.newBuilder()
//							.setSuccess(true)
//							.setTerm(CURRENT_TERM)
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build())
//							.build(),
//					SLAVE_1
//			);
//			when(logStorage.getLastAppliedLog())
//					.thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()));
//			when(logStorage.findFollowing(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()))
//					.thenReturn(Optional.empty());
//			leaderState.onAppendEntriesReply(appendEntriesReplyMessage);
//			assertThat(leaderState.getMaxReplicatedLogByServerId()).containsExactlyInAnyOrderEntriesOf(
//					Map.of(SLAVE_1, LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//			);
//			verifyNoInteractions(clusterRPCService);
//		}
//
//		@Test
//		void onAppendEntriesReplyGivenChangeWasAppliedSuccessfullyAndOtherLogsNeedToBeReplicatedHappyPath() throws IOException {
//			var appendEntriesReplyMessage = new AppendEntriesReplyMessage(
//					AppendEntriesReply.newBuilder()
//							.setSuccess(true)
//							.setTerm(CURRENT_TERM)
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build())
//							.build(),
//					SLAVE_1
//			);
//			when(logStorage.getLastAppliedLog())
//					.thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()));
//			when(logStorage.findFollowing(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()))
//					.thenReturn(Optional.of(Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
//							.setMsg("new log")
//							.build()));
//
//			when(clusterRPCService.appendLog(SLAVE_1, AppendEntriesRequest.newBuilder()
//					.setLeaderId(CURRENT_SERVER_ID)
//					.addAllEntries(List.of(
//							Log.newBuilder()
//									.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
//									.setMsg("new log")
//									.build()
//					))
//					.setPreviousLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//					.setLeaderCurrentLogId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//					.build())
//			).thenReturn(Mono.just(AppendEntriesReply.newBuilder().build()));
//
//			leaderState.onAppendEntriesReply(appendEntriesReplyMessage);
//			assertThat(leaderState.getMaxReplicatedLogByServerId()).containsExactlyInAnyOrderEntriesOf(
//					Map.of(SLAVE_1, LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//			);
//
//			verify(raftMessagePublisher)
//					.publish(new AppendEntriesReplyMessage(
//							AppendEntriesReply.newBuilder().build(),
//							Log.newBuilder()
//									.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
//									.setMsg("new log")
//									.build(),
//							Log.newBuilder()
//									.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//									.build(),
//							SLAVE_1
//					));
//		}
//
//		@Test
//		void onAppendEntriesReplyGivenChangeWasAppliedSuccessfullyAndOtherLogsNeedToBeReplicatedErrorOnCall() throws IOException {
//			var appendEntriesReplyMessage = new AppendEntriesReplyMessage(
//					AppendEntriesReply.newBuilder()
//							.setSuccess(true)
//							.setTerm(CURRENT_TERM)
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build())
//							.build(),
//					SLAVE_1
//			);
//			when(logStorage.getLastAppliedLog())
//					.thenReturn(Optional.of(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()));
//			when(logStorage.findFollowing(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build()))
//					.thenReturn(Optional.of(Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
//							.setMsg("new log")
//							.build()));
//
//			when(clusterRPCService.appendLog(SLAVE_1, AppendEntriesRequest.newBuilder()
//					.setLeaderId(CURRENT_SERVER_ID)
//					.addAllEntries(List.of(
//							Log.newBuilder()
//									.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
//									.setMsg("new log")
//									.build()
//					))
//					.setPreviousLog(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//					.setLeaderCurrentLogId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//					.build())
//			).thenReturn(Mono.error(new RuntimeException()));
//
//			leaderState.onAppendEntriesReply(appendEntriesReplyMessage);
//			assertThat(leaderState.getMaxReplicatedLogByServerId()).containsExactlyInAnyOrderEntriesOf(
//					Map.of(SLAVE_1, LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//			);
//			verify(raftMessagePublisher)
//					.publish(new AppendEntriesErrorMessage(
//							SLAVE_1,
//							Log.newBuilder()
//									.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//									.build(),
//							Log.newBuilder()
//									.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(2).build())
//									.setMsg("new log")
//									.build()
//					));
//
//		}
//
//		@Test
//		void onAppendEntriesReplyGivenChangeWasNotAppliedTermOfOtherServerIsHigher() {
//			leaderState.setMessageHandler(messageHandler);
//
//			var appendEntriesReplyMessage = new AppendEntriesReplyMessage(
//					AppendEntriesReply.newBuilder()
//							.setSuccess(false)
//							.setTerm(CURRENT_TERM + 1)
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(1).build())
//							.build(),
//					Log.newBuilder()
//							.setId(LogId.newBuilder().setTerm(CURRENT_TERM).setIndex(0).build())
//							.build(),
//					SLAVE_1
//			);
//
//			leaderState.onAppendEntriesReply(appendEntriesReplyMessage);
//
//			verify(messageHandler).changeState(NodeState.FOLLOWER);
//			verifyNoInteractions(clusterRPCService);
//		}
//
//	}
//
}

