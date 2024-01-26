package raft;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ServerBuilder;
import raft.cluster.impl.ClusterRPCServiceImpl;
import raft.cluster.impl.InMemoryClusterConfig;
import raft.domain.NodeState;
import raft.grpc.LogServiceImpl;
import raft.grpc.RaftServiceImpl;
import raft.message.RaftMessage;
import raft.message.RaftMessageType;
import raft.messaging.impl.BlockingQueueMessagePublisher;
import raft.messaging.impl.MessageHandleProcess;
import raft.messaging.impl.MessageHandler;
import raft.scheduling.impl.TimedMessageSenderImpl;
import raft.scheduling.impl.TimedTaskSchedulerImpl;
import raft.serialization.impl.LogIdSerializerDeserializer;
import raft.state_machine.State;
import raft.state_machine.candidate.CandidateAddLogHandler;
import raft.state_machine.candidate.CandidateAppendEntriesHandler;
import raft.state_machine.candidate.CandidateElectionTimeoutHandler;
import raft.state_machine.candidate.CandidateInitializer;
import raft.state_machine.candidate.CandidateRequestVoteErrorReceived;
import raft.state_machine.candidate.CandidateRequestVoteHandler;
import raft.state_machine.candidate.CandidateResourceReleaser;
import raft.state_machine.candidate.CandidateVoteReplyReceivedHandler;
import raft.state_machine.candidate.domain.CandidateStateData;
import raft.state_machine.candidate.domain.ElectionStats;
import raft.state_machine.follower.FollowerAddLogHandler;
import raft.state_machine.follower.FollowerAppendEntriesRequestMessageHandler;
import raft.state_machine.follower.FollowerHeartBeatCheckHandler;
import raft.state_machine.follower.FollowerInitializer;
import raft.state_machine.follower.FollowerRequestVoteRequestMessageHandler;
import raft.state_machine.follower.FollowerResourceReleaser;
import raft.state_machine.follower.FollowerStateData;
import raft.state_machine.impl.StateFactoryImpl;
import raft.state_machine.impl.StateImpl;
import raft.state_machine.leader.LeaderAddLogMessageHandler;
import raft.state_machine.leader.LeaderAppendEntriesErrorMessageHandler;
import raft.state_machine.leader.LeaderAppendEntriesHandler;
import raft.state_machine.leader.LeaderAppendEntriesReplyMessageHandler;
import raft.state_machine.leader.LeaderInitializer;
import raft.state_machine.leader.LeaderRequestVoteRequestMessageHandler;
import raft.state_machine.leader.LeaderResourceReleaser;
import raft.state_machine.leader.LeaderSendHeartBeatHandler;
import raft.state_machine.leader.data.LeaderStateData;
import raft.state_machine.leader.data.ServersReplicationState;
import raft.state_machine.leader.services.LogAppender;
import raft.state_machine.leader.services.LogIdGenerator;
import raft.storage.impl.FileElectionState;
import raft.storage.impl.FileLogStorage;
import raft.storage.impl.IndexFileImpl;
import raft.storage.impl.LogFileImpl;

public class RaftNodeMain {
	private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeMain.class);
	public static final String RW_SYNC_MODE = "rws";
	public static final String LOG_FILE_NAME = "log.txt";
	public static final String INDEX_FILE_NAME = "log_index.txt";
	public static final String ELECTION_FILE_NAME = "election.txt";
	public static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(4);
	public static final int MESSAGES_QUEUE_SIZE = 10_000;
	public static final String SERVERS_DELIMITER = ",";
	public static final String SERVER_ID_ADDRESS_DELIMITER = "=";
	public static final int ELECTION_TIMEOUT = 2000;
	public static final int HEART_BEAT_CHECK_TIMEOUT = 1000;
	public static final int HEART_BEATS_INTERVAL = 100;

	public static void main(String[] args) throws IOException, InterruptedException {
		var port = getPort();
		LOGGER.info("Starting new Raft instance... Port = {}", port);

		var basePath = Paths.get(getFolderForPersistence());
		var logIdSerializer = new LogIdSerializerDeserializer();

		// LOG Storage

		var log = basePath.resolve(LOG_FILE_NAME).toFile();
		var indexFile = basePath.resolve(INDEX_FILE_NAME).toFile();
		var electionFile = basePath.resolve(ELECTION_FILE_NAME).toFile();

		createFile(log);
		createFile(indexFile);
		createFile(electionFile);

		var logFile = new LogFileImpl(new RandomAccessFile(log, RW_SYNC_MODE));
		var logIndex = new IndexFileImpl<>(
				logIdSerializer,
				logIdSerializer,
				new RandomAccessFile(indexFile, RW_SYNC_MODE));

		var logStorage = new FileLogStorage(logIndex, logFile);

		var electionState = new FileElectionState(new RandomAccessFile(electionFile, RW_SYNC_MODE));

		// Task scheduler

		var timedTaskScheduler = new TimedTaskSchedulerImpl(SCHEDULED_EXECUTOR_SERVICE);

		// Messages queue
		var messagesQueue = new ArrayBlockingQueue<RaftMessage>(MESSAGES_QUEUE_SIZE);

		// Cluster config
		var addressByServerId = getAddressByServerId();
		var currentServerId = getCurrentServerId();
		LOGGER.info("Cluster config = {}. Current server = {}", addressByServerId, currentServerId);

		var clusterConfig = new InMemoryClusterConfig(addressByServerId, currentServerId);
		var clusterRPCService = new ClusterRPCServiceImpl(clusterConfig);

		var raftMessageMessagePublisher = new BlockingQueueMessagePublisher<>(messagesQueue);
		Supplier<State> candidateStateSupplier = () -> {
			var candidateStateData = new CandidateStateData();
			var electionStats = new ElectionStats(addressByServerId.keySet());
			return new StateImpl(Map.of(
					RaftMessageType.AddLog, new CandidateAddLogHandler(electionState),
					RaftMessageType.AppendEntriesRequestMessage, new CandidateAppendEntriesHandler(electionState),
					RaftMessageType.ElectionTimeout, new CandidateElectionTimeoutHandler(),
					RaftMessageType.Initialize, new CandidateInitializer(
							electionState,
							electionStats,
							clusterConfig,
							clusterRPCService,
							raftMessageMessagePublisher,
							logStorage,
							new TimedMessageSenderImpl<>(timedTaskScheduler, raftMessageMessagePublisher),
							ELECTION_TIMEOUT,
							candidateStateData
					),
					RaftMessageType.RequestVoteErrorReceived, new CandidateRequestVoteErrorReceived(electionStats),
					RaftMessageType.RequestVoteRequestMessage, new CandidateRequestVoteHandler(
							electionState
					),
					RaftMessageType.Release, new CandidateResourceReleaser(
							candidateStateData
					),
					RaftMessageType.RequestVoteReplyReceived, new CandidateVoteReplyReceivedHandler(electionStats)
			));
		};

		Supplier<State> followerStateSupplier = () -> {
			FollowerStateData followerStateData = new FollowerStateData(Clock.systemDefaultZone());

			return new StateImpl(Map.of(
					RaftMessageType.AddLog, new FollowerAddLogHandler(
							followerStateData,
							electionState
					),
					RaftMessageType.AppendEntriesRequestMessage, new FollowerAppendEntriesRequestMessageHandler(
							electionState,
							logStorage,
							followerStateData
					),
					RaftMessageType.HeartBeatCheck, new FollowerHeartBeatCheckHandler(
							getHearbeatTimeout(),
							followerStateData
					),
					RaftMessageType.Initialize, new FollowerInitializer(
							followerStateData,
							new TimedMessageSenderImpl<>(timedTaskScheduler, raftMessageMessagePublisher),
							HEART_BEAT_CHECK_TIMEOUT
					),
					RaftMessageType.RequestVoteRequestMessage, new FollowerRequestVoteRequestMessageHandler(
							electionState,
							followerStateData,
							logStorage
					),
					RaftMessageType.Release, new FollowerResourceReleaser(
							followerStateData
					)
			));
		};

		Supplier<State> leaderStateSupplier = () -> {
			LeaderStateData leaderStateData = new LeaderStateData();
			var serversReplicationState = new ServersReplicationState(clusterConfig);
			var logAppender = new LogAppender(clusterConfig, logStorage, electionState, clusterRPCService, raftMessageMessagePublisher);
			var logIdGenerator = new LogIdGenerator(logStorage, electionState);
			return new StateImpl(Map.of(
					RaftMessageType.AppendEntriesErrorMessage, new LeaderAppendEntriesErrorMessageHandler(logAppender),
					RaftMessageType.AddLog, new LeaderAddLogMessageHandler(
							logStorage,
							electionState,
							logAppender,
							logIdGenerator,
							clusterConfig,
							serversReplicationState
					),
					RaftMessageType.AppendEntriesRequestMessage, new LeaderAppendEntriesHandler(
							electionState
					),
					RaftMessageType.AppendEntriesReplyMessage, new LeaderAppendEntriesReplyMessageHandler(
							electionState,
							logStorage,
							serversReplicationState,
							logAppender
					),
					RaftMessageType.Initialize, new LeaderInitializer(
							logStorage,
							serversReplicationState,
							raftMessageMessagePublisher,
							new TimedMessageSenderImpl<>(timedTaskScheduler, raftMessageMessagePublisher),
							getHeartBeatsInterval(),
							leaderStateData
					),
					RaftMessageType.RequestVoteRequestMessage, new LeaderRequestVoteRequestMessageHandler(
							electionState
					),
					RaftMessageType.Release, new LeaderResourceReleaser(
							leaderStateData,
							logStorage
					),
					RaftMessageType.SendHeartBeat, new LeaderSendHeartBeatHandler(
							logStorage,
							serversReplicationState,
							logAppender,
							clusterConfig
					)
			));
		};
		var stateFactory = new StateFactoryImpl(Map.of(
				NodeState.CANDIDATE, candidateStateSupplier,
				NodeState.FOLLOWER, followerStateSupplier,
				NodeState.LEADER, leaderStateSupplier
		));

		var messageHandler = new MessageHandler(NodeState.FOLLOWER, stateFactory);
		LOGGER.info("Starting message handling process");
		Thread.startVirtualThread(new MessageHandleProcess(messageHandler, () -> !Thread.currentThread().isInterrupted(), messagesQueue));

		LOGGER.info("Starting server and blocking...");
		var server = ServerBuilder.forPort(port)
				.addService(new RaftServiceImpl(raftMessageMessagePublisher))
				.addService(new LogServiceImpl(raftMessageMessagePublisher))
				.build()
				.start();
		Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
		server.awaitTermination();
	}

	private static Duration getHearbeatTimeout() {
		return Duration.ofMillis(3000 + new SecureRandom().nextInt(2000));
	}

	private static int getHeartBeatsInterval() {
		return HEART_BEATS_INTERVAL + new SecureRandom().nextInt(2000);
	}

	private static int getCurrentServerId() {
		return Integer.parseInt(System.getenv("CURRENT_SERVER_ID"));
	}

	private static Map<Integer, String> getAddressByServerId() {
		return Stream.of(System.getenv("CLUSTER").trim().split(SERVERS_DELIMITER))
				.map(v -> {
					var arr = v.split(SERVER_ID_ADDRESS_DELIMITER);
					return new String[] {arr[0].trim(), arr[1].trim()};
				})
				.collect(Collectors.toMap(v -> Integer.parseInt(v[0]), v -> v[1]));
	}

	private static void createFile(File logFile) throws IOException {
		if (logFile.createNewFile()) {
			LOGGER.info("new file was created");
		} else {
			LOGGER.info("file already existed...");
		}
	}

	private static int getPort() {
		return Integer.parseInt(System.getenv("PORT"));
	}

	private static String getFolderForPersistence() {
		return System.getenv("PERSISTENCE_FOLDER");
	}

}
