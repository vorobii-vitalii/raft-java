package raft.grpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import raft.dto.AppendLogReply;
import raft.dto.AppendLogRequest;
import raft.dto.FetchLogsRequest;
import raft.dto.FetchLogsResponse;
import raft.dto.LogsServiceGrpc;
import raft.message.AddLog;
import raft.message.RaftMessage;
import raft.messaging.MessagePublisher;

public class LogServiceImpl extends LogsServiceGrpc.LogsServiceImplBase {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogServiceImpl.class);

	private final MessagePublisher<RaftMessage> raftMessageMessagePublisher;

	public LogServiceImpl(MessagePublisher<RaftMessage> raftMessageMessagePublisher) {
		this.raftMessageMessagePublisher = raftMessageMessagePublisher;
	}

	@Override
	public void appendLog(AppendLogRequest request, StreamObserver<AppendLogReply> responseObserver) {
		LOGGER.info("Received request to add new log {}", request);
		raftMessageMessagePublisher.publish(new AddLog(request, response -> {
			LOGGER.info("Response on the request = {}", request);
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		}));
	}

	@Override
	public void fetchAllLogs(FetchLogsRequest request, StreamObserver<FetchLogsResponse> responseObserver) {
		super.fetchAllLogs(request, responseObserver);
	}
}
