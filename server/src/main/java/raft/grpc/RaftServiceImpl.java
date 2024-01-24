package raft.grpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.RaftServiceGrpc;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import raft.message.AppendEntriesRequestMessage;
import raft.message.RaftMessage;
import raft.message.RequestVoteRequestMessage;
import raft.messaging.MessagePublisher;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
	private static final Logger LOGGER = LoggerFactory.getLogger(RaftServiceImpl.class);

	private final MessagePublisher<RaftMessage> raftMessageMessagePublisher;

	public RaftServiceImpl(MessagePublisher<RaftMessage> raftMessageMessagePublisher) {
		this.raftMessageMessagePublisher = raftMessageMessagePublisher;
	}

	@Override
	public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesReply> responseObserver) {
		LOGGER.info("Received new append entries request = {}", request);
		raftMessageMessagePublisher.publish(new AppendEntriesRequestMessage(request, response -> {
			LOGGER.info("Response to the request = {}", response);
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		}));
	}

	@Override
	public void requestVote(RequestVote request, StreamObserver<RequestVoteReply> responseObserver) {
		LOGGER.info("Received new vote request = {}", request);
		raftMessageMessagePublisher.publish(new RequestVoteRequestMessage(request, response -> {
			LOGGER.info("Response to the request = {}", response);
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		}));
	}
}

