package raft.cluster.impl;

import raft.cluster.ClusterConfig;
import raft.cluster.ClusterRPCService;
import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.ReactorRaftServiceGrpc;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import reactor.core.publisher.Mono;

public class ClusterRPCServiceImpl implements ClusterRPCService {
	private final ClusterConfig clusterConfig;

	public ClusterRPCServiceImpl(ClusterConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
	}

	@Override
	public Mono<AppendEntriesReply> appendLog(int serverId, AppendEntriesRequest request) {
		return ReactorRaftServiceGrpc.newReactorStub(clusterConfig.getChannelByServerId(serverId))
				.appendEntries(Mono.just(request));
	}

	@Override
	public Mono<RequestVoteReply> requestVote(int serverId, RequestVote requestVote) {
		return ReactorRaftServiceGrpc.newReactorStub(clusterConfig.getChannelByServerId(serverId))
				.requestVote(Mono.just(requestVote));
	}
}
