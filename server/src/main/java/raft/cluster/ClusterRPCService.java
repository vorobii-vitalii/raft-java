package raft.cluster;

import raft.dto.AppendEntriesReply;
import raft.dto.AppendEntriesRequest;
import raft.dto.RequestVote;
import raft.dto.RequestVoteReply;
import reactor.core.publisher.Mono;

public interface ClusterRPCService {
	Mono<AppendEntriesReply> appendLog(int serverId, AppendEntriesRequest request);
	Mono<RequestVoteReply> requestVote(int serverId, RequestVote requestVote);
}
