package raft.storage;

import java.io.IOException;
import java.util.Optional;

public interface ElectionState {
	void updateTerm(int term) throws IOException;
	void voteFor(int serverId) throws IOException;
	int getCurrentTerm() throws IOException;
	Optional<Integer> getVotedForInCurrentTerm() throws IOException;
}
