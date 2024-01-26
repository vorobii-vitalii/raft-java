package raft.state_machine.candidate.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.jupiter.api.Test;

class TestElectionStats {
	public static final int SERVER_1 = 1;
	public static final int SERVER_2 = 2;
	public static final int SERVER_3 = 3;

	ElectionStats electionStats = new ElectionStats(Set.of(SERVER_1, SERVER_2, SERVER_3));

	@Test
	void getStatusGivenMostServersVotedForMe() {
		electionStats.receiveVote(SERVER_1);
		electionStats.receiveVote(SERVER_2);
		assertThat(electionStats.getStatus()).isEqualTo(ElectionStatus.WON);
	}

	@Test
	void getStatusGivenMostServersNotVotedForMe() {
		electionStats.receiveRejection(SERVER_1);
		electionStats.receiveRejection(SERVER_2);
		assertThat(electionStats.getStatus()).isEqualTo(ElectionStatus.LOST);
	}

	@Test
	void getStatusGivenElectionResultNotClear() {
		electionStats.receiveRejection(SERVER_1);
		electionStats.receiveVote(SERVER_2);
		assertThat(electionStats.getStatus()).isEqualTo(ElectionStatus.NOT_DECIDED);
	}

}