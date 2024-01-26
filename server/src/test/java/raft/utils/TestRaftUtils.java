package raft.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestRaftUtils {

	public static Object[][] isQuorumParameters() {
		return new Object[][] {
				{
						Set.of(1, 2, 3, 4, 5),
						Set.of(1, 2),
						false
				},
				{
						Set.of(1, 2, 3, 4, 5),
						Set.of(1, 2, 3),
						true
				},
				{
						Set.of(1, 2, 3, 4, 5),
						Set.of(1, 2, 5),
						true
				},
				{
						Set.of(1, 2, 3, 4, 5, 6, 7),
						Set.of(1, 2, 5),
						false
				},
				{
						Set.of(1, 2, 3, 4, 5, 6, 7),
						Set.of(1, 2, 5, 7, 6),
						true
				}
		};
	}

	@ParameterizedTest
	@MethodSource("isQuorumParameters")
	void isQuorum(Set<Integer> all, Set<Integer> available, boolean expectQuorum) {
		assertThat(RaftUtils.isQuorum(all, available)).isEqualTo(expectQuorum);
	}
}
