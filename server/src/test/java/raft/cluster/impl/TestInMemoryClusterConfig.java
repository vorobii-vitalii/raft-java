package raft.cluster.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

class TestInMemoryClusterConfig {
	private static final int CURRENT_SERVER_ID = 3;
	private static final Map<Integer, String> ADDRESS_BY_SERVER_ID = Map.of(
			1, "localhost:1234",
			2, "localhost:1235",
			CURRENT_SERVER_ID, "localhost:1236"
	);

	InMemoryClusterConfig clusterConfig = new InMemoryClusterConfig(ADDRESS_BY_SERVER_ID, CURRENT_SERVER_ID);

	@Test
	void getChannelByServerIdVerifyCaching() {
		var channel = clusterConfig.getChannelByServerId(1);
		assertThat(clusterConfig.getChannelByServerId(1)).isSameAs(channel);
	}

	@Test
	void getOtherServerIds() {
		assertThat(clusterConfig.getOtherServerIds()).containsExactlyInAnyOrder(1, 2);
	}

	@Test
	void getCurrentServerId() {
		assertThat(clusterConfig.getCurrentServerId()).isEqualTo(CURRENT_SERVER_ID);
	}

}
