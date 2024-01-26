package raft.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestLogUtils {

	@Test
	void isOtherLogAtLeastAsNew() {
		assertThat(LogUtils.isOtherLogAtLeastAsNew(null, null)).isTrue();
	}
}