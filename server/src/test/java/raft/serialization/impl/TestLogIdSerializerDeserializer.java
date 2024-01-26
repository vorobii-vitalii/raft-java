package raft.serialization.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import raft.dto.LogId;

class TestLogIdSerializerDeserializer {

	LogIdSerializerDeserializer logIdSerializerDeserializer = new LogIdSerializerDeserializer();

	@Test
	void serializeAndDeserialize() {
		var logId = LogId.newBuilder().setIndex(2).setTerm(4).build();
		assertThat(logIdSerializerDeserializer.deserialize(logIdSerializerDeserializer.serialize(logId)))
				.isEqualTo(logId);
	}

}
