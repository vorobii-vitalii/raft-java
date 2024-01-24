package raft.serialization.impl;

import com.google.protobuf.InvalidProtocolBufferException;

import raft.dto.LogId;
import raft.serialization.Deserializer;
import raft.serialization.Serializer;

public class LogIdSerializerDeserializer implements Serializer<LogId>, Deserializer<LogId> {
	@Override
	public byte[] serialize(LogId logId) {
		return logId.toByteArray();
	}

	@Override
	public LogId deserialize(byte[] arr) {
		try {
			return LogId.parseFrom(arr);
		}
		catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
	}
}
