package raft.serialization;

public interface Deserializer<T> {
	T deserialize(byte[] arr);
}
