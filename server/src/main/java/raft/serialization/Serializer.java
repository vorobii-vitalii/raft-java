package raft.serialization;

public interface Serializer<T> {
	byte[] serialize(T obj);
}
