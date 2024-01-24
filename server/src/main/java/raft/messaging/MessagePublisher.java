package raft.messaging;

public interface MessagePublisher<T> {
	void publish(T msg);
}
