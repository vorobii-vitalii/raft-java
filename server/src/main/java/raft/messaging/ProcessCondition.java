package raft.messaging;

public interface ProcessCondition {
	boolean shouldContinue();
}
