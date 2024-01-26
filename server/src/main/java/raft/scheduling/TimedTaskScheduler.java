package raft.scheduling;

public interface TimedTaskScheduler {
	CancellableTask scheduleWithFixedDelay(int delayMs, Runnable action);
	CancellableTask scheduleAfter(int delayMs, Runnable action);
}
