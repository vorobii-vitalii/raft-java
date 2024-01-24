package raft.scheduling.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import raft.scheduling.CancellableTask;
import raft.scheduling.TimedTaskScheduler;

public class TimedTaskSchedulerImpl implements TimedTaskScheduler {
	private final ScheduledExecutorService scheduledExecutorService;

	public TimedTaskSchedulerImpl(ScheduledExecutorService scheduledExecutorService) {
		this.scheduledExecutorService = scheduledExecutorService;
	}

	@Override
	public CancellableTask scheduleWithFixedDelay(int delayMs, Runnable action) {
		return toCancellableTask(scheduledExecutorService.scheduleWithFixedDelay(action, delayMs, delayMs, TimeUnit.MILLISECONDS));
	}

	@Override
	public CancellableTask scheduleAfter(int delayMs, Runnable action) {
		return toCancellableTask(scheduledExecutorService.schedule(action, delayMs, TimeUnit.MILLISECONDS));
	}

	private CancellableTask toCancellableTask(ScheduledFuture<?> future) {
		return () -> future.cancel(true);
	}

}
