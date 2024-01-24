package raft.scheduling;

import java.util.function.Supplier;

public interface TimedMessageSender<MSG_TYPE> {
	CancellableTask schedulePeriodical(int delayMs, Supplier<MSG_TYPE> messageSupplier);
	CancellableTask scheduleOnce(int delay, Supplier<MSG_TYPE> messageSupplier);
}
