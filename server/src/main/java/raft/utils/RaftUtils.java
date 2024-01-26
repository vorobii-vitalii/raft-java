package raft.utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Set;

public final class RaftUtils {

	private RaftUtils() {
		// Utility classes should not be instantiated
	}

	public static <T> boolean isQuorum(Set<T> allServers, Set<T> available) {
		return available.size() * 2 > allServers.size();
	}

}
