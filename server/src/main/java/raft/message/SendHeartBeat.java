package raft.message;

public record SendHeartBeat() implements RaftMessage {
	@Override
	public RaftMessageType getType() {
		return RaftMessageType.SEND_HEART_BEAT;
	}
}
