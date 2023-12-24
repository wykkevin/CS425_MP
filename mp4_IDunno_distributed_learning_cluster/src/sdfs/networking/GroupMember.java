package sdfs.networking;

import org.json.simple.JSONObject;
import sdfs.JsonSerializable;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Data class for a node.
 */
public class GroupMember implements Comparable<GroupMember>, JsonSerializable {
    private InetAddress ip;
    private String joinTimestamp;
    private long ringId;

    public GroupMember(InetAddress ip) {
        this.ip = ip;
    }

    public GroupMember(String ipString, String joinTimestamp, long ringId) {
        try {
            this.ip = InetAddress.getByName(ipString);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        this.joinTimestamp = joinTimestamp;
        this.ringId = ringId;
    }

    public InetAddress getIp() {
        return ip;
    }

    public String getTimestamp() {
        return joinTimestamp;
    }

    public void setTimestamp(String joinTimestamp) {
        this.joinTimestamp = joinTimestamp;
    }

    public long getRingId() {
        return this.ringId;
    }

    public void setRingId(long ringId) {
        this.ringId = ringId;
    }

    public JSONObject toJson() {
        JSONObject output = new JSONObject();
        output.put("ip", ip.getHostName());
        output.put("joinTimestamp", joinTimestamp);
        output.put("ringId", ringId);
        return output;
    }

    public String toString() {
        return "ip: " + ip.getHostName() + " joinTimestamp: " + joinTimestamp + " ring id " + ringId;
    }

    @Override
    public int compareTo(GroupMember otherGroupMember) {
        return (int) (this.ringId - otherGroupMember.ringId);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (! (o instanceof GroupMember)) return false;
        return this.compareTo((GroupMember) o) == 0;
    }
}
