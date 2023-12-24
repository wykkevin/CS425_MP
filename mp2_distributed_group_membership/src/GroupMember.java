import java.net.InetAddress;

/**
 * Data class for a node.
 */
public class GroupMember {
    public static String MEMBER_ID_SEPARATOR = "@";
    private InetAddress ip;
    private String joinTimestamp;

    public GroupMember(InetAddress ip) {
        this.ip = ip;
    }

    public GroupMember(InetAddress ip, String joinTimestamp) {
        this.ip = ip;
        this.joinTimestamp = joinTimestamp;
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

    // Id is the combination of ip address and join timestamp
    public String getId() {
        return ip.getHostAddress() + MEMBER_ID_SEPARATOR + joinTimestamp;
    }
}
