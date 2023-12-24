import java.net.InetAddress;
import java.util.*;

/**
 * This class will build the topology of the group as a ring.
 */
public class ConnectionTopology {

    private List<GroupMember> memberList;

    /**
     * Constructor for a connection topology that each client build locally.
     *
     * @param memberList List of members that is final (including elements and order) when ConnectionTopology is built
     */
    public ConnectionTopology(List<GroupMember> memberList) {
        this.memberList = memberList;
    }

    /**
     * Get the list of group members that the given group member should connect to.
     * We will connect to one predecessor and two successors.
     *
     * @param ip An identification for a group member. Currently, we use its IP.
     * @return A list of members that will be the targets of the IP without duplicates.
     */
    public List<GroupMember> getTargets(InetAddress ip) {
        int myIpIndex = -1;
        for (int i = 0; i < memberList.size(); i++) {
            if (memberList.get(i).getIp().equals(ip)) {
                myIpIndex = i;
            }
        }

        List<GroupMember> resultList = new ArrayList<>();
        if (myIpIndex != -1) {
            Set<Integer> indexes = new HashSet<>();
            int predecessorIndex = (myIpIndex - 1 + memberList.size()) % memberList.size();
            if (predecessorIndex != myIpIndex) {
                indexes.add(predecessorIndex);
            }
            int firstSuccessorIndex = (myIpIndex + 1) % memberList.size();
            if (firstSuccessorIndex != myIpIndex) {
                indexes.add(firstSuccessorIndex);
            }
            int secondSuccessorIndex = (myIpIndex + 2) % memberList.size();
            if (secondSuccessorIndex != myIpIndex) {
                indexes.add(secondSuccessorIndex);
            }
            for (int index : indexes) {
                MemberGroupMain.LOGGER.info("Target member " + memberList.get(index).getIp().getHostAddress() + " at index " + index);
                resultList.add(memberList.get(index));
            }
        } else {
            MemberGroupMain.LOGGER.warning("Error: current ip is not found in the member list");
        }
        return resultList;
    }
}
