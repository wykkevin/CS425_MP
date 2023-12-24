package sdfs.networking;

import java.util.*;

import static sdfs.networking.UdpServent.LOGGER;

/**
 * This class will build the topology of the group as a ring.
 */
public class ConnectionTopology {

    public static final int RING_SIZE = 256;
    private List<GroupMember> memberList = new ArrayList<>();
    private Set<Long> occupiedIds = new HashSet<>();

    public ConnectionTopology() {
    }

    // Only use this constructor for a list of member with calculated ringIds
    public ConnectionTopology(List<GroupMember> memberList) {
        this.memberList.addAll(memberList);
        for (GroupMember member : memberList) {
            occupiedIds.add(member.getRingId());
        }
        Collections.sort(memberList); // sorted by ringId
    }

    public List<GroupMember> getMemberList() {
        return memberList;
    }

    public void addMember(GroupMember groupMember) {
        memberList.add(groupMember);
        Collections.sort(memberList); // sorted by ringId
    }

    public void removeMember(GroupMember member) {
        memberList.remove(member);
        occupiedIds.remove(member.getRingId());
    }

    public GroupMember setMemberWithRingId(GroupMember groupMember) {
        long ringId = getMemberRingId(groupMember);
        while (!occupiedIds.add(ringId)) {
            ringId = (ringId + 1) % RING_SIZE;
        }
        groupMember.setRingId(ringId);
        return groupMember;
    }

    public long getMemberRingId(GroupMember groupMember) {
        String uniqueID = groupMember.getIp().getHostAddress();
        return Math.abs(uniqueID.hashCode()) % RING_SIZE;
    }

    /**
     * Get the list of group members that the given group member should connect to.
     * We will connect to one predecessor and two successors.
     *
     * @param groupMember A group member.
     * @return A list of members that will be the targets of the IP without duplicates.
     */
    public List<GroupMember> getTargets(GroupMember groupMember) {
        int myIndex = getCurrentMemberIndex(groupMember);

        List<GroupMember> resultList = new ArrayList<>();
        if (myIndex != -1) {
            Set<Integer> indexes = new HashSet<>();
            int predecessorIndex = (myIndex - 1 + memberList.size()) % memberList.size();
            if (predecessorIndex != myIndex) {
                indexes.add(predecessorIndex);
            }
            int firstSuccessorIndex = (myIndex + 1) % memberList.size();
            if (firstSuccessorIndex != myIndex) {
                indexes.add(firstSuccessorIndex);
            }
            int secondSuccessorIndex = (myIndex + 2) % memberList.size();
            if (secondSuccessorIndex != myIndex) {
                indexes.add(secondSuccessorIndex);
            }
            for (int index : indexes) {
                LOGGER.fine("Target member " + memberList.get(index).getIp().getHostName() + " at index " + index);
                resultList.add(memberList.get(index));
            }
        } else {
            LOGGER.warning("Error: current ip is not found in the member list");
        }
        return resultList;
    }

    public GroupMember getSuccessor(GroupMember groupMember) {
        int myIndex = getCurrentMemberIndex(groupMember);
        int firstSuccessorIndex = (myIndex + 1) % memberList.size();
        return memberList.get(firstSuccessorIndex);
    }

    @Override
    public String toString() {
        return "ConnectionTopology{" + "ring=" + memberList + "}";
    }

    public GroupMember getTargetNodeForFile(String filePath) {
        int hashCode = filePath.hashCode() % RING_SIZE;
        // Find the node has the vmNumber or the next node that has the number larger than the expected vmNumber.
        for (int i = 0; i < memberList.size(); i++) {
            if (memberList.get(i).getRingId() > hashCode) {
                return memberList.get(i);
            }
        }
        return memberList.get(memberList.size() - 1);
    }

    private int getCurrentMemberIndex(GroupMember groupMember) {
        for (int i = 0; i < memberList.size(); i++) {
            if (memberList.get(i).getRingId() == groupMember.getRingId()) {
                return i;
            }
        }
        LOGGER.warning(groupMember + " is not found in current memberList");
        return -1;
    }
}
