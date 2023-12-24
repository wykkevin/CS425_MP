import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * This class will work as both a client and a server. Therefore, we have a centralized place for shared information
 * like the member list. All the commands that we send and receive in the system will be handled here.
 */
public class UdpServent {
    private static final int PING_FREQUENCY_MS = 500;
    private static final int PING_TIMEOUT_MS = 3000;
    public static final int GROUP_PORT = 8002;
    public static final String MEMBER_LIST_SEPARATOR = ",";

    private List<GroupMember> members = new ArrayList<>();
    private ConnectionTopology connectionTopology;
    private final DatagramSocket socket;
    private final GroupMember localMember;
    private HashMap<InetAddress, ScheduledFuture<?>> pingThreadMap = new HashMap<>();
    private HashMap<InetAddress, ScheduledFuture<?>> pingTimeoutThreadMap = new HashMap<>();
    public boolean isJoined = false;
    private int dropRate = 0;

    public UdpServent(int dropRate) {
        try {
            this.dropRate = dropRate;

            InetAddress localIp = InetAddress.getLocalHost();
            localMember = new GroupMember(localIp);
            MemberGroupMain.LOGGER.info("Current ip is " + localIp);
            members.add(localMember);

            socket = new DatagramSocket(GROUP_PORT);
        } catch (UnknownHostException | SocketException e) {
            throw new RuntimeException(e);
        }
        // Always start the server
        runServer();
    }

    public GroupMember getLocalMember() {
        return localMember;
    }

    // Send the message by UDP to a fixed port in the destination ip address.
    public void sendMessage(Message message, InetAddress destinationIp) {
        try {
            // Generate a random number between 1 to 100. If the value is smaller or equal to the dropRate, we will drop the message.
            if (new Random().nextInt(100) + 1 <= dropRate) {
                MemberGroupMain.LOGGER.fine("Message " + message.getSystemCommand() + " is dropped");
            } else {
                MemberGroupMain.LOGGER.fine("Client sends " + message.getSystemCommand() + " to " + destinationIp);
                byte[] buf = message.getSystemCommand().getBytes();
                DatagramPacket queryPacket = new DatagramPacket(buf, buf.length, destinationIp, GROUP_PORT);
                socket.send(queryPacket);
            }
        } catch (IOException e) {
            MemberGroupMain.LOGGER.log(Level.SEVERE, "Error: sending message failed " + e);
        }
    }

    // Reset the data. Used when leave the group.
    public void reset() {
        stopAllCurrentPingThreads();
        members = new ArrayList<>();
        connectionTopology = new ConnectionTopology(members);
        isJoined = false;
        localMember.setTimestamp("");
        members.add(localMember);
    }

    public String printMemberList() {
        StringBuilder sb = new StringBuilder();
        sb.append("IP\t\tJOIN TIMESTAMP\n");
        for (GroupMember groupMember : members) {
            sb.append(groupMember.getIp().getHostAddress() + "\t" + groupMember.getTimestamp());
            sb.append("\n");
        }
        return sb.toString();
    }

    // Used by the grep command
    public void sendMessageToAllMembers(Message message) {
        for (GroupMember groupMember : members) {
            sendMessage(message, groupMember.getIp());
        }
    }

    // Get the target members and send message to all of them.
    public void sendMessageToAllTargetMembers(Message message) {
        connectionTopology = new ConnectionTopology(members);
        List<GroupMember> targetMembers = connectionTopology.getTargets(localMember.getIp());
        for (GroupMember groupMember : targetMembers) {
            sendMessage(message, groupMember.getIp());
        }
    }

    // We should also ping the target member in most of the situations.
    private void sendMessageToAllTargetMembersAndStartPing(Message message) {
        sendMessageToAllTargetMembers(message);
        pingTargetMembers();
    }

    // Server will be responsible for receiving the message. It will keep running.
    private void runServer() {
        Thread thread = new Thread(() -> {
            byte[] buf = new byte[256];
            try {
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);
                    InetAddress clientAddress = packet.getAddress();

                    String inputLine = new String(packet.getData(), 0, packet.getLength());
                    MemberGroupMain.LOGGER.fine("Input get from the server " + inputLine);

                    SystemCommand parsedSystemCommand = CommandParserUtil.parseSystemCommand(inputLine);
                    if (parsedSystemCommand == null) {
                        MemberGroupMain.LOGGER.info("Server receives an unsupported command " + inputLine);
                    } else {
                        if (isJoined) {
                            if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.GREP)) {
                                String grepCommand = parsedSystemCommand.content;
                                System.out.println("grep command received " + grepCommand);
                                List<String> commandResults = GrepQueryHandler.getQueryResults(grepCommand);
                                for (String commandResult : commandResults) {
                                    sendMessage(new Message(SystemCommandType.GREP_RESP, commandResult), clientAddress);
                                }
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.GREP_RESP)) {
                                System.out.println(parsedSystemCommand.content);
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.CLIENT_JOIN)) {
                                // Return a list of available members in the group. Only introducer is expected to get this.
                                MemberGroupMain.LOGGER.info("client address " + clientAddress.getHostAddress() + " is joining through introducer");
                                sendMessage(new Message(SystemCommandType.ROUTE_JOIN, encodeMemberList()), clientAddress);
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.JOIN)) {
                                MemberGroupMain.LOGGER.info("joining client address " + parsedSystemCommand.content);
                                // Check if the new node is included in the current members
                                if (!isMemberInMembers(parsedSystemCommand.content)) {
                                    List<String> memberInfo = CommandParserUtil.parseMemberId(parsedSystemCommand.content);
                                    members.add(new GroupMember(InetAddress.getByName(memberInfo.get(0)), memberInfo.get(1)));
                                    // Send reply to the joining node
                                    sendMessage(new Message(SystemCommandType.SUCCESS_JOIN, encodeMemberList()), clientAddress);
                                    // Share JOIN with other target members
                                    sendMessageToAllTargetMembersAndStartPing(new Message(SystemCommandType.SHARE_JOIN, parsedSystemCommand.content));
                                }
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.SHARE_JOIN)) {
                                MemberGroupMain.LOGGER.info("join_share client address " + parsedSystemCommand.content);
                                // Check if the new node is included in the current members
                                if (!isMemberInMembers(parsedSystemCommand.content)) {
                                    List<String> memberInfo = CommandParserUtil.parseMemberId(parsedSystemCommand.content);
                                    members.add(new GroupMember(InetAddress.getByName(memberInfo.get(0)), memberInfo.get(1)));
                                    // Share JOIN with other target members
                                    sendMessageToAllTargetMembersAndStartPing(new Message(SystemCommandType.SHARE_JOIN, parsedSystemCommand.content));
                                }
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.LEAVE)) {
                                MemberGroupMain.LOGGER.info("leaving client address " + parsedSystemCommand.content);
                                if (isIpInMembers(parsedSystemCommand.content)) {
                                    removeMemberAndShareWithGroup(parsedSystemCommand.content);
                                }
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.PING)) {
                                MemberGroupMain.LOGGER.fine("Ping is received, sending back pong to " + clientAddress.getHostAddress());
                                sendMessage(new Message(SystemCommandType.PONG, null), clientAddress);
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.PONG)) {
                                // Cancel the timeout thread
                                MemberGroupMain.LOGGER.fine("Pong is received, canceling the timeout");
                                ScheduledFuture<?> pingTimeoutThread = pingTimeoutThreadMap.get(clientAddress);
                                if (pingTimeoutThread != null) {
                                    pingTimeoutThread.cancel(true);
                                    pingTimeoutThreadMap.remove(clientAddress);
                                }
                            } else {
                                MemberGroupMain.LOGGER.info("Command not handled " + inputLine);
                            }
                        } else {
                            // We will handle these two commands when the node is not joined into the group yet.
                            if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.ROUTE_JOIN)) {
                                // Receive the route information from introducer. Will try to join through one of the ips.
                                MemberGroupMain.LOGGER.info("get routed addresses " + parsedSystemCommand.content);
                                List<GroupMember> groupMembers = CommandParserUtil.parseMemberListFromCommand(parsedSystemCommand);
                                int memberCount = groupMembers.size();
                                GroupMember randomMember = groupMembers.get(new Random().nextInt(memberCount));
                                sendMessage(new Message(SystemCommandType.JOIN, localMember.getId()), randomMember.getIp());
                            } else if (parsedSystemCommand.systemCommandType.equals(SystemCommandType.SUCCESS_JOIN)) {
                                // update local member list based on the message
                                MemberGroupMain.LOGGER.info("Successfully joined");
                                members = CommandParserUtil.parseMemberListFromCommand(parsedSystemCommand);
                                isJoined = true;
                                pingTargetMembers();
                            } else {
                                // We might get some other messages when the node is not in the group. We will ignore those messages.
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
    }

    // Cancel all the current ping threads.
    private void stopAllCurrentPingThreads() {
        MemberGroupMain.LOGGER.fine("Stop all ping threads");
        for (InetAddress key : pingThreadMap.keySet()) {
            pingThreadMap.get(key).cancel(true);
        }
        for (InetAddress key : pingTimeoutThreadMap.keySet()) {
            pingTimeoutThreadMap.get(key).cancel(true);
        }
        pingThreadMap = new HashMap<>();
        pingTimeoutThreadMap = new HashMap<>();
    }

    // Cancel the current ping threads for the ip.
    private void stopPingThreadsToIp(InetAddress ip) {
        MemberGroupMain.LOGGER.fine("Stop the thread to " + ip);
        ScheduledFuture<?> pingThread = pingThreadMap.get(ip);
        if (pingThread != null) {
            pingThread.cancel(true);
            pingThreadMap.remove(ip);
        }
        ScheduledFuture<?> pingTimeoutThread = pingTimeoutThreadMap.get(ip);
        if (pingTimeoutThread != null) {
            pingTimeoutThread.cancel(true);
            pingTimeoutThreadMap.remove(ip);
        }
    }

    // Start the ping to target members. This will stop all the current pings, calculate the target members based on the
    // current information, and start to ping them.
    private void pingTargetMembers() {
        stopAllCurrentPingThreads();

        connectionTopology = new ConnectionTopology(members);
        List<GroupMember> targetMembers = connectionTopology.getTargets(localMember.getIp());
        for (GroupMember groupMember : targetMembers) {
            MemberGroupMain.LOGGER.info("Starting to ping " + groupMember.getIp().getHostAddress());
            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            ScheduledFuture<?> pingTaskHandler = scheduledExecutorService.scheduleAtFixedRate(() -> {
                // We need to start the timeout handler first to avoid the issue that the pong is received before the
                // handler is initiated. Only start if there is no current timeout task running.
                if (pingTimeoutThreadMap.get(groupMember.getIp()) == null) {
                    ScheduledFuture<?> pingTimeoutTaskHandler = scheduledExecutorService.schedule(() -> {
                        MemberGroupMain.LOGGER.info("Timeout is found for " + groupMember.getIp().getHostAddress());
                        stopPingThreadsToIp(groupMember.getIp());
                        removeMemberAndShareWithGroup(groupMember.getIp().getHostAddress());
                    }, PING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    pingTimeoutThreadMap.put(groupMember.getIp(), pingTimeoutTaskHandler);
                }

                MemberGroupMain.LOGGER.fine("Ping is sent to " + groupMember.getIp().getHostAddress());
                sendMessage(new Message(SystemCommandType.PING, null), groupMember.getIp());
            }, 0, PING_FREQUENCY_MS, TimeUnit.MILLISECONDS);
            pingThreadMap.put(groupMember.getIp(), pingTaskHandler);
        }
    }

    private void removeMemberAndShareWithGroup(String ip) {
        try {
            stopPingThreadsToIp(InetAddress.getByName(ip));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        // update local information regarding the member that left.
        Iterator<GroupMember> it = members.iterator();
        while (it.hasNext()) {
            GroupMember groupMember = it.next();
            if (groupMember.getIp().getHostAddress().equals(ip)) {
                it.remove();
                break;
            }
        }
        // Share LEAVE with other target members
        sendMessageToAllTargetMembersAndStartPing(new Message(SystemCommandType.LEAVE, ip));
    }

    private boolean isMemberInMembers(String memberId) {
        List<String> parsedResult = CommandParserUtil.parseMemberId(memberId);
        String ip = parsedResult.get(0);
        for (GroupMember groupMember : members) {
            if (groupMember.getIp().getHostAddress().equals(ip)) {
                return true;
            }
        }
        return false;
    }

    private boolean isIpInMembers(String ip) {
        for (GroupMember groupMember : members) {
            if (groupMember.getIp().getHostAddress().equals(ip)) {
                return true;
            }
        }
        return false;
    }

    private String encodeMemberList() {
        StringBuilder sb = new StringBuilder();
        for (GroupMember groupMember : members) {
            sb.append(groupMember.getId());
            sb.append(MEMBER_LIST_SEPARATOR);
        }
        return sb.toString();
    }
}
