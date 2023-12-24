package sdfs.networking;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import sdfs.GrepQueryHandler;
import sdfs.SdfsFileMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

/**
 * This class will work as both a client and a server. Therefore, we have a centralized place for shared information
 * like the member list. All the commands that we send and receive in the system will be handled here.
 */
public class UdpServent {
    private static final int PING_FREQUENCY_MS = 1000;
    private static final int PING_TIMEOUT_MS = 3000;
    private static final int MASTER_SANITY_CHECK_SECONDS = 3;
    public static final int GROUP_PORT = 8012; // Used to communicate membership in the system
    public static final int JOIN_PORT = 8013; // Used to communicate with external nodes
    public static final int FILE_PORT = 8014; // Used to communicate files in the system
    public static final int FAILURE_DETECTOR_PORT = 8015; // Used for ping and pong in failure detector
    private static final String LOCAL_DIRECTORY = "LocalDir/";
    public static String Db_DIRECTORY = "Db/";
    private static final String REMOTE_DIRECTORY = System.getProperty("user.dir") + "/";
    public static final int NUMBER_OF_REPLICAS = 3;
    private static final String FILE_VERSION_DELIMITER = "#";

    private MasterInfo masterInfo; // Master will handle file related messages
    private ConnectionTopology connectionTopology = new ConnectionTopology();
    private final DatagramSocket membershipSocket;
    private final DatagramSocket joinSocket;
    private final DatagramSocket fileSocket;
    private final DatagramSocket failureDetectorSocket;
    private GroupMember localMember;
    private HashMap<InetAddress, ScheduledFuture<?>> pingThreadMap = new HashMap<>();
    private HashMap<InetAddress, ScheduledFuture<?>> pingTimeoutThreadMap = new HashMap<>();
    public boolean isJoined = false;
    public boolean isJoining = false;
    private HashMap<String, SdfsFileMetadata> fileMetadata = new HashMap<>();
    public Set<String> storedFiles = new HashSet<>();
    private boolean isAddingFile = false;
    private boolean isGettingFile = false;
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    Future<?> pongTaskHandler;
    private boolean isElectionInProgress = false;
    private long cachedMaximumElectionInitiatorId = -1;

    public static final Logger LOGGER;

    static {
        LOGGER = Logger.getLogger(UdpServent.class.getName());
        try {
            System.setProperty("java.util.logging.SimpleFormatter.format",
                    "[%1$tF %1$tT] [%4$-7s] %5$s %n");
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("uuuuMMddHHmmss");
            LocalDateTime now = LocalDateTime.now();
            FileHandler fh = new FileHandler(System.getProperty("user.dir") + "/logFiles/log_" + dtf.format(now) + ".log");
            fh.setLevel(Level.FINE);
            fh.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fh);
            LOGGER.setLevel(Level.FINE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public UdpServent() {
        try {
            InetAddress localIp = InetAddress.getLocalHost();
            localMember = new GroupMember(localIp);
            LOGGER.info("Current ip is " + localIp.getHostName());

            membershipSocket = new DatagramSocket(GROUP_PORT);
            joinSocket = new DatagramSocket(JOIN_PORT);
            fileSocket = new DatagramSocket(FILE_PORT);
            failureDetectorSocket = new DatagramSocket(FAILURE_DETECTOR_PORT);
        } catch (UnknownHostException | SocketException e) {
            throw new RuntimeException(e);
        }
        // Always start the servers. They will be responsible for receiving the message. It will keep running.
        runMembershipServer();
        runJoinServer();
        runFileServer();
    }

    public GroupMember getLocalMember() {
        return localMember;
    }

    public MasterInfo getMaster() {
        return masterInfo;
    }

    public void initiateIntroducer() {
        isJoined = true;
        localMember.setTimestamp(String.valueOf(System.currentTimeMillis()));
        localMember = connectionTopology.setMemberWithRingId(localMember);
        connectionTopology.addMember(localMember);
        // Let the introducer also be the default master
        masterInfo = new MasterInfo(localMember);
        startThreadForReconstructingFileAndMetadata();
    }

    // Send the message by UDP to a fixed port in the destination ip address.
    public void sendMessage(String message, InetAddress destinationIp, int port) {
        try {
            LOGGER.fine("Client sends " + message + " to " + destinationIp + " at " + port);
            byte[] buf = message.getBytes();
            DatagramPacket queryPacket = new DatagramPacket(buf, buf.length, destinationIp, port);
            if (port == GROUP_PORT) {
                membershipSocket.send(queryPacket);
            } else if (port == JOIN_PORT) {
                joinSocket.send(queryPacket);
            } else if (port == FILE_PORT) {
                fileSocket.send(queryPacket);
            } else if (port == FAILURE_DETECTOR_PORT) {
                failureDetectorSocket.send(queryPacket);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error: sending message failed " + e);
        }
    }

    // Reset the data. Used when leave the group.
    public void reset() {
        stopAllCurrentPingThreads();
        connectionTopology = new ConnectionTopology();
        isJoined = false;
        localMember.setTimestamp("");
        fileMetadata = new HashMap<>();
        storedFiles = new HashSet<>();
        isElectionInProgress = false;
        cachedMaximumElectionInitiatorId = -1;
        masterInfo = new MasterInfo();
    }

    public String printMemberList() {
        StringBuilder sb = new StringBuilder();
        sb.append("IP\t\t\tJOIN TIMESTAMP\t\tRING ID\n");
        for (GroupMember groupMember : connectionTopology.getMemberList()) {
            sb.append(groupMember.getIp().getHostName() + "\t" + groupMember.getTimestamp() + "\t" + groupMember.getRingId() + "\n");
        }
        return sb.toString();
    }

    // Used by the grep command
    public void sendMessageToAllMembers(String message, int port) {
        for (GroupMember groupMember : connectionTopology.getMemberList()) {
            sendMessage(message, groupMember.getIp(), port);
        }
    }

    // Get the target members and send message to all of them.
    public void sendMessageToAllTargetMembers(String message, int port) {
        List<GroupMember> targetMembers = connectionTopology.getTargets(localMember);
        for (GroupMember groupMember : targetMembers) {
            sendMessage(message, groupMember.getIp(), port);
        }
    }

    public boolean sendMessageToMaster(String message, int port) {
        if (masterInfo != null && !isElectionInProgress) {
            sendMessage(message, masterInfo.getMasterGroupMember().getIp(), port);
            return true;
        }
        return false;
    }

    // We should also ping the target member in most of the situations.
    private void sendMessageToAllTargetMembersAndStartPing(String message, int port) {
        sendMessageToAllTargetMembers(message, port);
        pingTargetMembers();
    }

    private void sendMessageToSuccessor(String message, int port) {
        sendMessage(message, connectionTopology.getSuccessor(localMember).getIp(), port);
    }

    private void runMembershipServer() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            byte[] buf = new byte[2048];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    membershipSocket.receive(packet);
                    InetAddress clientAddress = packet.getAddress();

                    String inputLine = new String(packet.getData(), 0, packet.getLength());
                    LOGGER.fine("Group server input from [" + clientAddress.getHostName() + "]: " + inputLine);

                    JSONParser jsonParser = new JSONParser();
                    JSONObject resultObject = (JSONObject) jsonParser.parse(inputLine);
                    String messageType = (String) resultObject.get("command");

                    if (messageType == null) {
                        LOGGER.warning("Server receives an unsupported command " + inputLine);
                    } else {
                        JSONObject newMessageJsonObject = new JSONObject();
                        if (isJoined) {
                            if (messageType.equals(MessageType.GREP.toString())) {
                                String grepCommand = (String) resultObject.get("grep");
                                System.out.println("grep command received " + grepCommand);
                                List<String> commandResults = GrepQueryHandler.getQueryResults(grepCommand);
                                for (String commandResult : commandResults) {
                                    newMessageJsonObject.put("command", MessageType.GREP_RESP.toString());
                                    newMessageJsonObject.put("grepResponse", commandResult);
                                    sendMessage(newMessageJsonObject.toString(), clientAddress, GROUP_PORT);
                                }
                            } else if (messageType.equals(MessageType.GREP_RESP.toString())) {
                                System.out.println((String) resultObject.get("grepResponse"));
                            } else if (messageType.equals(MessageType.SHARE_JOIN.toString())) {
                                GroupMember member = CommandParserUtil.decodeMember((JSONObject) resultObject.get("member"));
                                LOGGER.info("join_share client address " + member);
                                // Check if the new node is included in the current members
                                if (!isMemberInMembers(member)) {
                                    connectionTopology.addMember(member);
                                    // Share JOIN with other target members
                                    newMessageJsonObject.put("command", MessageType.SHARE_JOIN.toString());
                                    newMessageJsonObject.put("member", member.toJson());
                                    sendMessageToAllTargetMembersAndStartPing(newMessageJsonObject.toString(), GROUP_PORT);
                                }
                            } else if (messageType.equals(MessageType.LEAVE.toString())) {
                                String leavingIp = (String) resultObject.get("ip");
                                LOGGER.info("leaving client address " + leavingIp);
                                // If this is master, remove the leaving member from backup metadata store locations
                                // and send it to a new one.
                                if (isIpInMembers(leavingIp)) {
                                    removeMemberAndShareWithGroup(leavingIp);
                                }

                                if (localMember.compareTo(masterInfo.getMasterGroupMember()) == 0) {
                                    List<String> backupMetadataStoreLocations = masterInfo.getBackupMetadataStoreLocations();
                                    Iterator<String> locationIterator = backupMetadataStoreLocations.iterator();
                                    while (locationIterator.hasNext()) {
                                        String ip = locationIterator.next();
                                        if (ip.equals(leavingIp)) {
                                            locationIterator.remove();
                                        }
                                    }
                                    masterInfo.setBackupMetadataStoreLocations(backupMetadataStoreLocations);
                                    reconstructFileMetadataBackupLocations();
                                }

                            } else if (messageType.equals(MessageType.ELECTION.toString())) {
                                GroupMember electingMember = CommandParserUtil.decodeMember((JSONObject) resultObject.get("electingMember"));
                                LOGGER.info("Received election message for " + electingMember.getIp().getHostName()
                                        + ", with id " + electingMember.getRingId() + ", local member id " + localMember.getRingId());
                                if (electingMember.getRingId() == localMember.getRingId()) {
                                    LOGGER.info("Current member is elected as the leader. Broadcasting elected message.");
                                    // Election success, send message: elected this member to successor.
                                    this.masterInfo.setMasterGroupMember(localMember);
                                    newMessageJsonObject.put("command", MessageType.ELECTED.toString());
                                    newMessageJsonObject.put("electedMember", localMember.toJson());
                                    sendMessageToSuccessor(newMessageJsonObject.toJSONString(), GROUP_PORT);
                                } else {
                                    // only process election message if the initiator id of current election is
                                    // greater than or equal to cached maximum
                                    long currentElectionInitiatorId = (long) resultObject.get("initiator");
                                    if (currentElectionInitiatorId < cachedMaximumElectionInitiatorId) {
                                        LOGGER.info("Suppressed election with initiator id " + currentElectionInitiatorId + " which is less than cached ID " + cachedMaximumElectionInitiatorId);
                                    } else {
                                        // Forward election message with maximum id
                                        LOGGER.info("Forwarding election message.");
                                        newMessageJsonObject.put("initiator", currentElectionInitiatorId);
                                        newMessageJsonObject.put("command", MessageType.ELECTION.toString());
                                        if (electingMember.getRingId() >= localMember.getRingId()) {
                                            newMessageJsonObject.put("electingMember", electingMember.toJson());
                                        } else {
                                            newMessageJsonObject.put("electingMember", localMember.toJson());
                                        }
                                        // cache initiator id and start election process if not yet
                                        this.isElectionInProgress = true;
                                        this.cachedMaximumElectionInitiatorId = Math.max(currentElectionInitiatorId, cachedMaximumElectionInitiatorId);
                                        sendMessageToSuccessor(newMessageJsonObject.toJSONString(), GROUP_PORT);
                                    }
                                }
                            } else if (messageType.equals(MessageType.ELECTED.toString())) {
                                GroupMember electedMember = CommandParserUtil.decodeMember((JSONObject) resultObject.get("electedMember"));
                                LOGGER.info("Received notice of member" + electedMember.getIp().getHostName() + "is elected");
                                masterInfo.setMasterGroupMember(electedMember);
                                if (electedMember.compareTo(localMember) == 0) {
                                    LOGGER.info("Elected message has reached every other member in the ring. Requesting backup fileMetadata from " + masterInfo.getBackupMetadataStoreLocations());
                                    List<String> backupMetadataStoreLocations = masterInfo.getBackupMetadataStoreLocations();

                                    Iterator<String> locationIterator = backupMetadataStoreLocations.iterator();
                                    while (locationIterator.hasNext()) {
                                        String ip = locationIterator.next();
                                        List<String> memberIpList = connectionTopology.getMemberList().stream().map(groupMember -> groupMember.getIp().getHostName()).collect(Collectors.toList());
                                        if (!memberIpList.contains(ip)) {
                                            locationIterator.remove();
                                        }
                                        newMessageJsonObject.put("command", MessageType.REQUEST_BACKUP_METADATA.toString());
                                        sendMessage(newMessageJsonObject.toJSONString(), InetAddress.getByName(ip), GROUP_PORT);
                                        if (ip.equals(localMember.getIp().getHostName())) {
                                            locationIterator.remove();
                                        }
                                    }
                                    masterInfo.setBackupMetadataStoreLocations(backupMetadataStoreLocations);
                                } else {
                                    sendMessageToSuccessor(inputLine, GROUP_PORT);
                                }
                            } else if (messageType.equals(MessageType.REQUEST_BACKUP_METADATA.toString())) {
                                if (!clientAddress.getHostName().equals(localMember.getIp().getHostName())) {
                                    LOGGER.info("Sending backup metadata back to new master.");
                                    // put fileMetadata back to sender (new master).
                                    MasterInfo.sendFileMetadataTo(clientAddress, fileMetadata);
                                }
                            } else if (messageType.equals(MessageType.PUT_BACKUP_METADATA.toString())) {
                                if (localMember.compareTo(masterInfo.getMasterGroupMember()) != 0) {
                                    // If you are not master, simply decode metadata and store at local. You have chosen to be
                                    // one of the fileMetadata backups by the master.
                                    fileMetadata = CommandParserUtil.decodeFileMetadata(resultObject);
                                } else {
                                    // if local = master, this is the response from a backup after requesting fileMetadata.
                                    //  verify fileMetadata and broadcast end election message.
                                    fileMetadata = CommandParserUtil.decodeFileMetadata(resultObject);
                                    startThreadForReconstructingFileAndMetadata();
                                    // Reconstruct backup metadata store locations
                                    reconstructFileMetadataBackupLocations();
                                    newMessageJsonObject.put("command", MessageType.END_ELECTION.toString());
                                    sendMessageToAllMembers(newMessageJsonObject.toJSONString(), GROUP_PORT);
                                }
                            } else if (messageType.equals(MessageType.END_ELECTION.toString())) {
                                LOGGER.info("Election ended. Resume to normal file operations.");
                                isElectionInProgress = false;
                                cachedMaximumElectionInitiatorId = -1;
                            }
                        }
                    }
                } catch (IOException | ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void reconstructFileMetadataBackupLocations() {
        LOGGER.fine("Current backup locations : " + masterInfo.getBackupMetadataStoreLocations());
        while (masterInfo.getBackupMetadataStoreLocations().size() < Math.min(NUMBER_OF_REPLICAS, connectionTopology.numberOfMembers())) {
            GroupMember randomAliveMember = null;
            while (randomAliveMember == null || randomAliveMember.compareTo(localMember) == 0 || masterInfo.getBackupMetadataStoreLocations().contains(randomAliveMember)) {
                LOGGER.fine(" isinbackup" + ((Boolean) (masterInfo.getBackupMetadataStoreLocations().contains(randomAliveMember))));
                randomAliveMember = connectionTopology.getMemberList().get(new Random().nextInt(connectionTopology.numberOfMembers()));
                LOGGER.fine("trying to add " + randomAliveMember);
            }
            LOGGER.fine("decided to add" + randomAliveMember);
            masterInfo.addToBackupMetadataStoreLocations(randomAliveMember.getIp().getHostName());
        }
        LOGGER.fine("Reconstructed backup locations : " + masterInfo.getBackupMetadataStoreLocations());
        sendFileMetadataToAllBackupLocations();
    }

    private void runJoinServer() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            byte[] buf = new byte[2048];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    joinSocket.receive(packet);
                    InetAddress clientAddress = packet.getAddress();

                    String inputLine = new String(packet.getData(), 0, packet.getLength());
                    LOGGER.fine("Join server input from [" + clientAddress.getHostName() + "]: " + inputLine);

                    JSONParser jsonParser = new JSONParser();
                    JSONObject resultObject = (JSONObject) jsonParser.parse(inputLine);
                    String messageType = (String) resultObject.get("command");
                    if (messageType == null) {
                        LOGGER.warning("Server receives an unsupported command " + inputLine);
                    } else {
                        JSONObject newMessageJsonObject = new JSONObject();
                        if (isJoined) {
                            if (messageType.equals(MessageType.CLIENT_JOIN.toString())) {
                                // A client want to join, return my ip to it.
                                LOGGER.info("client address " + clientAddress.getHostName() + " wants to join");
                                newMessageJsonObject.put("command", MessageType.ALLOW_JOIN.toString());
                                newMessageJsonObject.put("member", localMember.toJson());
                                sendMessage(newMessageJsonObject.toString(), clientAddress, JOIN_PORT);
                            } else if (messageType.equals(MessageType.JOIN.toString())) {
                                GroupMember newMember = CommandParserUtil.decodeMember((JSONObject) resultObject.get("member"));
                                LOGGER.info("joining client address " + newMember);
                                // Check if the new node is included in the current members
                                if (!isMemberInMembers(newMember)) {
                                    GroupMember updatedNewMember = connectionTopology.setMemberWithRingId(newMember);
                                    connectionTopology.addMember(updatedNewMember);

                                    if (connectionTopology.numberOfMembers() < 1 + NUMBER_OF_REPLICAS) {
                                        LOGGER.info("Adding to backupMetadataStoreLocations and sending backup file metadata to " + clientAddress);
                                        masterInfo.addToBackupMetadataStoreLocations(clientAddress.getHostName());
                                        MasterInfo.sendFileMetadataTo(clientAddress, fileMetadata);

                                    }


                                    // Send reply to the joining node
                                    newMessageJsonObject.put("command", MessageType.SUCCESS_JOIN.toString());
                                    newMessageJsonObject.put("memberList", CommandParserUtil.encodeMemberList(connectionTopology.getMemberList()));
                                    newMessageJsonObject.put("master", this.masterInfo.toJson());
                                    newMessageJsonObject.put("updatedMember", updatedNewMember.toJson());
                                    sendMessage(newMessageJsonObject.toString(), clientAddress, JOIN_PORT);

                                    // Share JOIN with other target members
                                    JSONObject anotherMessageJsonObject = new JSONObject();
                                    anotherMessageJsonObject.put("command", MessageType.SHARE_JOIN.toString());
                                    anotherMessageJsonObject.put("member", updatedNewMember.toJson());
                                    sendMessageToAllTargetMembersAndStartPing(anotherMessageJsonObject.toString(), GROUP_PORT);
                                }
                            } else {
                                LOGGER.info("Command not handled " + inputLine);
                            }
                        } else {
                            // We will handle these two commands when the node is not joined into the group yet.
                            if (messageType.equals(MessageType.ALLOW_JOIN.toString())) {
                                // Receive the address information from a node in the system. Will try to join the first one that responds.
                                GroupMember memberInSystem = CommandParserUtil.decodeMember((JSONObject) resultObject.get("member"));
                                LOGGER.info("get address of a member in the system " + memberInSystem + ". This node isJoining " + isJoining);
                                // Make sure only join once.
                                if (!isJoining) {
                                    isJoining = true;
                                    newMessageJsonObject.put("command", MessageType.JOIN.toString());
                                    newMessageJsonObject.put("member", localMember.toJson());
                                    sendMessage(newMessageJsonObject.toString(), memberInSystem.getIp(), JOIN_PORT);
                                }
                            } else if (messageType.equals(MessageType.SUCCESS_JOIN.toString())) {
                                // update local member list based on the message
                                LOGGER.info("Successfully joined");
                                List<GroupMember> members = CommandParserUtil.decodeMemberList((JSONArray) resultObject.get("memberList"));
                                localMember = CommandParserUtil.decodeMember((JSONObject) resultObject.get("updatedMember"));
                                connectionTopology = new ConnectionTopology(members);
                                masterInfo = CommandParserUtil.decodeMaster((JSONObject) resultObject.get("master"));
                                isJoined = true;
                                isJoining = false;
                                pingTargetMembers();
                            } else {
                                // We might get some other messages when the node is not in the group. We will ignore those messages.
                            }
                        }
                    }
                } catch (IOException | ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void runFileServer() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            byte[] buf = new byte[2048];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    fileSocket.receive(packet);
                    InetAddress clientAddress = packet.getAddress();

                    String inputLine = new String(packet.getData(), 0, packet.getLength());
                    LOGGER.fine("File server input from [" + clientAddress.getHostName() + "]: " + inputLine);

                    JSONParser jsonParser = new JSONParser();
                    JSONObject resultObject = (JSONObject) jsonParser.parse(inputLine);
                    String messageType = (String) resultObject.get("command");
                    if (messageType == null) {
                        LOGGER.warning("Server receives an unsupported command " + inputLine);
                    } else {
                        JSONObject newMessageJsonObject = new JSONObject();
                        if (isJoined) {
                            if (messageType.equals(MessageType.PUT.toString())) {
                                // Only file master should get this message. Master will decide where to put the file and send the result to the client.
                                isAddingFile = true;
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                String localFilePath = (String) resultObject.get("localFilePath");
                                long version = 0;
                                String memberIpString;
                                if (fileMetadata.containsKey(sdfsFilePath)) {
                                    version = fileMetadata.get(sdfsFilePath).getLatestVersion() + 1;
                                    memberIpString = fileMetadata.get(sdfsFilePath).getStoreLocations().get(0);
                                } else {
                                    memberIpString = connectionTopology.getTargetNodeForFile(sdfsFilePath).getIp().getHostName();
                                }
                                LOGGER.info("Member " + memberIpString + " will store file " + localFilePath + " with version " + version + " to " + sdfsFilePath);
                                newMessageJsonObject.put("command", MessageType.PUT_LOCATION.toString());
                                newMessageJsonObject.put("memberIpString", memberIpString);
                                newMessageJsonObject.put("localFilePath", localFilePath);
                                newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                newMessageJsonObject.put("version", version);
                                sendMessage(newMessageJsonObject.toString(), clientAddress, FILE_PORT);
                            } else if (messageType.equals(MessageType.PUT_LOCATION.toString())) {
                                // Client knows the put location and send the file to the VM.
                                String ipString = (String) resultObject.get("memberIpString");
                                InetAddress storingMemberIp = InetAddress.getByName(ipString);
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                String localFilePath = (String) resultObject.get("localFilePath");
                                long version = (Long) resultObject.get("version");
                                // LocalDir -> Db
                                int exitValue = sendFile(localFilePath, insertVersionInFileName(sdfsFilePath, version), storingMemberIp, true, false);
                                if (exitValue == 0) {
                                    // Succeed
                                    newMessageJsonObject.put("command", MessageType.FILE_UPLOADED.toString());
                                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                    newMessageJsonObject.put("neededReplicas", NUMBER_OF_REPLICAS);
                                    newMessageJsonObject.put("version", version);
                                    newMessageJsonObject.put("clientIp", localMember.getIp().getHostName());
                                    LOGGER.fine("Send file uploaded message to " + ipString);
                                    sendMessage(newMessageJsonObject.toString(), storingMemberIp, FILE_PORT);
                                } else {
                                    System.out.println("Uploading file local " + localFilePath + " failed with exit value " + exitValue);
                                }
                            } else if (messageType.equals(MessageType.FILE_UPLOADED.toString())) {
                                // Scp succeeds. Notify the receiver of the file.
                                long neededReplicas = (Long) resultObject.get("neededReplicas");
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                long version = (Long) resultObject.get("version");
                                String clientIp = (String) resultObject.get("clientIp");
                                LOGGER.fine("File " + sdfsFilePath + " version " + version + " is uploaded. NeededReplicas is " + neededReplicas);
                                storedFiles.add(sdfsFilePath);
                                // Notify master that file is received.
                                newMessageJsonObject.put("command", MessageType.FILE_RECEIVED.toString());
                                newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                newMessageJsonObject.put("storingMemberIp", localMember.getIp().getHostName());
                                newMessageJsonObject.put("version", version);
                                newMessageJsonObject.put("clientIp", clientIp);
                                sendMessageToMaster(newMessageJsonObject.toString(), FILE_PORT);
                                if (neededReplicas > 0) {
                                    // Send needed replicas to the successor.
                                    // Db -> Db
                                    int exitValue = sendFile(insertVersionInFileName(sdfsFilePath, version), insertVersionInFileName(sdfsFilePath, version), connectionTopology.getSuccessor(localMember).getIp(), false, false);
                                    if (exitValue == 0) {
                                        // Send to successor
                                        JSONObject anotherMessageJsonObject = new JSONObject();
                                        anotherMessageJsonObject.put("command", MessageType.FILE_UPLOADED.toString());
                                        anotherMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                        anotherMessageJsonObject.put("neededReplicas", neededReplicas - 1);
                                        anotherMessageJsonObject.put("version", version);
                                        anotherMessageJsonObject.put("clientIp", clientIp);
                                        sendMessageToSuccessor(anotherMessageJsonObject.toString(), FILE_PORT);
                                    } else {
                                        System.out.println("Uploading file sdfs " + sdfsFilePath + " failed with exit value " + exitValue);
                                    }
                                }
                            } else if (messageType.equals(MessageType.FILE_RECEIVED.toString())) {
                                // Only master should receive this. Master will add the information to metadata.
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                long version = (Long) resultObject.get("version");
                                String storingMemberIp = (String) resultObject.get("storingMemberIp");
                                String clientIp = (String) resultObject.get("clientIp");
                                LOGGER.info("Notification that file " + sdfsFilePath + " is stored at " + storingMemberIp);

                                SdfsFileMetadata newFileMetadata;
                                List<String> storingIps = new ArrayList<>();
                                if (fileMetadata.containsKey(sdfsFilePath)) {
                                    SdfsFileMetadata currentMetadata = fileMetadata.get(sdfsFilePath);
                                    if (currentMetadata.getLatestVersion() == version) {
                                        storingIps = fileMetadata.get(sdfsFilePath).getStoreLocations();
                                    }
                                }
                                if (!storingIps.contains(storingMemberIp)) {
                                    storingIps.add(storingMemberIp);
                                    newFileMetadata = new SdfsFileMetadata(version, storingIps);
                                    fileMetadata.put(sdfsFilePath, newFileMetadata);
                                    sendFileMetadataToAllBackupLocations();

                                    if (storingIps.size() == NUMBER_OF_REPLICAS + 1) {
                                        isAddingFile = false;
                                        newMessageJsonObject.put("command", MessageType.PUT_FINISHED.toString());
                                        newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                        sendMessage(newMessageJsonObject.toString(), InetAddress.getByName(clientIp), FILE_PORT);
                                    }
                                }
                            } else if (messageType.equals(MessageType.PUT_FINISHED.toString())) {
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                System.out.println("Put " + sdfsFilePath + " is finished");
                            } else if (messageType.equals(MessageType.GET.toString())) {
                                // Only master should receive this. Master will tell the client the storing location of the file.
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                String localFilePath = (String) resultObject.get("localFilePath");
                                long requestedVersionCount = (Long) resultObject.get("requestedVersionCount");
                                LOGGER.info("Client wants to get " + sdfsFilePath + " and save to " + localFilePath);
                                if (fileMetadata.containsKey(sdfsFilePath)) {
                                    long latestVersion = fileMetadata.get(sdfsFilePath).getLatestVersion();
                                    if (requestedVersionCount > latestVersion + 1) {
                                        requestedVersionCount = latestVersion + 1; // Only return the number of versions that are available.
                                    }
                                    List<String> storingMembers = fileMetadata.get(sdfsFilePath).getStoreLocations();
                                    newMessageJsonObject.put("command", MessageType.GET_RESPONSE.toString());
                                    newMessageJsonObject.put("localFilePath", localFilePath);
                                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                    newMessageJsonObject.put("version", latestVersion);
                                    newMessageJsonObject.put("storingMembers", CommandParserUtil.encodeIpList(storingMembers));
                                    newMessageJsonObject.put("requestedVersionCount", requestedVersionCount);
                                } else {
                                    LOGGER.warning("Client wants to get " + sdfsFilePath + " but it doesn't exist.");
                                    newMessageJsonObject.put("command", MessageType.GET_ERROR.toString());
                                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                }
                                sendMessage(newMessageJsonObject.toString(), clientAddress, FILE_PORT);
                            } else if (messageType.equals(MessageType.GET_RESPONSE.toString())) {
                                // Client gets the response and request the file from the storing locations.
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                String localFilePath = (String) resultObject.get("localFilePath");
                                long version = (Long) resultObject.get("version");
                                long requestedVersionCount = (Long) resultObject.get("requestedVersionCount");
                                List<String> storingMembers = CommandParserUtil.decodeIpList((JSONArray) resultObject.get("storingMembers"));
                                isGettingFile = true;
                                for (String storingMember : storingMembers) {
                                    newMessageJsonObject.put("command", MessageType.REQUEST_FILE.toString());
                                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                    newMessageJsonObject.put("localFilePath", localFilePath);
                                    newMessageJsonObject.put("sendToLocal", true);
                                    newMessageJsonObject.put("clientIp", localMember.getIp().getHostName());
                                    newMessageJsonObject.put("version", version);
                                    newMessageJsonObject.put("requestedVersionCount", requestedVersionCount);
                                    InetAddress firstIpAddress = InetAddress.getByName(storingMember);
                                    sendMessage(newMessageJsonObject.toString(), firstIpAddress, FILE_PORT);
                                }
                            } else if (messageType.equals(MessageType.GET_ERROR.toString())) {
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                System.out.println(sdfsFilePath + " doesn't exist");
                            } else if (messageType.equals(MessageType.REQUEST_FILE.toString())) {
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                String localFilePath = (String) resultObject.get("localFilePath");
                                String clientIpString = (String) resultObject.get("clientIp");
                                boolean sendToLocal = (Boolean) resultObject.get("sendToLocal");
                                InetAddress clientIp = InetAddress.getByName(clientIpString);
                                long version = (Long) resultObject.get("version");
                                long requestedVersionCount = (Long) resultObject.get("requestedVersionCount");
                                LOGGER.fine("Get file " + sdfsFilePath + " and save to " + localFilePath);
                                // Db -> LocalDir
                                int exitValue = -1;
                                if (requestedVersionCount <= 0) {
                                    exitValue = sendFile(insertVersionInFileName(sdfsFilePath, version), localFilePath, clientIp, false, sendToLocal);
                                } else {
                                    for (long i = version - requestedVersionCount + 1; i <= version; i++) {
                                        exitValue = sendFile(insertVersionInFileName(sdfsFilePath, i), insertVersionInFileName(localFilePath, i), clientIp, false, sendToLocal);
                                        // Also send to master when copying file between nodes. Master will update metadata based on that.
                                        if (!sendToLocal) {
                                            JSONObject anotherMessageJsonObject = new JSONObject();
                                            anotherMessageJsonObject.put("command", MessageType.FILE_RECEIVED.toString());
                                            anotherMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                            anotherMessageJsonObject.put("storingMemberIp", clientIp.getHostName());
                                            anotherMessageJsonObject.put("version", version);
                                            sendMessageToMaster(anotherMessageJsonObject.toString(), FILE_PORT);
                                        }
                                    }
                                }
                                if (exitValue == 0) {
                                    // Send to client
                                    newMessageJsonObject.put("command", MessageType.FILE_DOWNLOADED.toString());
                                    newMessageJsonObject.put("filePath", localFilePath);
                                    newMessageJsonObject.put("sendToLocal", sendToLocal);
                                    sendMessage(newMessageJsonObject.toString(), clientIp, FILE_PORT);
                                }
                            } else if (messageType.equals(MessageType.FILE_DOWNLOADED.toString())) {
                                String filePath = (String) resultObject.get("filePath");
                                boolean sendToLocal = (Boolean) resultObject.get("sendToLocal");
                                if (sendToLocal && isGettingFile) {
                                    isGettingFile = false;
                                    System.out.println("File " + filePath + " is received");
                                } else if (!sendToLocal) {
                                    storedFiles.add(filePath);
                                }
                            } else if (messageType.equals(MessageType.DELETE.toString())) {
                                // Only master should get this message, return the list of Vms tha has the file and
                                // remove the file from metadata
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                if (fileMetadata.containsKey(sdfsFilePath)) {
                                    List<String> ips = fileMetadata.get(sdfsFilePath).getStoreLocations();
                                    long latestVersion = fileMetadata.get(sdfsFilePath).getLatestVersion();
                                    fileMetadata.remove(sdfsFilePath);
                                    sendFileMetadataToAllBackupLocations();

                                    LOGGER.info("Delete file " + sdfsFilePath + ". " + ips + " are storing the file");
                                    newMessageJsonObject.put("command", MessageType.DELETE_TARGET.toString());
                                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                    newMessageJsonObject.put("ips", CommandParserUtil.encodeIpList(ips));
                                    newMessageJsonObject.put("latestVersion", latestVersion);
                                    sendMessage(newMessageJsonObject.toString(), clientAddress, FILE_PORT);
                                } else {
                                    LOGGER.warning(sdfsFilePath + " does not exist");
                                }
                            } else if (messageType.equals(MessageType.DELETE_TARGET.toString())) {
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                List<String> ips = CommandParserUtil.decodeIpList((JSONArray) resultObject.get("ips"));
                                long latestVersion = (Long) resultObject.get("latestVersion");
                                for (String ip : ips) {
                                    LOGGER.fine("Asking " + ip + "to delete file " + sdfsFilePath);
                                    JSONObject anotherMessageJsonObject = new JSONObject();
                                    anotherMessageJsonObject.put("command", MessageType.DELETE_COMMAND.toString());
                                    anotherMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                    anotherMessageJsonObject.put("latestVersion", latestVersion);
                                    sendMessage(anotherMessageJsonObject.toString(), InetAddress.getByName(ip), FILE_PORT);
                                }
                            } else if (messageType.equals(MessageType.DELETE_COMMAND.toString())) {
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                long latestVersion = (Long) resultObject.get("latestVersion");
                                LOGGER.fine("Deleting stored file " + sdfsFilePath);
                                for (long i = 0; i <= latestVersion; i++) {
                                    storedFiles.remove(sdfsFilePath);
                                    int exitValue = removeFile(insertVersionInFileName(sdfsFilePath, i));
                                    if (exitValue == 0) {
                                        LOGGER.info("Delete " + sdfsFilePath + " with version " + i + " succeeds");
                                    } else {
                                        LOGGER.info("Delete " + sdfsFilePath + " with version " + i + "fails");
                                    }
                                }
                            } else if (messageType.equals(MessageType.LS.toString())) {
                                // Only master should get this message
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                List<String> ips = new ArrayList<>();
                                if (fileMetadata.containsKey(sdfsFilePath)) {
                                    ips = fileMetadata.get(sdfsFilePath).getStoreLocations();
                                }
                                newMessageJsonObject.put("command", MessageType.LS_RESPONSE.toString());
                                newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                newMessageJsonObject.put("ips", CommandParserUtil.encodeIpList(ips));
                                sendMessage(newMessageJsonObject.toString(), clientAddress, FILE_PORT);
                            } else if (messageType.equals(MessageType.LS_RESPONSE.toString())) {
                                List<String> ips = CommandParserUtil.decodeIpList((JSONArray) resultObject.get("ips"));
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                System.out.println(sdfsFilePath + " is stored in " + ips);
                            } else if (messageType.equals(MessageType.COPY_FILE.toString())) {
                                List<String> ips = CommandParserUtil.decodeIpList((JSONArray) resultObject.get("ips"));
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                long version = (Long) resultObject.get("version");
                                String firstIp = ips.get(0);
                                ips.remove(0);

                                newMessageJsonObject.put("command", MessageType.REQUEST_FILE.toString());
                                newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                newMessageJsonObject.put("localFilePath", sdfsFilePath);
                                newMessageJsonObject.put("sendToLocal", false);
                                newMessageJsonObject.put("clientIp", localMember.getIp().getHostName());
                                newMessageJsonObject.put("version", version);
                                newMessageJsonObject.put("requestedVersionCount", version + 1); // Request all versions.
                                sendMessage(newMessageJsonObject.toJSONString(), InetAddress.getByName(firstIp), FILE_PORT);
                            }
                        }
                    }
                } catch (IOException | ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void runFailureDetectorServer() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        pongTaskHandler = executorService.submit(() -> {
            byte[] buf = new byte[128];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    failureDetectorSocket.receive(packet);
                    InetAddress clientAddress = packet.getAddress();

                    String inputLine = new String(packet.getData(), 0, packet.getLength());
                    LOGGER.fine("Failure detector server input from [" + clientAddress.getHostName() + "]: " + inputLine);

                    JSONParser jsonParser = new JSONParser();
                    JSONObject resultObject = (JSONObject) jsonParser.parse(inputLine);
                    String messageType = (String) resultObject.get("command");
                    if (messageType == null) {
                        LOGGER.warning("Server receives an unsupported command " + inputLine);
                    } else {
                        JSONObject newMessageJsonObject = new JSONObject();
                        if (isJoined) {
                            if (messageType.equals(MessageType.PING.toString())) {
                                LOGGER.fine("Ping is received, sending back pong to " + clientAddress.getHostName());
                                newMessageJsonObject.put("command", MessageType.PONG.toString());
                                sendMessage(newMessageJsonObject.toString(), clientAddress, FAILURE_DETECTOR_PORT);
                            } else if (messageType.equals(MessageType.PONG.toString())) {
                                // Cancel the timeout thread
                                LOGGER.fine("Pong is received, canceling the timeout");
                                ScheduledFuture<?> pingTimeoutThread = pingTimeoutThreadMap.get(clientAddress);
                                if (pingTimeoutThread != null) {
                                    pingTimeoutThread.cancel(true);
                                    pingTimeoutThreadMap.remove(clientAddress);
                                }
                            }
                        }
                    }
                } catch (IOException | ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    // Cancel all the current ping threads.
    private void stopAllCurrentPingThreads() {
        LOGGER.fine("Stop all ping threads");
        for (InetAddress key : pingThreadMap.keySet()) {
            pingThreadMap.get(key).cancel(true);
        }
        for (InetAddress key : pingTimeoutThreadMap.keySet()) {
            pingTimeoutThreadMap.get(key).cancel(true);
        }
        pingThreadMap = new HashMap<>();
        pingTimeoutThreadMap = new HashMap<>();
        if (pongTaskHandler != null) {
            pongTaskHandler.cancel(true);
        }
    }

    // Cancel the current ping threads for the ip.
    private void stopPingThreadsToIp(InetAddress ip) {
        LOGGER.fine("Stop the thread to " + ip);
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
        runFailureDetectorServer();

        List<GroupMember> targetMembers = connectionTopology.getTargets(localMember);
        for (GroupMember targetMember : targetMembers) {
            LOGGER.fine("Starting to ping " + targetMember.getIp().getHostName());
            ScheduledFuture<?> pingTaskHandler = scheduledExecutorService.scheduleAtFixedRate(() -> {
                // We need to start the timeout handler first to avoid the issue that the pong is received before the
                // handler is initiated. Only start if there is no current timeout task running.
                if (pingTimeoutThreadMap.get(targetMember.getIp()) == null) {
                    ScheduledFuture<?> pingTimeoutTaskHandler = scheduledExecutorService.schedule(() -> {
                        LOGGER.info("Timeout is found for " + targetMember.getIp().getHostName());
                        stopPingThreadsToIp(targetMember.getIp());
                        removeMemberAndShareWithGroup(targetMember.getIp().getHostName());

                        // Start Election if current leader times out
                        LOGGER.fine("Leader times out, initiating election.");
                        if (this.masterInfo.getMasterGroupMember().getRingId() == connectionTopology.getMemberRingId(targetMember)) {
                            JSONObject electionMessageJsonObject = new JSONObject();
                            electionMessageJsonObject.put("command", MessageType.ELECTION.toString());
                            electionMessageJsonObject.put("electingMember", localMember.toJson());
                            electionMessageJsonObject.put("initiator", connectionTopology.getMemberRingId(localMember));
                            isElectionInProgress = true;
                            sendMessageToSuccessor(electionMessageJsonObject.toJSONString(), GROUP_PORT);
                        }
                    }, PING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    pingTimeoutThreadMap.put(targetMember.getIp(), pingTimeoutTaskHandler);
                }

                LOGGER.fine("Ping is sent to " + targetMember.getIp().getHostName());
                JSONObject newMessageJsonObject = new JSONObject();
                newMessageJsonObject.put("command", MessageType.PING.toString());
                sendMessage(newMessageJsonObject.toString(), targetMember.getIp(), FAILURE_DETECTOR_PORT);
            }, 0, PING_FREQUENCY_MS, TimeUnit.MILLISECONDS);
            pingThreadMap.put(targetMember.getIp(), pingTaskHandler);
        }
    }

    private void removeMemberAndShareWithGroup(String ip) {
        try {
            stopPingThreadsToIp(InetAddress.getByName(ip));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        // update local information regarding the member that left.
        connectionTopology.removeMember(ip);
        // Share LEAVE with other target members
        JSONObject newMessageJsonObject = new JSONObject();
        newMessageJsonObject.put("command", MessageType.LEAVE.toString());
        newMessageJsonObject.put("ip", ip);
        sendMessageToAllTargetMembersAndStartPing(newMessageJsonObject.toString(), GROUP_PORT);
    }

    private boolean isMemberInMembers(GroupMember member) {
        String ip = member.getIp().getHostName();
        return isIpInMembers(ip);
    }

    private boolean isIpInMembers(String ip) {
        for (GroupMember groupMember : connectionTopology.getMemberList()) {
            if (groupMember.getIp().getHostName().equals(ip)) {
                return true;
            }
        }
        return false;
    }

    private int sendFile(String originalFilePath, String targetFilePath, InetAddress ip, boolean isOriginalFileLocal, boolean isTargetFileLocal) {
        LOGGER.info("Sending file to VM " + ip.getHostName() + " and store in " + targetFilePath);
        try {
            // Example: when in the mp3 folder, do scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null LocalDir/testFile.txt fa22-cs425-0501.cs.illinois.edu:/home/yuankai4/cs425-mp1-yuankai-zhuoyi/mp3_simple_distributed_file_system/Db/tttt.txt
            // "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" part will skip the auth check
            String originalFileDirectory = "";
            if (isOriginalFileLocal) {
                originalFileDirectory = LOCAL_DIRECTORY;
            } else {
                originalFileDirectory = Db_DIRECTORY;
            }
            String targetFileDirectory = "";
            if (isTargetFileLocal) {
                targetFileDirectory = LOCAL_DIRECTORY;
            } else {
                targetFileDirectory = Db_DIRECTORY;
            }
            String[] args = new String[]{"sh", "-c", "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null " + originalFileDirectory + originalFilePath + " " + ip.getHostName() + ":" + REMOTE_DIRECTORY + targetFileDirectory + targetFilePath};
            ProcessBuilder builder = new ProcessBuilder(args);
            Process process = builder.start();

            BufferedReader responseReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String commandOutput;
            while ((commandOutput = responseReader.readLine()) != null) {
                System.out.println(commandOutput);
            }

            process.waitFor();
            process.destroy();
            return process.exitValue();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int removeFile(String sdfsFilePath) {
        try {
            String[] args = new String[]{"sh", "-c", "rm " + Db_DIRECTORY + sdfsFilePath};
            ProcessBuilder builder = new ProcessBuilder(args);
            Process process = builder.start();

            BufferedReader responseReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String commandOutput;
            while ((commandOutput = responseReader.readLine()) != null) {
                System.out.println(commandOutput);
            }

            process.waitFor();
            process.destroy();
            return process.exitValue();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String insertVersionInFileName(String fileName, long version) {
        String[] fileNameParts = fileName.split("\\.");
        if (fileNameParts.length == 1) {
            // No extension part
            return fileName + FILE_VERSION_DELIMITER + version;
        } else if (fileNameParts.length == 2) {
            return fileNameParts[0] + FILE_VERSION_DELIMITER + version + "." + fileNameParts[1];
        } else {
            System.out.println("Unexpected file format when insert version " + fileName);
            return fileName;
        }
    }

    private void sendFileMetadataToAllBackupLocations() {
        LOGGER.fine("sending file metadata to all backup locations: " + masterInfo.getBackupMetadataStoreLocations());
        for (String ip : masterInfo.getBackupMetadataStoreLocations()) {
            if (ip.equals(localMember.getIp().getHostName())) {
                continue;
            }

            try {
                MasterInfo.sendFileMetadataTo(InetAddress.getByName(ip), fileMetadata);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Call this after a master is initiated or elected so that we can update the file storing locations with three seconds interval.
     */
    private void startThreadForReconstructingFileAndMetadata() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(() -> {
            if (isAddingFile) {
                LOGGER.fine("Adding files, skip reconstructing check");
            } else {
                LOGGER.fine("reconstructing Metadata");
                for (String filePath : fileMetadata.keySet()) {
                    SdfsFileMetadata metadata = fileMetadata.get(filePath);
                    List<String> locations = metadata.getStoreLocations();
                    List<String> memberIpList = connectionTopology.getMemberList().stream().map(groupMember -> groupMember.getIp().getHostName()).collect(Collectors.toList());
                    Iterator<String> locationIterator = locations.iterator();
                    while (locationIterator.hasNext()) {
                        String location = locationIterator.next();
                        if (!memberIpList.contains(location)) {
                            locationIterator.remove();
                        }
                    }
                    int replicasNeeded = NUMBER_OF_REPLICAS + 1 - locations.size(); // Add 1 for the original copy number
                    // Get a random list of nodes needed to get this file
                    List<GroupMember> allowedMembers = getMembersNotStoringCurrentFile(locations);
                    List<GroupMember> targetMembers = new ArrayList<>();
                    for (int i = 0; i < replicasNeeded; i++) {
                        Random rand = new Random();
                        int randomIndex = rand.nextInt(allowedMembers.size());
                        GroupMember targetMember = allowedMembers.get(randomIndex);
                        targetMembers.add(targetMember);
                        allowedMembers.remove(randomIndex);
                    }
                    for (GroupMember targetMember : targetMembers) {
                        LOGGER.fine(filePath + " will be stored in " + targetMember.getIp().getHostName());
                        JSONObject newMessageJsonObject = new JSONObject();
                        newMessageJsonObject.put("command", MessageType.COPY_FILE.toString());
                        newMessageJsonObject.put("ips", CommandParserUtil.encodeIpList(locations));
                        newMessageJsonObject.put("sdfsFilePath", filePath);
                        newMessageJsonObject.put("version", metadata.getLatestVersion());
                        sendMessage(newMessageJsonObject.toString(), targetMember.getIp(), FILE_PORT);
                        locations.add(targetMember.getIp().getHostName());
                    }
                    SdfsFileMetadata newFileMetadata = new SdfsFileMetadata(metadata.getLatestVersion(), locations);
                    fileMetadata.put(filePath, newFileMetadata);

                }
            }
        }, 0, MASTER_SANITY_CHECK_SECONDS, TimeUnit.SECONDS);
    }

    private List<GroupMember> getMembersNotStoringCurrentFile(List<String> storingLocations) {
        List<GroupMember> allowedMembers = new ArrayList<>();
        for (GroupMember member : connectionTopology.getMemberList()) {
            String memberIp = member.getIp().getHostName();
            if (!storingLocations.contains(memberIp)) {
                allowedMembers.add(member);
            }
        }
        return allowedMembers;
    }
}
