package sdfs.networking;

import ml.AvailableModel;
import ml.MLQueryData;
import ml.MachineLearningUtil;
import ml.RawMLQueryData;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import sdfs.GetFileType;
import sdfs.GrepQueryHandler;
import sdfs.SdfsFileMetadata;
import sdfs.WorkerQueryPair;

import java.io.BufferedReader;
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
    private static final int COORDINATOR_ASSIGN_QUERY_SECONDS = 5;
    public static final int GROUP_PORT = 8012; // Used to communicate membership in the system
    public static final int JOIN_PORT = 8013; // Used to communicate with external nodes
    public static final int FILE_PORT = 8014; // Used to communicate files in the system
    public static final int FAILURE_DETECTOR_PORT = 8005; // Used for ping and pong in failure detector
    public static final int ML_PORT = 8016; // Used for machine learning related queries
    private static final String LOCAL_DIRECTORY = "LocalDir/";
    public static String Db_DIRECTORY = "Db/";
    private static final String REMOTE_DIRECTORY = System.getProperty("user.dir") + "/";
    public static final int NUMBER_OF_REPLICAS = 3;
    public static final String ZIP_EXTENSION = ".zip";
    public static final String TEXT_EXTENSION = ".txt";
    private long lastFailureDetectionTime;
    private boolean sentStuckMessage = false;
    private Set<String> stuckedVMs = new HashSet<>();

    private MasterInfo masterInfo; // Master will handle file related messages
    private ConnectionTopology connectionTopology = new ConnectionTopology();
    private final DatagramSocket membershipSocket;
    private final DatagramSocket joinSocket;
    private final DatagramSocket fileSocket;
    private final DatagramSocket failureDetectorSocket;
    private final DatagramSocket mlSocket;
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
    private boolean isHotReplaceInProgress = false;

    private MachineLearningUtil machineLearningUtil = new MachineLearningUtil();
    private MLQueryData receivedMlQuery;
    private Set<String> receivedFiles = new HashSet<>();

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
            mlSocket = new DatagramSocket(ML_PORT);
        } catch (UnknownHostException | SocketException e) {
            throw new RuntimeException(e);
        }
        // Always start the servers. They will be responsible for receiving the message. It will keep running.
        runMembershipServer();
        runJoinServer();
        runFileServer();
        runMachineLearningServer();
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
        masterInfo = new MasterInfo(localMember.getIp().getHostName());
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
            } else if (port == ML_PORT) {
                mlSocket.send(queryPacket);
            }
        } catch (Exception e) {
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
        isHotReplaceInProgress = false;
        masterInfo = new MasterInfo();
        receivedFiles = new HashSet<>();
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
        if (masterInfo != null && !isHotReplaceInProgress) {
            try {
                sendMessage(message, InetAddress.getByName(masterInfo.getMasterGroupMemberIp()), port);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
            byte[] buf = new byte[81920];
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
                                LOGGER.fine("join_share client address " + member);
                                // Check if the new node is included in the current members
                                if (!isMemberInMembers(member)) {
                                    connectionTopology.addMember(member);
                                    // Share JOIN with other target members
                                    newMessageJsonObject.put("command", MessageType.SHARE_JOIN.toString());
                                    newMessageJsonObject.put("member", member.toJson());
                                    sendMessageToAllTargetMembersAndStartPing(newMessageJsonObject.toString(), GROUP_PORT);
                                }
                            } else if (messageType.equals(MessageType.PUT_BACKUP_METADATA.toString())) {
                                // Only the new hot replace member should receive this.
                                fileMetadata = CommandParserUtil.decodeFileMetadata(resultObject);
                            } else if (messageType.equals(MessageType.PUT_RAW_JOB_DATA.toString())) {
                                JSONObject preprocessingQueries = (JSONObject) resultObject.get("preprocessingQueries");
                                LOGGER.fine("PUT_RAW_JOB_DATA preprocessingQueries " + preprocessingQueries.toJSONString());
                                masterInfo.updatePreprocessingQueries(preprocessingQueries);
                            } else if (messageType.equals(MessageType.PUT_NEW_JOB_DATA.toString())) {
                                JSONObject todoQueries = (JSONObject) resultObject.get("todoQueries");
                                List<String> resultFiles = CommandParserUtil.decodeStringList((JSONArray) resultObject.get("resultFiles"));
                                LOGGER.fine("PUT_NEW_JOB_DATA todoQueries " + todoQueries.toJSONString());
                                LOGGER.fine("PUT_NEW_JOB_DATA resultFiles " + resultFiles);
                                masterInfo.updateTodoQueries(todoQueries);
                                masterInfo.resultFilePaths = resultFiles;
                            } else if (messageType.equals(MessageType.PUT_IN_PROGRESS_DATA.toString())) {
                                JSONObject workersToInProgressQueries = (JSONObject) resultObject.get("workersToInProgressQueries");
                                LOGGER.fine("PUT_IN_PROGRESS_DATA workersToInProgressQueries " + workersToInProgressQueries.toJSONString());
                                masterInfo.updateWorkerToInProgressQueriesToJson(workersToInProgressQueries);
                                masterInfo.startTime = (long) resultObject.get("startTime");
                            } else if (messageType.equals(MessageType.PUT_FINISHED_DATA.toString())) {
                                LOGGER.fine("PUT_FINISHED_DATA finishedAlexnetQueries " + ((Long) resultObject.get("finishedAlexnetQueries")).intValue());
                                LOGGER.fine("PUT_FINISHED_DATA finishedResnetQueries " + ((Long) resultObject.get("finishedResnetQueries")).intValue());
                                masterInfo.finishedAlexnetQueries = ((Long) resultObject.get("finishedAlexnetQueries")).intValue();
                                masterInfo.finishedResnetQueries = ((Long) resultObject.get("finishedResnetQueries")).intValue();
                                masterInfo.alexnetFinishTime = CommandParserUtil.decodeLongList((JSONArray) resultObject.get("alexnetFinishTime"));
                                masterInfo.resnetFinishTime = CommandParserUtil.decodeLongList((JSONArray) resultObject.get("resnetFinishTime"));
                                masterInfo.alexnetRunTimes = CommandParserUtil.decodeDoubleList((JSONArray) resultObject.get("alexnetRunTimes"));
                                masterInfo.resnetRunTimes = CommandParserUtil.decodeDoubleList((JSONArray) resultObject.get("resnetRunTimes"));
                            } else if (messageType.equals(MessageType.PUT_ROUTE_CLIENT.toString())) {
                                LOGGER.fine("PUT_ROUTE_CLIENT routeResultClientIp " + resultObject.get("clientIp"));
                                masterInfo.routeResultClientIp = (String) resultObject.get("clientIp");
                            } else if (messageType.equals(MessageType.STUCKED_VM.toString())) {
                                LOGGER.info(clientAddress.getHostName() + " becomes unstable");
                                stuckedVMs.add(clientAddress.getHostName());
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
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
                            } else if (messageType.equals(MessageType.SHARE_NEW_HOT_REPLACE_GROUPMEMBER.toString())) {
                                String newHotReplaceGroupMemberIp = (String) resultObject.get("newHotReplaceGroupMemberIp");
                                masterInfo.setHotReplaceGroupMemberIp(newHotReplaceGroupMemberIp);
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

                                // if there is no hot replace yet (still at the beginning of initialization), volunteer to be it.
                                LOGGER.fine("Current hot replace is " + masterInfo.getHotReplaceGroupMemberIp());
                                if (masterInfo.getHotReplaceGroupMemberIp() == null) {
                                    JSONObject anotherMessageJsonObject = new JSONObject();
                                    anotherMessageJsonObject.put("command", MessageType.SHARE_NEW_HOT_REPLACE_GROUPMEMBER.toString());
                                    anotherMessageJsonObject.put("newHotReplaceGroupMemberIp", localMember.getIp().getHostName());
                                    sendMessageToAllMembers(anotherMessageJsonObject.toJSONString(), JOIN_PORT);
                                }

                                isJoined = true;
                                isJoining = false;
                                pingTargetMembers();
                            } else {
                                // We might get some other messages when the node is not in the group. We will ignore those messages.
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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
                                int exitValue = sendFile(localFilePath, CommandParserUtil.insertVersionInFileName(sdfsFilePath, version), storingMemberIp, true, false);
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
                                    int exitValue = sendFile(CommandParserUtil.insertVersionInFileName(sdfsFilePath, version), CommandParserUtil.insertVersionInFileName(sdfsFilePath, version), connectionTopology.getSuccessor(localMember).getIp(), false, false);
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
                                    sendFileMetadataToHotReplace();

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
                                String fileType = (String) resultObject.get("fileType");
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
                                    newMessageJsonObject.put("storingMembers", CommandParserUtil.encodeStringList(storingMembers));
                                    newMessageJsonObject.put("requestedVersionCount", requestedVersionCount);
                                    newMessageJsonObject.put("fileType", fileType);
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
                                List<String> storingMembers = CommandParserUtil.decodeStringList((JSONArray) resultObject.get("storingMembers"));
                                String fileType = (String) resultObject.get("fileType");
                                isGettingFile = true;
                                for (String storingMember : storingMembers) {
                                    newMessageJsonObject.put("command", MessageType.REQUEST_FILE.toString());
                                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                    newMessageJsonObject.put("localFilePath", localFilePath);
                                    newMessageJsonObject.put("sendToLocal", true);
                                    newMessageJsonObject.put("clientIp", localMember.getIp().getHostName());
                                    newMessageJsonObject.put("version", version);
                                    newMessageJsonObject.put("requestedVersionCount", requestedVersionCount);
                                    newMessageJsonObject.put("fileType", fileType);
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
                                String fileType = (String) resultObject.get("fileType");
                                LOGGER.fine("Get file " + sdfsFilePath + " and save to " + localFilePath);
                                // Db -> LocalDir
                                int exitValue = -1;
                                if (requestedVersionCount <= 0) {
                                    exitValue = sendFile(CommandParserUtil.insertVersionInFileName(sdfsFilePath, version), localFilePath, clientIp, false, sendToLocal);
                                } else {
                                    for (long i = version - requestedVersionCount + 1; i <= version; i++) {
                                        exitValue = sendFile(CommandParserUtil.insertVersionInFileName(sdfsFilePath, i), CommandParserUtil.insertVersionInFileName(localFilePath, i), clientIp, false, sendToLocal);
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
                                    newMessageJsonObject.put("fileType", fileType);
                                    sendMessage(newMessageJsonObject.toString(), clientIp, FILE_PORT);
                                }
                            } else if (messageType.equals(MessageType.FILE_DOWNLOADED.toString())) {
                                String filePath = (String) resultObject.get("filePath");
                                boolean sendToLocal = (Boolean) resultObject.get("sendToLocal");
                                String fileType = (String) resultObject.get("fileType");
                                if (sendToLocal && isGettingFile) {
                                    isGettingFile = false;
                                    if (fileType.equals(GetFileType.NORMAL.toString())) {
                                        System.out.println("File " + filePath + " is received");
                                    } else if (fileType.equals(GetFileType.QUERY_FILE.toString())) {
                                        // Coordinator receives the query file.
                                        System.out.println("Query file " + filePath + " is received");
                                        receivedFiles.add(filePath);
                                        masterInfo.handleQueryFile(filePath);
                                        // update hot replace masterInfo. handleQueryFile requires update of all components of
                                        // master ml info so update everything.
                                        sendPreprocessingQueriesToHotReplace();
                                        sendNewJobDataToHotReplace();
                                    } else if (fileType.equals(GetFileType.TEST_INPUT.toString())) {
                                        System.out.println("Test input " + filePath + "is received");
                                        receivedFiles.add(filePath);
                                        CommandParserUtil.unzipLocalDirFileAndReturnPureName(filePath);
                                        handleTestInput();
                                    }
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
                                    sendFileMetadataToHotReplace();

                                    LOGGER.info("Delete file " + sdfsFilePath + ". " + ips + " are storing the file");
                                    newMessageJsonObject.put("command", MessageType.DELETE_TARGET.toString());
                                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                                    newMessageJsonObject.put("ips", CommandParserUtil.encodeStringList(ips));
                                    newMessageJsonObject.put("latestVersion", latestVersion);
                                    sendMessage(newMessageJsonObject.toString(), clientAddress, FILE_PORT);
                                } else {
                                    LOGGER.warning(sdfsFilePath + " does not exist");
                                }
                            } else if (messageType.equals(MessageType.DELETE_TARGET.toString())) {
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                List<String> ips = CommandParserUtil.decodeStringList((JSONArray) resultObject.get("ips"));
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
                                    int exitValue = removeFile(CommandParserUtil.insertVersionInFileName(sdfsFilePath, i));
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
                                newMessageJsonObject.put("ips", CommandParserUtil.encodeStringList(ips));
                                sendMessage(newMessageJsonObject.toString(), clientAddress, FILE_PORT);
                            } else if (messageType.equals(MessageType.LS_RESPONSE.toString())) {
                                List<String> ips = CommandParserUtil.decodeStringList((JSONArray) resultObject.get("ips"));
                                String sdfsFilePath = (String) resultObject.get("sdfsFilePath");
                                System.out.println(sdfsFilePath + " is stored in " + ips);
                            } else if (messageType.equals(MessageType.COPY_FILE.toString())) {
                                List<String> ips = CommandParserUtil.decodeStringList((JSONArray) resultObject.get("ips"));
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
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void runFailureDetectorServer() {
        lastFailureDetectionTime = System.currentTimeMillis(); // Initial value
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        pongTaskHandler = executorService.submit(() -> {
            byte[] buf = new byte[256];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    failureDetectorSocket.receive(packet);
                    InetAddress clientAddress = packet.getAddress();
                    JSONObject newMessageJsonObject = new JSONObject();

                    lastFailureDetectionTime = System.currentTimeMillis();
                    if (sentStuckMessage) {
                        newMessageJsonObject.put("command", MessageType.RECOVERED_VM.toString());
                        sendMessageToAllMembers(newMessageJsonObject.toString(), FAILURE_DETECTOR_PORT);
                        sentStuckMessage = false;
                    }

                    String inputLine = new String(packet.getData(), 0, packet.getLength());
                    LOGGER.fine("Failure detector server input from [" + clientAddress.getHostName() + "]: " + inputLine);

                    JSONParser jsonParser = new JSONParser();
                    JSONObject resultObject = (JSONObject) jsonParser.parse(inputLine);
                    String messageType = (String) resultObject.get("command");
                    if (messageType == null) {
                        LOGGER.warning("Server receives an unsupported command " + inputLine);
                    } else {
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
                            } else if (messageType.equals(MessageType.LEAVE.toString())) {
                                GroupMember leavingMember = CommandParserUtil.decodeMember((JSONObject) resultObject.get("member"));
                                LOGGER.info("leave is received for " + leavingMember.getIp().getHostName());

                                if (isMemberInMembers(leavingMember)) {
                                    removeMemberAndShareWithGroup(leavingMember);
                                    handleBackupData(leavingMember.getIp().getHostName());
                                }
                            } else if (messageType.equals(MessageType.INITIATE_HOT_REPLACE.toString())) {
                                // Only current hot replace member should receive this.
                                LOGGER.info("Master failed, initiating hot replace by hot replace.");
                                if (!isHotReplaceInProgress && !masterInfo.isMasterMember(localMember)) {
                                    isHotReplaceInProgress = true;
                                    masterInfo.setMasterGroupMemberIp(localMember.getIp().getHostName());
                                    GroupMember newHotReplaceMember = connectionTopology.getSuccessor(localMember);
                                    String hotReplaceGroupMemberIp = newHotReplaceMember.getIp().getHostName();
                                    masterInfo.setHotReplaceGroupMemberIp(hotReplaceGroupMemberIp);
                                    newMessageJsonObject.put("command", MessageType.HOT_REPLACE.toString());
                                    newMessageJsonObject.put("hotReplaceGroupMemberIp", hotReplaceGroupMemberIp);
                                    sendMessageToSuccessor(newMessageJsonObject.toJSONString(), FAILURE_DETECTOR_PORT);
                                }
                            } else if (messageType.equals(MessageType.HOT_REPLACE.toString())) {
                                LOGGER.info("Hot replace message is received. Current hot replace " + masterInfo.getHotReplaceGroupMemberIp() + " will be coordinator");
                                if (masterInfo.isMasterMember(localMember)) {
                                    // Message has gone through all the nodes
                                    LOGGER.fine("Master got the hot replace message. Information has gone through all the nodes.");
                                    // Recalculate some master info after master failure
                                    List<String> memberIpList = connectionTopology.getMemberList().stream().map(groupMember -> groupMember.getIp().getHostName()).collect(Collectors.toList());
                                    masterInfo.resetCurrentJobs(memberIpList);
                                    for (RawMLQueryData rawMLQueryData : masterInfo.getPreprocessingQueries().values()) {
                                        processRawInput(rawMLQueryData);
                                    }
                                    // Start master threads
                                    startThreadForReconstructingFileAndMetadata();
                                    startThreadForAssignJobToWorkers();
                                    // Send backup data to new backup
                                    sendMasterMLInfoToHotReplace();
                                    sendFileMetadataToHotReplace();
                                    // End election
                                    newMessageJsonObject.put("command", MessageType.END_ELECTION.toString());
                                    sendMessageToAllMembers(newMessageJsonObject.toJSONString(), FAILURE_DETECTOR_PORT);
                                } else {
                                    isHotReplaceInProgress = true;
                                    masterInfo.setMasterGroupMemberIp(masterInfo.getHotReplaceGroupMemberIp());
                                    String hotReplaceGroupMemberIp = (String) resultObject.get("hotReplaceGroupMemberIp");
                                    LOGGER.fine("Starting hot replace, new hot replace is " + hotReplaceGroupMemberIp);
                                    masterInfo.setHotReplaceGroupMemberIp(hotReplaceGroupMemberIp);
                                    // Reset receivedMlQuery because master will clean up all in progress queries
                                    receivedMlQuery = null;

                                    newMessageJsonObject.put("command", MessageType.HOT_REPLACE.toString());
                                    newMessageJsonObject.put("hotReplaceGroupMemberIp", hotReplaceGroupMemberIp);
                                    sendMessageToSuccessor(newMessageJsonObject.toJSONString(), FAILURE_DETECTOR_PORT);
                                }
                            } else if (messageType.equals(MessageType.END_ELECTION.toString())) {
                                LOGGER.info("Hot replace ended. Resume to normal file operations.");
                                isHotReplaceInProgress = false;
                            } else if (messageType.equals(MessageType.RECOVERED_VM.toString())) {
                                LOGGER.info(clientAddress.getHostName() + "is stable now");
                                stuckedVMs.remove(clientAddress.getHostName());
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void runMachineLearningServer() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            byte[] buf = new byte[20480];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    mlSocket.receive(packet);
                    InetAddress clientAddress = packet.getAddress();

                    String inputLine = new String(packet.getData(), 0, packet.getLength());
                    LOGGER.fine("ML server input from [" + clientAddress.getHostName() + "]: " + inputLine);

                    JSONParser jsonParser = new JSONParser();
                    JSONObject resultObject = (JSONObject) jsonParser.parse(inputLine);
                    String messageType = (String) resultObject.get("command");
                    if (messageType == null) {
                        LOGGER.warning("Server receives an unsupported command " + inputLine);
                    } else {
                        JSONObject newMessageJsonObject = new JSONObject();
                        if (isJoined) {
                            if (messageType.equals(MessageType.START_TRAINING.toString())) {
                                // Only coordinator should get this message
                                // Use more detailed task assignment for real training
                                newMessageJsonObject.put("command", MessageType.WORKER_START_TRAINING.toString());
                                newMessageJsonObject.put("clientIp", clientAddress.getHostName());
                                sendMessageToAllMembers(newMessageJsonObject.toJSONString(), ML_PORT);
                            } else if (messageType.equals(MessageType.WORKER_START_TRAINING.toString())) {
                                LOGGER.info("Worker starts to train models");
                                machineLearningUtil.trainNeuralNetworks();
                                String clientIpString = (String) resultObject.get("clientIp");
                                newMessageJsonObject.put("command", MessageType.TRAINING_FINISHED.toString());
                                newMessageJsonObject.put("clientIp", clientIpString);
                                sendMessageToMaster(newMessageJsonObject.toJSONString(), ML_PORT);
                            } else if (messageType.equals(MessageType.TRAINING_FINISHED.toString())) {
                                LOGGER.info("Training is finished");
                                masterInfo.addNewWorker(clientAddress.getHostName());

                                if (masterInfo.getFreeWorkers().size() >= 10) {
                                    String clientIpString = (String) resultObject.get("clientIp");
                                    InetAddress clientIp = InetAddress.getByName(clientIpString);
                                    newMessageJsonObject.put("command", MessageType.ALL_TRAINING_FINISHED.toString());
                                    sendMessage(newMessageJsonObject.toJSONString(), clientIp, ML_PORT);
                                    // Start the thread for assigning job after training is done.
                                    startThreadForAssignJobToWorkers();
                                }
                            } else if (messageType.equals(MessageType.ALL_TRAINING_FINISHED.toString())) {
                                // Print out message to the client that training is done.
                                System.out.println("Neural network is ready");
                            } else if (messageType.equals(MessageType.ADD_JOB.toString())) {
                                // Only coordinator should receive this
                                // Since coordinator is the same as file master, it can ask itself about the store location of the file.
                                String queryFile = (String) resultObject.get("queryFile"); // queryFile has the format of query1
                                String model = (String) resultObject.get("model");
                                Long batchSize = (Long) resultObject.get("batchSize");
                                LOGGER.info("Job received queryFile " + queryFile + " model " + model);
                                if (model.equalsIgnoreCase("alexnet")) {
                                    RawMLQueryData rawMLQueryData = new RawMLQueryData(queryFile, AvailableModel.ALEXNET, batchSize);
                                    masterInfo.addRawQuery(queryFile, rawMLQueryData);
                                    processRawInput(rawMLQueryData);
                                } else if (model.equalsIgnoreCase("resnet")) {
                                    RawMLQueryData rawMLQueryData = new RawMLQueryData(queryFile, AvailableModel.RESNET, batchSize);
                                    masterInfo.addRawQuery(queryFile, rawMLQueryData);
                                    processRawInput(rawMLQueryData);
                                } else {
                                    System.out.println("Model is not supported");
                                }
                                sendPreprocessingQueriesToHotReplace();
                            } else if (messageType.equals(MessageType.PROCESS_QUERY.toString())) {
                                LOGGER.fine("Start to process query");
                                MLQueryData mlQueryData = CommandParserUtil.decodeMLQueryData((JSONObject) resultObject.get("query"));
                                receivedMlQuery = mlQueryData;
                                String queryPath = mlQueryData.getQueryName() + ZIP_EXTENSION;
                                if (receivedFiles.contains(queryPath)) {
                                    // Get files directly
                                    handleTestInput();
                                } else {
                                    // Get test input from sdfs and unzip it
                                    JSONObject getFileMessageJsonObject = new JSONObject();
                                    getFileMessageJsonObject.put("command", MessageType.GET.toString());
                                    getFileMessageJsonObject.put("sdfsFilePath", queryPath);
                                    getFileMessageJsonObject.put("localFilePath", queryPath);
                                    getFileMessageJsonObject.put("requestedVersionCount", -1);
                                    getFileMessageJsonObject.put("fileType", GetFileType.TEST_INPUT.toString());
                                    sendMessageToMaster(getFileMessageJsonObject.toString(), FILE_PORT);
                                }
                            } else if (messageType.equals(MessageType.QUERY_FINISHED.toString())) {
                                LOGGER.fine("Processing is finished");
                                // Only coordinator should receive this
                                MLQueryData mlQueryData = CommandParserUtil.decodeMLQueryData((JSONObject) resultObject.get("query"));

                                masterInfo.handleFinishedQuery(clientAddress.getHostName(), mlQueryData);
                                sendInProgressDataToHotReplace();
                                sendFinishDataToHotReplace();

                                String routeResultClientIp = masterInfo.routeResultClientIp;
                                if (routeResultClientIp != null) {
                                    newMessageJsonObject.put("command", MessageType.ROUTE_RESULT.toString());
                                    newMessageJsonObject.put("result", mlQueryData.getResult());
                                    sendMessage(newMessageJsonObject.toString(), InetAddress.getByName(routeResultClientIp), ML_PORT);
                                }

                                // Return a valid list if jobs are done
                                List<String> resultFiles = masterInfo.uploadResultFiles();
                                // Use for loop because we expect a small number of result files. Implement put multiple to improve performance.
                                for (String file : resultFiles) {
                                    JSONObject uploadFileMessageJsonObject = new JSONObject();
                                    uploadFileMessageJsonObject.put("command", MessageType.PUT.toString());
                                    uploadFileMessageJsonObject.put("localFilePath", file);
                                    uploadFileMessageJsonObject.put("sdfsFilePath", file);
                                    sendMessageToMaster(uploadFileMessageJsonObject.toString(), FILE_PORT);
                                }
                            } else if (messageType.equals(MessageType.ROUTE_RESULT.toString())) {
                                String result = (String) resultObject.get("result");
                                System.out.println(result);
                            } else if (messageType.equals(MessageType.SHOW_QUERY_RATE.toString())) {
                                newMessageJsonObject.put("command", MessageType.QUERY_RATE_RESULT.toString());
                                newMessageJsonObject.put("result", masterInfo.showQueryRateAndFinishedQueryCount());
                                sendMessage(newMessageJsonObject.toString(), clientAddress, ML_PORT);
                            } else if (messageType.equals(MessageType.QUERY_RATE_RESULT.toString())) {
                                String result = (String) resultObject.get("result");
                                System.out.println(result);
                            } else if (messageType.equals(MessageType.SHOW_QUERY_DATA.toString())) {
                                newMessageJsonObject.put("command", MessageType.QUERY_DATA_RESULT.toString());
                                newMessageJsonObject.put("result", masterInfo.showRunTimeStats());
                                sendMessage(newMessageJsonObject.toString(), clientAddress, ML_PORT);
                            } else if (messageType.equals(MessageType.QUERY_DATA_RESULT.toString())) {
                                String result = (String) resultObject.get("result");
                                System.out.println(result);
                            } else if (messageType.equals(MessageType.SHOW_ASSIGNMENT.toString())) {
                                newMessageJsonObject.put("command", MessageType.ASSIGNMENT_RESULT.toString());
                                newMessageJsonObject.put("result", masterInfo.showVMAssignments());
                                sendMessage(newMessageJsonObject.toString(), clientAddress, ML_PORT);
                            } else if (messageType.equals(MessageType.ASSIGNMENT_RESULT.toString())) {
                                String result = (String) resultObject.get("result");
                                System.out.println(result);
                            } else if (messageType.equals(MessageType.GET_QUERY_RESULT.toString())) {
                                masterInfo.routeResultClientIp = clientAddress.getHostName();
                                sendRouteClientIp();
                            } else if (messageType.equals(MessageType.STOP_QUERY_RESULT.toString())) {
                                masterInfo.routeResultClientIp = null;
                                sendRouteClientIp();
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

    private void sendMessageToHotReplace(String message, int port) {
        try {
            sendMessage(message, InetAddress.getByName(masterInfo.getHotReplaceGroupMemberIp()), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
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
                long currentTime = System.currentTimeMillis();

                if (currentTime - lastFailureDetectionTime > 1500 && !sentStuckMessage) {
                    LOGGER.fine("Find unexpected failure detector time interval " + (currentTime - lastFailureDetectionTime));
                    JSONObject newMessageJsonObject = new JSONObject();
                    newMessageJsonObject.put("command", MessageType.STUCKED_VM.toString());
                    sendMessageToAllMembers(newMessageJsonObject.toString(), GROUP_PORT);
                    sentStuckMessage = true;
                }

                // We need to start the timeout handler first to avoid the issue that the pong is received before the
                // handler is initiated. Only start if there is no current timeout task running.
                if (pingTimeoutThreadMap.get(targetMember.getIp()) == null) {
                    ScheduledFuture<?> pingTimeoutTaskHandler = scheduledExecutorService.schedule(() -> {
                        String leavingIp = targetMember.getIp().getHostName();
                        LOGGER.info("Timeout is found for " + leavingIp);
                        LOGGER.fine("Stucked VMs are " + stuckedVMs);
                        if (stuckedVMs.contains(localMember.getIp().getHostName())) {
                            LOGGER.fine("I am stuck. Ignore any timeouts");
                        } else if (!stuckedVMs.contains(leavingIp)) {
                            LOGGER.fine("This is a real timeout " + leavingIp);
                            removeMemberAndShareWithGroup(targetMember);
                            handleBackupData(leavingIp);
                            JSONObject newMessageJsonObject = new JSONObject();
                            newMessageJsonObject.put("command", MessageType.LEAVE.toString());
                            newMessageJsonObject.put("member", targetMember.toJson());
                            sendMessageToAllTargetMembers(newMessageJsonObject.toString(), FAILURE_DETECTOR_PORT);

                            // Start Election if current leader times out
                            if (this.masterInfo.getMasterGroupMemberIp().equals(leavingIp)) {
                                LOGGER.fine("Leader times out, initiating hot replace.");
                                try {
                                    JSONObject electionMessageJsonObject = new JSONObject();
                                    electionMessageJsonObject.put("command", MessageType.INITIATE_HOT_REPLACE.toString());
                                    sendMessage(electionMessageJsonObject.toJSONString(), InetAddress.getByName(masterInfo.getHotReplaceGroupMemberIp()), FAILURE_DETECTOR_PORT);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        } else {
                            LOGGER.fine(leavingIp + " is a stucked vm");
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

    private void handleBackupData(String leavingIp) {
        // If this is master, handle backup data.
        if (masterInfo.isMasterMember(localMember)) {
            masterInfo.handleFailedWorker(leavingIp);

            if (leavingIp.equals(masterInfo.getHotReplaceGroupMemberIp())) {
                // Choose a new backup, send backup file, broadcast to group
                GroupMember newHotReplaceMember = connectionTopology.getSuccessor(localMember);
                String hotReplaceGroupMemberIp = newHotReplaceMember.getIp().getHostName();
                LOGGER.info("Current backup is left. Choose " + hotReplaceGroupMemberIp + " to be the new hot replace");
                masterInfo.setHotReplaceGroupMemberIp(hotReplaceGroupMemberIp);
                sendFileMetadataToHotReplace();
                sendMasterMLInfoToHotReplace();
                JSONObject anotherMessageJsonObject = new JSONObject();
                anotherMessageJsonObject.put("command", MessageType.SHARE_NEW_HOT_REPLACE_GROUPMEMBER.toString());
                anotherMessageJsonObject.put("newHotReplaceGroupMemberIp", hotReplaceGroupMemberIp);
                sendMessageToAllMembers(anotherMessageJsonObject.toJSONString(), JOIN_PORT);
            }
        }
    }

    private void removeMemberAndShareWithGroup(GroupMember member) {
        stopPingThreadsToIp(member.getIp());
        // update local information regarding the member that left.
        connectionTopology.removeMember(member);
        // Share LEAVE with other target members
        JSONObject newMessageJsonObject = new JSONObject();
        newMessageJsonObject.put("command", MessageType.LEAVE.toString());
        newMessageJsonObject.put("member", member.toJson());
        sendMessageToAllTargetMembersAndStartPing(newMessageJsonObject.toString(), FAILURE_DETECTOR_PORT);
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
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
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
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    private void sendFileMetadataToHotReplace() {
        LOGGER.fine("sending file metadata to hot replace: " + masterInfo.getHotReplaceGroupMemberIp());
        try {
            MasterInfo.sendFileMetadataTo(InetAddress.getByName(masterInfo.getHotReplaceGroupMemberIp()), fileMetadata);
        } catch (Exception e) {
            e.printStackTrace();
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
                        newMessageJsonObject.put("ips", CommandParserUtil.encodeStringList(locations));
                        newMessageJsonObject.put("sdfsFilePath", filePath);
                        newMessageJsonObject.put("version", metadata.getLatestVersion());
                        sendMessage(newMessageJsonObject.toString(), targetMember.getIp(), FILE_PORT);
                        locations.add(targetMember.getIp().getHostName());
                    }
                    SdfsFileMetadata newFileMetadata = new SdfsFileMetadata(metadata.getLatestVersion(), locations);
                    fileMetadata.put(filePath, newFileMetadata);
                    sendFileMetadataToHotReplace();
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

    private void startThreadForAssignJobToWorkers() {
        LOGGER.fine("startThreadForAssignJobToWorkers");
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(() -> {
            while (true) {
                try {
                    WorkerQueryPair workerQueryPair = masterInfo.updateDataAndGetAssignment();
                    LOGGER.info("WorkerQueryPair is " + workerQueryPair.toString());
                    if (workerQueryPair.workerIp == null) {
                        break;
                    } else {
                        sendNewJobDataToHotReplace();
                        sendInProgressDataToHotReplace();
                        // Send message to the worker for the query
                        JSONObject newMessageJsonObject = new JSONObject();
                        newMessageJsonObject.put("command", MessageType.PROCESS_QUERY.toString());
                        newMessageJsonObject.put("query", workerQueryPair.mlQueryData.toJson());
                        sendMessage(newMessageJsonObject.toString(), InetAddress.getByName(workerQueryPair.workerIp), ML_PORT);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            masterInfo.showQueryRateAndFinishedQueryCount(); // C1
            masterInfo.showRunTimeStats(); // C2
            masterInfo.showVMAssignments(); // C5
        }, 0, COORDINATOR_ASSIGN_QUERY_SECONDS, TimeUnit.SECONDS);
    }

    // Only coordinator should call this. We either process the input after message is received or
    private void processRawInput(RawMLQueryData rawMLQueryData) throws UnknownHostException {
        String queryFile = rawMLQueryData.getQueryFile();
        JSONObject newMessageJsonObject = new JSONObject();
        String queryZipFile = queryFile + ZIP_EXTENSION;
        if (fileMetadata.containsKey(queryZipFile)) {
            long latestVersion = fileMetadata.get(queryZipFile).getLatestVersion();
            List<String> storingMembers = fileMetadata.get(queryZipFile).getStoreLocations();
            isGettingFile = true;
            newMessageJsonObject.put("command", MessageType.REQUEST_FILE.toString());
            newMessageJsonObject.put("sdfsFilePath", queryZipFile);
            newMessageJsonObject.put("localFilePath", queryZipFile);
            newMessageJsonObject.put("sendToLocal", true);
            newMessageJsonObject.put("clientIp", localMember.getIp().getHostName());
            newMessageJsonObject.put("version", latestVersion);
            newMessageJsonObject.put("requestedVersionCount", -1);
            newMessageJsonObject.put("fileType", GetFileType.QUERY_FILE.toString());
            InetAddress firstIpAddress = InetAddress.getByName(storingMembers.get(0));
            sendMessage(newMessageJsonObject.toString(), firstIpAddress, FILE_PORT);
        } else {
            LOGGER.warning("Query file " + queryZipFile + " doesn't exist in sdfs");
        }
    }

    private void handleTestInput() {
        if (receivedMlQuery != null) {
            long inferenceStartTime = System.currentTimeMillis();
            String result = machineLearningUtil.runModel(receivedMlQuery);
            double runTimeInSeconds = (System.currentTimeMillis() - inferenceStartTime) / 1000.0;
            receivedMlQuery.setResult(result);
            receivedMlQuery.setRunTimeInSeconds(runTimeInSeconds);

            JSONObject newMessageJsonObject = new JSONObject();
            newMessageJsonObject.put("command", MessageType.QUERY_FINISHED.toString());
            newMessageJsonObject.put("query", receivedMlQuery.toJson());
            sendMessageToMaster(newMessageJsonObject.toJSONString(), ML_PORT);
        }
    }

    /**
     * Send message Info to hot replace as different parts.
     */
    private void sendMasterMLInfoToHotReplace() {
        sendPreprocessingQueriesToHotReplace();
        sendNewJobDataToHotReplace();
        sendInProgressDataToHotReplace();
        sendFinishDataToHotReplace();
        sendRouteClientIp();
    }

    private void sendPreprocessingQueriesToHotReplace() {
        LOGGER.fine("sendPreprocessingQueriesToHotReplace");
        JSONObject messageJsonObject = new JSONObject();
        messageJsonObject.put("command", MessageType.PUT_RAW_JOB_DATA.toString());
        messageJsonObject.put("preprocessingQueries", masterInfo.preprocessingQueriesToJson());
        sendMessageToHotReplace(messageJsonObject.toString(), GROUP_PORT);
    }

    private void sendNewJobDataToHotReplace() {
        LOGGER.fine("sendNewJobDataToHotReplace");
        JSONObject messageJsonObject = new JSONObject();
        messageJsonObject.put("command", MessageType.PUT_NEW_JOB_DATA.toString());
        messageJsonObject.put("todoQueries", masterInfo.todoQueriesToJson());
        messageJsonObject.put("resultFiles", CommandParserUtil.encodeStringList(masterInfo.resultFilePaths));
        sendMessageToHotReplace(messageJsonObject.toString(), GROUP_PORT);
    }

    private void sendInProgressDataToHotReplace() {
        LOGGER.fine("sendInProgressDataToHotReplace");
        JSONObject messageJsonObject = new JSONObject();
        messageJsonObject.put("command", MessageType.PUT_IN_PROGRESS_DATA.toString());
        messageJsonObject.put("workersToInProgressQueries", masterInfo.workerToInProgressQueriesToJson());
        messageJsonObject.put("startTime", masterInfo.startTime);
        sendMessageToHotReplace(messageJsonObject.toString(), GROUP_PORT);
    }

    private void sendFinishDataToHotReplace() {
        LOGGER.fine("sendFinishDataToHotReplace");
        JSONObject messageJsonObject = new JSONObject();
        messageJsonObject.put("command", MessageType.PUT_FINISHED_DATA.toString());
        messageJsonObject.put("finishedAlexnetQueries", masterInfo.finishedAlexnetQueries);
        messageJsonObject.put("finishedResnetQueries", masterInfo.finishedResnetQueries);
        messageJsonObject.put("alexnetFinishTime", CommandParserUtil.encodeLongList(masterInfo.alexnetFinishTime));
        messageJsonObject.put("resnetFinishTime", CommandParserUtil.encodeLongList(masterInfo.resnetFinishTime));
        messageJsonObject.put("alexnetRunTimes", CommandParserUtil.encodeDoubleList(masterInfo.alexnetRunTimes));
        messageJsonObject.put("resnetRunTimes", CommandParserUtil.encodeDoubleList(masterInfo.resnetRunTimes));
        sendMessageToHotReplace(messageJsonObject.toString(), GROUP_PORT);
    }

    private void sendRouteClientIp() {
        LOGGER.fine("sendRouteClientIp");
        JSONObject messageJsonObject = new JSONObject();
        messageJsonObject.put("command", MessageType.PUT_ROUTE_CLIENT.toString());
        messageJsonObject.put("clientIp", masterInfo.routeResultClientIp);
        sendMessageToHotReplace(messageJsonObject.toString(), GROUP_PORT);
    }
}
