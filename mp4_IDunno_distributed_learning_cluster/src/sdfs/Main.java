package sdfs;

import org.json.simple.JSONObject;
import sdfs.networking.CommandParserUtil;
import sdfs.networking.MessageType;
import sdfs.networking.UdpServent;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static sdfs.networking.UdpServent.*;

/**
 * This is the main class to start a node in the member group. User will be required to provide some basic information.
 * We also handle the user input here.
 */
public class Main {
    // We keep a list of nodes that might be in the system
    private static final String[] possibleNodesInSystem = {
            "fa22-cs425-0501.cs.illinois.edu",
            "fa22-cs425-0502.cs.illinois.edu",
            "fa22-cs425-0503.cs.illinois.edu",
            "fa22-cs425-0504.cs.illinois.edu",
            "fa22-cs425-0505.cs.illinois.edu",
            "fa22-cs425-0506.cs.illinois.edu",
            "fa22-cs425-0507.cs.illinois.edu",
            "fa22-cs425-0508.cs.illinois.edu",
            "fa22-cs425-0509.cs.illinois.edu",
            "fa22-cs425-0510.cs.illinois.edu"
    };
    public static final UdpServent udpServent = new UdpServent();

    public static void main(String[] args) {
        /*
         * When It is the first member. Then we will create a group. We usually set VM01 to be the first member and
         * treat it as the introducer. We will need to type "y" when the message appears.
         */
        Scanner scanner = new Scanner(System.in);
        System.out.print("Are you the first member? (y/n) ");
        String isIntroducer = scanner.nextLine();

        if (isIntroducer.equals("y")) {
            cleanUpStoredFilesBeforeJoin();
            udpServent.initiateIntroducer();
        }

        while (true) {
            System.out.print("$ ");
            String command = scanner.nextLine();
            UserInputCommand parsedCommand = CommandParserUtil.parseUserInput(command);
            if (parsedCommand == null) {
                System.out.println("User input is not recognized");
            } else {
                JSONObject newMessageJsonObject = new JSONObject();
                if (parsedCommand.userInputCommandType == UserInputCommandType.LIST_MEM) {
                    if (udpServent.getMaster() != null) {
                        System.out.println("Master: " + udpServent.getMaster().getMasterGroupMemberIp());
                        System.out.println("Hot replace: " + udpServent.getMaster().getHotReplaceGroupMemberIp());
                    } else {
                        System.out.println("No master information. Use join to connect to the cluster.");
                    }
                    String memberShip = udpServent.printMemberList();
                    System.out.println("Current members:\n" + memberShip);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.LIST_SELF) {
                    System.out.println("Self ip: " + udpServent.getLocalMember().getIp() + " join timestamp " + udpServent.getLocalMember().getTimestamp() + " ring id " + udpServent.getLocalMember().getRingId());
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.JOIN) {
                    if (udpServent.isJoined) {
                        System.out.println("The client is already in the group");
                    } else {
                        cleanUpStoredFilesBeforeJoin();
                        // shuffle the list to balance the load
                        List<String> ips = Arrays.asList(possibleNodesInSystem);
                        Collections.shuffle(ips);
                        udpServent.getLocalMember().setTimestamp(String.valueOf(System.currentTimeMillis()));
                        newMessageJsonObject.put("command", MessageType.CLIENT_JOIN.toString());
                        newMessageJsonObject.put("member", udpServent.getLocalMember().toJson());
                        try {
                            for (int i = 0; i < ips.size(); i++) {
                                if (udpServent.isJoining || udpServent.isJoined) {
                                    break;
                                }
                                LOGGER.fine("try to join through " + ips.get(i));
                                InetAddress ipAddress = InetAddress.getByName(ips.get(i));
                                udpServent.sendMessage(newMessageJsonObject.toString(), ipAddress, UdpServent.JOIN_PORT);
                            }
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.LEAVE) {
                    newMessageJsonObject.put("command", MessageType.LEAVE.toString());
                    newMessageJsonObject.put("member", udpServent.getLocalMember().toJson());
                    udpServent.sendMessageToAllTargetMembers(newMessageJsonObject.toString(), UdpServent.FAILURE_DETECTOR_PORT);
                    udpServent.reset();
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.GREP) {
                    newMessageJsonObject.put("command", MessageType.GREP.toString());
                    newMessageJsonObject.put("grep", command);
                    udpServent.sendMessageToAllMembers(newMessageJsonObject.toString(), UdpServent.GROUP_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.PUT) {
                    String[] files = parsedCommand.content.split(" ");
                    String localFilePath = files[0];
                    String sdfsFilePath = files[1];
                    newMessageJsonObject.put("command", MessageType.PUT.toString());
                    newMessageJsonObject.put("localFilePath", localFilePath);
                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.FILE_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.GET) {
                    // Send to master to know the information about the version
                    String[] files = parsedCommand.content.split(" ");
                    String localFilePath = files[1];
                    String sdfsFilePath = files[0];
                    newMessageJsonObject.put("command", MessageType.GET.toString());
                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                    newMessageJsonObject.put("localFilePath", localFilePath);
                    newMessageJsonObject.put("requestedVersionCount", -1); // value that is <= 0 represents to get the latest version
                    newMessageJsonObject.put("fileType", GetFileType.NORMAL.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.FILE_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.DELETE) {
                    String sdfsFilePath = parsedCommand.content;
                    newMessageJsonObject.put("command", MessageType.DELETE.toString());
                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.FILE_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.LS) {
                    String sdfsFilePath = parsedCommand.content;
                    newMessageJsonObject.put("command", MessageType.LS.toString());
                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.FILE_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.STORE) {
                    System.out.println("Local files: " + udpServent.storedFiles);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.GET_VERSIONS) {
                    String[] inputs = parsedCommand.content.split(" ");
                    String sdfsFilePath = inputs[0];
                    int versions = Integer.parseInt(inputs[1]);
                    String localFilePath = inputs[2];
                    newMessageJsonObject.put("command", MessageType.GET.toString());
                    newMessageJsonObject.put("sdfsFilePath", sdfsFilePath);
                    newMessageJsonObject.put("localFilePath", localFilePath);
                    newMessageJsonObject.put("requestedVersionCount", versions);
                    newMessageJsonObject.put("fileType", GetFileType.NORMAL.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.FILE_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.LINUX_COMMAND) {
                    // Assume only local command here
                    runLinuxCommand(parsedCommand.content);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.START_TRAINING) {
                    newMessageJsonObject.put("command", MessageType.START_TRAINING.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.ML_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.STORE_QUERY) {
                    String[] inputs = parsedCommand.content.split(" ");
                    String queryFile = inputs[0];

                    // Put query zip file in SDFS
                    newMessageJsonObject.put("command", MessageType.PUT.toString());
                    newMessageJsonObject.put("localFilePath", queryFile + ZIP_EXTENSION);
                    newMessageJsonObject.put("sdfsFilePath", queryFile + ZIP_EXTENSION);
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.FILE_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.ADD_JOB) {
                    String[] inputs = parsedCommand.content.split(" ");
                    String model = inputs[0];
                    String queryFile = inputs[1];
                    int batchSize = Integer.parseInt(inputs[2]);

                    newMessageJsonObject.put("command", MessageType.ADD_JOB.toString());
                    newMessageJsonObject.put("model", model); // Use "alexnet" or "resnet"
                    newMessageJsonObject.put("queryFile", queryFile); // queryFile has the format of query1
                    newMessageJsonObject.put("batchSize", batchSize);
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.ML_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.SHOW_QUERY_RATE) {
                    newMessageJsonObject.put("command", MessageType.SHOW_QUERY_RATE.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.ML_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.SHOW_QUERY_DATA) {
                    newMessageJsonObject.put("command", MessageType.SHOW_QUERY_DATA.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.ML_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.GET_QUERY_RESULT) {
                    newMessageJsonObject.put("command", MessageType.GET_QUERY_RESULT.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.ML_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.STOP_QUERY_RESULT) {
                    newMessageJsonObject.put("command", MessageType.STOP_QUERY_RESULT.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.ML_PORT);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.SHOW_ASSIGNMENT) {
                    newMessageJsonObject.put("command", MessageType.SHOW_ASSIGNMENT.toString());
                    sendMessageToMasterAndCheckStatus(newMessageJsonObject.toString(), udpServent, UdpServent.ML_PORT);
                } else {
                    System.out.println("Unsupported user input");
                }
            }
        }
    }

    public static void sendMessageToMasterAndCheckStatus(String message, UdpServent udpServent, int port) {
        boolean isSucceeded = udpServent.sendMessageToMaster(message, port);
        if (!isSucceeded) {
            System.out.println("Try again later");
        }
    }

    private static void cleanUpStoredFilesBeforeJoin() {
        runLinuxCommand("rm -rf " + Db_DIRECTORY + " && mkdir Db");
    }

    private static void runLinuxCommand(String command) {
        try {
            String[] args = new String[]{"sh", "-c", command};
            ProcessBuilder builder = new ProcessBuilder(args);
            Process process = builder.start();

            BufferedReader inputResponseReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String commandOutput;
            while ((commandOutput = inputResponseReader.readLine()) != null) {
                System.out.println(commandOutput);
            }
            BufferedReader errorResponseReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((commandOutput = errorResponseReader.readLine()) != null) {
                System.out.println(commandOutput);
            }

            process.waitFor();
            process.destroy();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
