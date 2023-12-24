import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.logging.*;

/**
 * This is the main class to start a node in the member group. User will be required to provide some basic information.
 * We also handle the user input here.
 */
public class MemberGroupMain {
    private static InetAddress INTRODUCER_IP;

    static {
        try {
            INTRODUCER_IP = InetAddress.getByName("fa22-cs425-0501.cs.illinois.edu");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        /*
         * When It is the first member. Then we will create a group. We will always start VM01 as the first member and
         * treat it as the introducer. We will need "01" as the argument.
         */
        Scanner scanner = new Scanner(System.in);
        System.out.print("Which VM is this? ");
        String vmId = scanner.nextLine();
        System.out.print("Message will be dropped by X%: (0 means no dropped messages) ");
        int dropRate = 0;
        String dropRateInput = scanner.nextLine();
        try {
            dropRate = Integer.parseInt(dropRateInput);
        } catch (Exception e) {
            System.out.println("Invalid drop rate");
        }

        UdpServent udpServent = new UdpServent(dropRate);
        if (vmId.equals("01")) {
            udpServent.isJoined = true;
            udpServent.getLocalMember().setTimestamp(String.valueOf(System.currentTimeMillis()));
        }


        while (true) {
            System.out.print("$ ");
            String command = scanner.nextLine();
            UserInputCommand parsedCommand = CommandParserUtil.parseUserInput(command);
            if (parsedCommand == null) {
                System.out.println("User input is not recognized");
            } else {
                if (parsedCommand.userInputCommandType == UserInputCommandType.LIST_MEM) {
                    String memberShip = udpServent.printMemberList();
                    System.out.println("Current members:\n" + memberShip);
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.LIST_SELF) {
                    System.out.println("Self ip: " + udpServent.getLocalMember().getIp() + " join timestamp " + udpServent.getLocalMember().getTimestamp());
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.JOIN) {
                    if (udpServent.isJoined) {
                        System.out.println("The client is already in the group");
                    } else {
                        // VM01 works as the introducer.
                        udpServent.getLocalMember().setTimestamp(String.valueOf(System.currentTimeMillis()));
                        udpServent.sendMessage(new Message(SystemCommandType.CLIENT_JOIN, udpServent.getLocalMember().getId()), INTRODUCER_IP);
                    }
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.LEAVE) {
                    udpServent.sendMessageToAllTargetMembers(new Message(SystemCommandType.LEAVE, udpServent.getLocalMember().getIp().getHostAddress()));
                    udpServent.reset();
                } else if (parsedCommand.userInputCommandType == UserInputCommandType.GREP) {
                    udpServent.sendMessageToAllMembers(new Message(SystemCommandType.GREP, command));
                } else {
                    System.out.println("Unsupported user input");
                }
            }
        }
    }
}
