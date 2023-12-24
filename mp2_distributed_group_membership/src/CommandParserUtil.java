import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Util class to parse the commands. All commands will have the format like "COMMAND_TYPE;optional_command_content"
 */
public class CommandParserUtil {
    public static String COMMAND_SEPARATOR = ";";

    public static UserInputCommand parseUserInput(String userInput) {
        String[] splitInput = userInput.split(" ");
        if (splitInput.length > 0) {
            String command = splitInput[0];
            if (command.equals("grep")) {
                return new UserInputCommand(UserInputCommandType.GREP, userInput);
            } else if (command.equals("join")) {
                return new UserInputCommand(UserInputCommandType.JOIN, null);
            } else if (command.equals("leave")) {
                return new UserInputCommand(UserInputCommandType.LEAVE, null);
            } else if (command.equals("list_mem")) {
                return new UserInputCommand(UserInputCommandType.LIST_MEM, null);
            } else if (command.equals("list_self")) {
                return new UserInputCommand(UserInputCommandType.LIST_SELF, null);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public static SystemCommand parseSystemCommand(String systemCommand) {
        String[] splitCommand = systemCommand.split(COMMAND_SEPARATOR);
        if (splitCommand.length > 0) {
            SystemCommandType systemCommandType = toDecryptedCommandType(splitCommand[0]);
            if (systemCommandType == SystemCommandType.PING) {
                return new SystemCommand(SystemCommandType.PING, null);
            } else if (systemCommandType == SystemCommandType.PONG) {
                return new SystemCommand(SystemCommandType.PONG, null);
            } else if (systemCommandType == SystemCommandType.GREP) {
                return new SystemCommand(SystemCommandType.GREP, systemCommand.substring(5)); // Remove the "GREP " part
            } else if (splitCommand.length == 2) {
                return new SystemCommand(systemCommandType, splitCommand[1]);
            } else {
                // All possible cases have been covered.
                return null;
            }
        } else {
            return null;
        }
    }

    public static SystemCommandType toDecryptedCommandType(String encryptedCommandType) {
        return SystemCommandType.valueOf(encryptedCommandType);
    }

    /**
     * Return updated member list from on a JOIN_REPLY command.
     *
     * @param command
     * @return updated member list from on a JOIN_REPLY command.
     */
    public static List<GroupMember> parseMemberListFromCommand(SystemCommand command) {
        List<GroupMember> updatedMemberList = new ArrayList<>();
        MemberGroupMain.LOGGER.fine("Parse member list from command " + command.content);
        String[] newMemberAddressList = command.content.split(UdpServent.MEMBER_LIST_SEPARATOR);
        for (String member : newMemberAddressList) {
            List<String> parsedMemberId = parseMemberId(member);
            try {
                updatedMemberList.add(new GroupMember(InetAddress.getByName(parsedMemberId.get(0)), parsedMemberId.get(1)));
            } catch (UnknownHostException | ArrayIndexOutOfBoundsException e) {
                throw new RuntimeException(e);
            }
        }
        return updatedMemberList;
    }

    // The first value is the ip address, and the second value is the join timestamp. The second value will be an empty
    // string if it is not joined yet.
    public static List<String> parseMemberId(String memberId) {
        List<String> output = new ArrayList<>();
        String[] parsedResult = memberId.split(GroupMember.MEMBER_ID_SEPARATOR);
        output.add(parsedResult[0]);
        if (parsedResult.length == 1) {
            output.add("");
        } else {
            output.add(parsedResult[1]);
        }
        return output;
    }
}
