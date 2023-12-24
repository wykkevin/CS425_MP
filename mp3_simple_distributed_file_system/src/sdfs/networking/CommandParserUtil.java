package sdfs.networking;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sdfs.SdfsFileMetadata;
import sdfs.UserInputCommand;
import sdfs.UserInputCommandType;

import java.util.*;

/**
 * Util class to parse the commands. All commands will have the format like "COMMAND_TYPE;optional_command_content"
 */
public class CommandParserUtil {

    public static UserInputCommand parseUserInput(String userInput) {
        String[] splitInput = userInput.split(" ");
        if (splitInput.length > 0) {
            String command = splitInput[0];
            if (userInput.length() >= command.length() + 1) {
                String content = userInput.substring(command.length() + 1); // Remove command text and the space
                if (command.equalsIgnoreCase("grep")) {
                    return new UserInputCommand(UserInputCommandType.GREP, userInput);
                } else if (command.equalsIgnoreCase("put")) { // put localfilename sdfsfilename
                    return new UserInputCommand(UserInputCommandType.PUT, content);
                } else if (command.equalsIgnoreCase("get")) { // get sdfsfilename localfilename
                    return new UserInputCommand(UserInputCommandType.GET, content);
                } else if (command.equalsIgnoreCase("delete")) { // delete sdfsfilename.
                    return new UserInputCommand(UserInputCommandType.DELETE, content);
                } else if (command.equalsIgnoreCase("ls")) { // ls sdfsfilename.
                    return new UserInputCommand(UserInputCommandType.LS, content);
                } else if (command.equalsIgnoreCase("get-versions")) {
                    return new UserInputCommand(UserInputCommandType.GET_VERSIONS, content);
                } else if (command.equalsIgnoreCase("linux-command")) {
                    return new UserInputCommand(UserInputCommandType.LINUX_COMMAND, content);
                }
            } else {
                if (command.equalsIgnoreCase("join")) {
                    return new UserInputCommand(UserInputCommandType.JOIN, null);
                } else if (command.equalsIgnoreCase("leave")) {
                    return new UserInputCommand(UserInputCommandType.LEAVE, null);
                } else if (command.equalsIgnoreCase("list_mem")) {
                    return new UserInputCommand(UserInputCommandType.LIST_MEM, null);
                } else if (command.equalsIgnoreCase("list_self")) {
                    return new UserInputCommand(UserInputCommandType.LIST_SELF, null);
                } else if (command.equalsIgnoreCase("store")) {
                    return new UserInputCommand(UserInputCommandType.STORE, null);
                }
            }
        }
        return null;
    }

    public static List<GroupMember> decodeMemberList(JSONArray memberArray) {
        List<GroupMember> members = new ArrayList<>();
        for (int i = 0; i < memberArray.size(); i++) {
            JSONObject memberObject = (JSONObject) memberArray.get(i);
            GroupMember member = decodeMember(memberObject);
            members.add(member);
        }
        return members;
    }

    public static GroupMember decodeMember(JSONObject memberObject) {
        return new GroupMember((String) memberObject.get("ip"), (String) memberObject.get("joinTimestamp"), (Long) memberObject.get("ringId"));
    }

    public static JSONArray encodeMemberList(List<GroupMember> members) {
        JSONArray memberArray = new JSONArray();
        for (GroupMember member : members) {
            memberArray.add(member.toJson());
        }
        return memberArray;
    }

    public static JSONArray encodeIpList(List<String> ips) {
        JSONArray ipJsonArray = new JSONArray();
        for (String ip : ips) {
            ipJsonArray.add(ip);
        }
        return ipJsonArray;
    }

    public static List<String> decodeIpList(JSONArray ipArray) {
        List<String> ips = new ArrayList<>();
        for (int i = 0; i < ipArray.size(); i++) {
            ips.add((String) ipArray.get(i));
        }
        return ips;
    }

    public static HashMap<String, SdfsFileMetadata> decodeFileMetadata(JSONObject resultObject) {
        HashMap<String, SdfsFileMetadata> fileMetadata = new HashMap<>();
        JSONObject newFileMetadataFromBackupJsonObject = (JSONObject) resultObject.get("fileMetadata");
        Set<Map.Entry<Object, Object>> entries = newFileMetadataFromBackupJsonObject.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {

            String sdfsFilePath = (String) entry.getKey();

            JSONObject sdfsFileMetadataJsonObject = (JSONObject) entry.getValue();
            Long latestVersion = (Long) (sdfsFileMetadataJsonObject).get("latestVersion");
            List<String> storeLocations = CommandParserUtil
                    .decodeIpList((JSONArray) sdfsFileMetadataJsonObject.get("storeLocations"));

            fileMetadata.put(sdfsFilePath, new SdfsFileMetadata(latestVersion, storeLocations));
        }
        return fileMetadata;
    }

    public static MasterInfo decodeMaster(JSONObject resultObject) {
        MasterInfo masterInfo = new MasterInfo();
        masterInfo.setMasterGroupMember(decodeMember((JSONObject) resultObject.get("masterGroupMember")));
        masterInfo.setBackupMetadataStoreLocations(decodeIpList((JSONArray) resultObject.get("backupMetadataStoreLocations")));
        return masterInfo;
    }

}
