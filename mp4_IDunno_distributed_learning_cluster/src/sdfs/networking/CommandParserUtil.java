package sdfs.networking;

import ml.AvailableModel;
import ml.MLQueryData;
import ml.RawMLQueryData;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sdfs.SdfsFileMetadata;
import sdfs.UserInputCommand;
import sdfs.UserInputCommandType;

import java.io.IOException;
import java.util.*;

/**
 * Util class to parse the commands. All commands will have the format like "COMMAND_TYPE;optional_command_content"
 */
public class CommandParserUtil {
    private static final String FILE_VERSION_DELIMITER = "#";

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
                } else if (command.equalsIgnoreCase("store-query")) {
                    return new UserInputCommand(UserInputCommandType.STORE_QUERY, content); // store-query queryFile (without .zip)
                } else if (command.equalsIgnoreCase("add-job")) {
                    return new UserInputCommand(UserInputCommandType.ADD_JOB, content); // add-job model queryFile batchSize
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
                } else if (command.equalsIgnoreCase("train")) {
                    return new UserInputCommand(UserInputCommandType.START_TRAINING, null);
                } else if (command.equalsIgnoreCase("show-query-rate")) {
                    return new UserInputCommand(UserInputCommandType.SHOW_QUERY_RATE, null);
                } else if (command.equalsIgnoreCase("show-query-data")) {
                    return new UserInputCommand(UserInputCommandType.SHOW_QUERY_DATA, null);
                } else if (command.equalsIgnoreCase("get-query-result")) {
                    return new UserInputCommand(UserInputCommandType.GET_QUERY_RESULT, null);
                } else if (command.equalsIgnoreCase("stop-query-result")) {
                    return new UserInputCommand(UserInputCommandType.STOP_QUERY_RESULT, null);
                } else if (command.equalsIgnoreCase("show-assignment")) {
                    return new UserInputCommand(UserInputCommandType.SHOW_ASSIGNMENT, null);
                }
            }
        }
        return null;
    }

    public static String insertVersionInFileName(String fileName, long version) {
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

    public static List<GroupMember> decodeMemberList(JSONArray memberArray) {
        List<GroupMember> members = new ArrayList<>();
        for (int i = 0; i < memberArray.size(); i++) {
            JSONObject memberObject = (JSONObject) memberArray.get(i);
            GroupMember member = decodeMember(memberObject);
            members.add(member);
        }
        return members;
    }

    public static RawMLQueryData decodeRawMLQueryData(JSONObject jsonObject) {
        return new RawMLQueryData(
                (String) jsonObject.get("queryFile"),
                AvailableModel.valueOf((String) jsonObject.get("model")),
                (Long) jsonObject.get("batchSize"));
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

    public static JSONArray encodeStringList(List<String> values) {
        JSONArray jsonArray = new JSONArray();
        for (String value : values) {
            jsonArray.add(value);
        }
        return jsonArray;
    }

    public static List<String> decodeStringList(JSONArray jsonArray) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            values.add((String) jsonArray.get(i));
        }
        return values;
    }

    public static JSONArray encodeLongList(List<Long> values) {
        JSONArray jsonArray = new JSONArray();
        for (Long value : values) {
            jsonArray.add(value);
        }
        return jsonArray;
    }

    public static List<Long> decodeLongList(JSONArray jsonArray) {
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            values.add((long) jsonArray.get(i));
        }
        return values;
    }

    public static JSONArray encodeDoubleList(List<Double> values) {
        JSONArray jsonArray = new JSONArray();
        for (Double value : values) {
            jsonArray.add(value);
        }
        return jsonArray;
    }

    public static List<Double> decodeDoubleList(JSONArray jsonArray) {
        List<Double> values = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            values.add((double) jsonArray.get(i));
        }
        return values;
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
                    .decodeStringList((JSONArray) sdfsFileMetadataJsonObject.get("storeLocations"));

            fileMetadata.put(sdfsFilePath, new SdfsFileMetadata(latestVersion, storeLocations));
        }
        return fileMetadata;
    }

    public static MasterInfo decodeMaster(JSONObject resultObject) {
        MasterInfo masterInfo = new MasterInfo();
        masterInfo.setMasterGroupMemberIp((String) resultObject.get("masterGroupMemberIp"));
        masterInfo.setHotReplaceGroupMemberIp((String) resultObject.get("hotReplaceGroupMemberIp"));
        return masterInfo;
    }

    public static MLQueryData decodeMLQueryData(JSONObject queryObject) {
        JSONArray contentArray = (JSONArray) queryObject.get("content");
        List<String> contentList = new ArrayList<>();
        for (int i = 0; i < contentArray.size(); i++) {
            contentList.add((String) contentArray.get(i));
        }
        String model = (String) queryObject.get("model");
        AvailableModel availableModel = AvailableModel.ALEXNET;
        if (model.equals(AvailableModel.ALEXNET.toString())) {
            availableModel = AvailableModel.ALEXNET;
        } else if (model.equals(AvailableModel.RESNET.toString())) {
            availableModel = AvailableModel.RESNET;
        }
        return new MLQueryData(contentList, availableModel, (String) queryObject.get("result"), (String) queryObject.get("queryPath"), (Double) queryObject.get("runTime"));
    }

    public static String unzipLocalDirFileAndReturnPureName(String filePath) {
        String[] splitedFilename = filePath.split("\\.");
        String queryName = splitedFilename[0];
        // Unzip the zip
        try {
            String[] arguments = new String[]{"sh", "-c", "unzip -o ./LocalDir/" + filePath + " -d ./LocalDir/"};
            ProcessBuilder builder = new ProcessBuilder(arguments);
            Process process = builder.start();
            process.waitFor();
            process.destroy();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return queryName;
    }
}
