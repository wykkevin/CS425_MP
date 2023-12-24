package sdfs.networking;

import org.json.simple.JSONObject;
import sdfs.JsonSerializable;
import sdfs.Main;
import sdfs.SdfsFileMetadata;

import java.net.InetAddress;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class MasterInfo implements JsonSerializable {

    private GroupMember masterGroupMember;
    private List<String> backupMetadataStoreLocations;

    public MasterInfo(GroupMember masterGroupMember) {
        this.masterGroupMember = masterGroupMember;
        backupMetadataStoreLocations = new ArrayList<>();
    }

    public MasterInfo() {

    }

    public static void sendFileMetadataTo(InetAddress targetMemberIp, Map<String, SdfsFileMetadata> fileMetadata) {
        JSONObject messageJsonObject = new JSONObject();
        messageJsonObject.put("command", MessageType.PUT_BACKUP_METADATA.toString());

        JSONObject fileMetadataJsonObject = new JSONObject();
        for (Map.Entry<String, SdfsFileMetadata> fileMetadataEntry : fileMetadata.entrySet()) {
            fileMetadataJsonObject.put(fileMetadataEntry.getKey(), fileMetadataEntry.getValue().toJson());
        }
        messageJsonObject.put("fileMetadata", fileMetadataJsonObject);

        Main.udpServent.sendMessage(messageJsonObject.toJSONString(), targetMemberIp, UdpServent.GROUP_PORT);
    }

    public void addToBackupMetadataStoreLocations(String ip) {
        backupMetadataStoreLocations.add(ip);
    }

    public void setMasterGroupMember(GroupMember masterGroupMember) {
        this.masterGroupMember = masterGroupMember;
    }

    public GroupMember getMasterGroupMember() {
        return masterGroupMember;
    }

    public List<String> getBackupMetadataStoreLocations() {
        return backupMetadataStoreLocations;
    }

    public void setBackupMetadataStoreLocations(List<String> backupMetadataStoreLocations) {
        this.backupMetadataStoreLocations = backupMetadataStoreLocations;
    }


    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("masterGroupMember", masterGroupMember.toJson());
        jsonObject.put("backupMetadataStoreLocations", CommandParserUtil.encodeIpList(backupMetadataStoreLocations));
        return jsonObject;
    }
}
