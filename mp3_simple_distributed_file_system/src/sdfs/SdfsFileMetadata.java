package sdfs;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sdfs.networking.CommandParserUtil;

import java.util.List;

/**
 * Data class for the metadata information of the files.
 */
public class SdfsFileMetadata implements JsonSerializable {
    private long latestVersion;
    private List<String> storeLocations;

    public SdfsFileMetadata(long latestVersion, List<String> storeLocations) {
        this.latestVersion = latestVersion;
        this.storeLocations = storeLocations;
    }

    public long getLatestVersion() {
        return latestVersion;
    }

    public List<String> getStoreLocations() {
        return storeLocations;
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("latestVersion", latestVersion);
        jsonObject.put("storeLocations", CommandParserUtil.encodeIpList(storeLocations));
        return jsonObject;
    }
}
