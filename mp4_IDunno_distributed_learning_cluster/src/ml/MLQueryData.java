package ml;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sdfs.JsonSerializable;

import java.util.List;

/**
 * Data class for query that is separated in batches
 */
public class MLQueryData implements JsonSerializable {
    private List<String> contentList;
    private AvailableModel availableModel;
    private String result;
    private String queryName; // queryName has a format like query1
    private double runTimeInSeconds;

    public MLQueryData(List<String> contentList, AvailableModel availableModel, String queryName) {
        this.contentList = contentList;
        this.availableModel = availableModel;
        this.queryName = queryName;
    }

    public MLQueryData(List<String> contentList, AvailableModel availableModel, String result, String queryName, double runTimeInSeconds) {
        this.contentList = contentList;
        this.availableModel = availableModel;
        this.result = result;
        this.queryName = queryName;
        this.runTimeInSeconds = runTimeInSeconds;
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        JSONArray contentArray = new JSONArray();
        for (String content : contentList) {
            contentArray.add(content);
        }
        jsonObject.put("content", contentArray);
        jsonObject.put("model", availableModel.toString());
        jsonObject.put("result", result);
        jsonObject.put("queryPath", queryName);
        jsonObject.put("runTime", runTimeInSeconds);
        return jsonObject;
    }

    public List<String> getContentList() {
        return contentList;
    }

    public AvailableModel getAvailableModel() {
        return availableModel;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getQueryName() {
        return queryName;
    }

    public double getRunTimeInSeconds() {
        return runTimeInSeconds;
    }

    public void setRunTimeInSeconds(double runTimeInSeconds) {
        this.runTimeInSeconds = runTimeInSeconds;
    }
}
