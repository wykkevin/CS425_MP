package ml;

import org.json.simple.JSONObject;
import sdfs.JsonSerializable;

/**
 * Data class for query input from user
 */
public class RawMLQueryData implements JsonSerializable {
    private String queryFile; // queryFile has the format of query1
    private AvailableModel model;
    private Long batchSize;

    public RawMLQueryData(String queryFile, AvailableModel model, Long batchSize) {
        this.queryFile = queryFile;
        this.model = model;
        this.batchSize = batchSize;
    }

    public String getQueryFile() {
        return queryFile;
    }

    public AvailableModel getModel() {
        return model;
    }

    public Long getBatchSize() {
        return batchSize;
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("queryFile", queryFile);
        jsonObject.put("model", model.toString());
        jsonObject.put("batchSize", batchSize);
        return jsonObject;
    }


}
