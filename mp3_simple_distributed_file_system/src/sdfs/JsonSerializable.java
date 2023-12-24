package sdfs;

import org.json.simple.JSONObject;

/**
 * Some models in our system needs to be serialized to be included in messages.
 */
public interface JsonSerializable {
    JSONObject toJson();

}
