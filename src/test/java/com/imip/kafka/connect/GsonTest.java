package com.imip.kafka.connect;
import org.junit.Test;
import com.google.gson.*;

public class GsonTest {
    @Test
    public void testKeyValueParser() {
        // Your JSON string
        String jsonString = "{\"ID\":41,\"FirstName\":\"Nguyen\",\"LatName\":\"Hoàng2\",\"Email\":\"ngoc@gmail.com\"}";

        // Parse the JSON string into a JsonObject
        JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();

        // Iterate over the entry set of the JsonObject
        for (java.util.Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            JsonElement value = entry.getValue();

            // Print the key and value
            System.out.println("Key: " + key + ", Value: " + value);
        }
    }

    @Test
    public void generateConditions() {
        // Example JSON input
        String jsonString = "{\"conditions\": [{\"column\": \"column1\", \"value\": \"value1\"}, {\"column\": \"column2\", \"value\": \"value2\"}]}";

        // Parse JSON string
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
        JsonArray conditionsArray = jsonObject.getAsJsonArray("conditions");

        // Generate conditions
        StringBuilder conditionsBuilder = new StringBuilder();
        for (int i = 0; i < conditionsArray.size(); i++) {
            JsonObject conditionObject = conditionsArray.get(i).getAsJsonObject();
            String column = conditionObject.get("column").getAsString();
            String value = conditionObject.get("value").getAsString();

            // Construct condition
            conditionsBuilder.append(column).append(" = '").append(value).append("'");

            // Append "AND" for all conditions except the last one
            if (i < conditionsArray.size() - 1) {
                conditionsBuilder.append(" AND ");
            }
        }

        // Output generated conditions
        String conditions = conditionsBuilder.toString();
        System.out.println("Generated conditions: " + conditions);
    }
}