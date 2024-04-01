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
}