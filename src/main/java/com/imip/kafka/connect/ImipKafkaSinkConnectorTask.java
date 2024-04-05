package com.imip.kafka.connect;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import com.google.gson.JsonParser;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ImipKafkaSinkConnectorTask extends SinkTask {
    private final static Logger logger = LoggerFactory.getLogger(ImipKafkaSinkConnectorTask.class);
    @Nullable
    private ErrantRecordReporter reporter;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    CopyOnWriteArrayList<Exception> errorsEncountered = new CopyOnWriteArrayList<>();

    @Override
    public void initialize(SinkTaskContext context) {
        super.initialize(context);
        reporter = context.errantRecordReporter();
    }

    @Override
    public void start(Map<String, String> properties) {
    }

    private void processRecord(SinkRecord record) {
        try {
            logger.info("[{}] Record key received: {}", record.topic(), record.key().toString());
            if (record.value() != null) {
                JsonObject jsonObject = JsonParser.parseString(record.value().toString()).getAsJsonObject();
                logger.info("value json: {}", jsonObject.toString());
                logger.info("payload: {}", jsonObject.get("payload").toString());
                // create or update
                JsonObject payloadObj = jsonObject.getAsJsonObject("payload");
                String op = payloadObj.get("op").getAsString();
                logger.info("after data: {}", payloadObj.get("after").toString());
                switch(op) {
                    case "c":
                        logger.info("Process case CREATE");
                        break;
                    case "u":
                        logger.info("Process case UPDATE");
                        break;
                    default:
                        logger.error("Operator invalid");
                }
            }
            else {
                // delete 
                logger.info("Process case DELETE");
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (final SinkRecord record : records) {
            processRecord(record);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Connector Task");

    }

    // public static void main(String[] args) {
    //     logger.info("Test");
    //     String msg = "{\"name\": \"John\"}";
    //     try {
    //         JSONObject obj = new JSONObject(msg);
    //         logger.info(obj.toString());
    //     } catch (Exception e) {
    //         logger.error(e.getMessage());
    //     }
    // }
}

