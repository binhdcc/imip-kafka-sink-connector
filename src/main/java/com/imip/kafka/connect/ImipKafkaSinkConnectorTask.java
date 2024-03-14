package com.imip.kafka.connect;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ImipKafkaSinkConnectorTask extends SinkTask {
    private final Logger log = LoggerFactory.getLogger(this.toString());

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
        log.info(" Message received: {}", record.value().toString());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (final SinkRecord record : records) {
            processRecord(record);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping Sink Connector Task");

    }
}

