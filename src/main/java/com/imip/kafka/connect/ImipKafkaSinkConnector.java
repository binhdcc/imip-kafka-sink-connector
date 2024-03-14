package com.imip.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.imip.kafka.connect.ImipKafkaConnectorConfig.CONFIG_DEF;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImipKafkaSinkConnector extends SinkConnector {

    private final Logger log = LoggerFactory.getLogger(ImipKafkaSinkConnector.class);

    private Map<String, String> originalProps;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ImipKafkaSinkConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> originalProps) {
        this.originalProps = originalProps;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(originalProps);
        }
        return configs;
    }

    @Override
    public void stop() {
    }
}
