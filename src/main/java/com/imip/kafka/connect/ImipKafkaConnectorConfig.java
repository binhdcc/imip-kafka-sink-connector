package com.imip.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class ImipKafkaConnectorConfig extends AbstractConfig {

    public ImipKafkaConnectorConfig(final Map<?, ?> originalProps) {
        super(CONFIG_DEF, originalProps);
    }

    public static final ConfigDef CONFIG_DEF = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        addParams(configDef);
        return configDef;
    }

    private static void addParams(final ConfigDef configDef) {
        configDef.
                define(
                        "minio.endpoint",
                        Type.STRING,
                        Importance.HIGH,
                        "The Minio endpoint")
                .define(
                        "minio.port",
                        Type.INT,
                        Importance.HIGH,
                        "The Minio port")
                .define(
                        "minio.access_key",
                        Type.STRING,
                        Importance.HIGH,
                        "The Minio access key")
                .define(
                        "minio.secret_key",
                        Type.STRING,
                        Importance.HIGH,
                        "The Minio secret key")
                .define(
                        "task.sleep.timeout",
                        Type.INT,
                        5000,
                        Importance.HIGH,
                        "Sleep timeout used by tasks during each poll");
    }

}
