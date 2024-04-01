package com.imip.kafka.connect;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.google.gson.JsonObject;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;

public class ImipKafkaOperator {
    private final static Logger logger = LoggerFactory.getLogger(ImipKafkaOperator.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ImipKafkaOperator")
                .setMaster("local[*]")
                .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
                .set("spark.hadoop.fs.s3a.access.key", "NF2uF9BAICYJydkwCn2X")
                .set("spark.hadoop.fs.s3a.secret.key", "jbM1NJApsXGzTJKxCWPwgVIcW6Qiy2diYLzpFUE9")
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
                    

      // Create Spark session
      SparkSession spark = SparkSession.builder()
              .config(conf)
              .getOrCreate();
        // Set Kafka broker properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Change to your Kafka broker address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "imip-consumer-group"); // Specify consumer group
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to Kafka topic
        consumer.subscribe(Collections.singletonList("imip")); // Change to your topic name

        // Start consuming messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Adjust poll duration as needed
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                    // Process the message here
                    System.out.printf("Spark: %s", spark.sessionUUID());
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
                            try {
                                // String deltaPathFile = String.format("/tmp/test/%s", record.topic());
                                String deltaPathFile = String.format("s3a://imip-delta-lake/%s", record.topic());

                                logger.info("deltaPathFile: {}", deltaPathFile);
                                List<String> jsonData = Arrays.asList(payloadObj.get("after").toString());

                                logger.info("data jsonData: {}", jsonData.toString());
                                
                                Dataset<String> tempDataSet = spark.createDataset(jsonData, Encoders.STRING());
                                Dataset<Row> df = spark.read().json(tempDataSet);
                                df.show();
                                // Write DataFrame to Delta Lake
                                df.write()
                                .format("delta")
                                .mode("append")
                                .save(deltaPathFile);
                                
                                df = spark.read().format("delta").load(deltaPathFile);
                                df.show();
                            } catch(Exception e) {
                                e.printStackTrace();
                            }

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
                }
            }
        } finally {
            consumer.close();
        }
    }
}

