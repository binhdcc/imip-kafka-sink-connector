package com.imip.kafka.connect;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class ImipKafkaOpeator {
    private final static Logger logger = LoggerFactory.getLogger(ImipKafkaOpeator.class);
    public static void main(String[] args) throws InterruptedException {
        // Create Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("ImipKafkaOpeator")
                .setMaster("local[*]"); // Use "local[*]" for testing

        // Create Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create Streaming Context with 1 second batch interval
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));

        // Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "imip-consumer-deltalake"); // Change group id for different consumers
        kafkaParams.put("auto.offset.reset", "latest"); // Change offset reset value if needed

        // Kafka topics to subscribe
        Collection<String> topics = Arrays.asList("imip");

        // Create Kafka direct stream
        JavaInputDStream<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        // Process the received data
        stream.foreachRDD(rdd -> {
            rdd.foreach(record -> {
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

            });
        });

        // Start the computation
        ssc.start();

        // Wait for the computation to terminate
        ssc.awaitTermination();
    }
}
