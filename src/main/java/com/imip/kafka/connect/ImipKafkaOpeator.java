package com.imip.kafka.connect;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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

import scala.Tuple2;
import scala.reflect.internal.util.Collections;
import scala.util.control.Exception;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;


class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
      if (instance == null) {
        instance = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate();
      }
      return instance;
    }
  }
@SuppressWarnings("deprecation")
public class ImipKafkaOpeator {
    private final static Logger logger = LoggerFactory.getLogger(ImipKafkaOpeator.class);
    
    public static class ImipMessage implements java.io.Serializable {
        private String key;
        private String value;
      
        public String getKey() {
          return key;
        }
      
        public void setKey(String key) {
          this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
      }
    public static void main(String[] args) throws InterruptedException {
        // ImipSparkOperator iso = new ImipSparkOperator();
        // SparkSession spark;
        // Create Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("ImipKafkaOpeator")
                .setMaster("local[*]")
                .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
                .set("spark.hadoop.fs.s3a.access.key", "NF2uF9BAICYJydkwCn2X")
                .set("spark.hadoop.fs.s3a.secret.key", "jbM1NJApsXGzTJKxCWPwgVIcW6Qiy2diYLzpFUE9")
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

        //  // Create Spark session
        // SparkSession spark = SparkSession.builder()
        //         .config(conf)
        //         .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Create Java Spark Context
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(5000));

        // Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "imip-consumer-deltalake"); 
        kafkaParams.put("auto.offset.reset", "latest");

        // Kafka topics to subscribe
        Collection<String> topics = Arrays.asList("imip");

        // Create Kafka direct stream
        JavaInputDStream<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> kafkaStream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        // Process the received data
        // stream.foreachRDD((rdd, time) -> {

        //         SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
        //         if (record.value() != null) {
        //             JsonObject jsonObject = JsonParser.parseString(record.value().toString()).getAsJsonObject();
        //             logger.info("value json: {}", jsonObject.toString());
        //             logger.info("payload: {}", jsonObject.get("payload").toString());
        //             // create or update
        //             JsonObject payloadObj = jsonObject.getAsJsonObject("payload");
        //             String op = payloadObj.get("op").getAsString();
        //             logger.info("after data: {}", payloadObj.get("after").toString());
        //             switch(op) {
        //                 case "c":
        //                     logger.info("Process case CREATE");
        //                     try {
        //                         String deltaPathFile = String.format("s3a://imip-delta-lake/%s", record.topic());
        //                         List<String> jsonData = Arrays.asList(payloadObj.get("after").toString());
        //                         Dataset<String> tempDataSet = spark.createDataset(jsonData, Encoders.STRING());
        //                         Dataset<Row> df = spark.read().json(tempDataSet);
        //                         // Write DataFrame to Delta Lake
        //                         df.write()
        //                             .format("delta")
        //                             .mode("append") // Overwrite existing data in the Delta table
        //                             .save(deltaPathFile);
        //                     } catch(Exception e) {
        //                         e.printStackTrace();
        //                     }

        //                     break;
        //                 case "u":
        //                     logger.info("Process case UPDATE");
        //                     break;
        //                 default:
        //                     logger.error("Operator invalid");
        //             }
        //         }
        //         else {
        //             // delete 
        //             logger.info("Process case DELETE");
        //         }

        //     });
        // });
        // Process each RDD from Kafka
        // kafkaStream.foreachRDD((rdd, time) -> {
        //     SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
        //     logger.info("Configure before foreach: {}", spark.conf().toString());
        //     List<ConsumerRecord<String,String>> records = rdd.collect();

        //     // Perform actions on the collected data
        //     for (ConsumerRecord<String, String> record : records) {
        //         // Extract key and value from the record
        //         String key = record.key();
        //         String value = record.value();
    
        //         // logger.info("Config spark after: {}", spark.conf().toString());
        //         logger.info("key: {}", key);
        //         logger.info("value: {}", value);
        //         if (spark != null) {
        //             logger.info("spark config: {}", spark)
        //         }
        //     }
            
            kafkaStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> rdd) -> {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                // Process each RDD
                rdd.foreach(record -> {
                    // // Process each record (ConsumerRecord<String, String>)
                    String key = record.key();
                    String value = record.value();
                    logger.info("key: {} \n value: {}", key, value);
                    if (value != null) {
                        JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
                        logger.info("value json: {}", jsonObject.toString());
                        logger.info("payload: {}", jsonObject.get("payload").toString());
                        // create or update
                        JsonObject payloadObj = jsonObject.getAsJsonObject("payload");
                        String op = payloadObj.get("op").getAsString();
                        logger.info("after data: {}", payloadObj.get("after").toString());
                        switch(op) {
                            case "c":
                                logger.info("Process case CREATE");
                                    String deltaPathFile = String.format("s3a://imip-delta-lake/%s", record.topic());
                                    // List<String> jsonData = Arrays.asList(payloadObj.get("after").toString());
                                    // String payloadAfter = payloadObj.get("after").toString()

                                    // Dataset<Row> df = spark.read().json(spark.createDataset(Collections.singletonList(jsonString)));

                                    // Dataset<String> tempDataSet = spark.createDataset(jsonData, Encoders.STRING());
                                    // Dataset<Row> df = spark.read().json(tempDataSet);
                                    // // Write DataFrame to Delta Lake
                                    // df.write()
                                    //     .format("delta")
                                    //     .mode("append") // Overwrite existing data in the Delta table
                                    //     .save(deltaPathFile);

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

            // JavaRDD<ImipMessage> rowRDD = rdd.map(record -> {
            //     ImipMessage im = new ImipMessage();
            //     im.setKey(record.key());
            //     im.setValue(record.value());
            //     return im;
            //   });
            //   Dataset<Row> msg = spark.createDataFrame(rowRDD, ImipMessage.class);
            //   msg.show();

    
            // Dataset<Row> df = spark.createDataFrame(rdd.map(record -> RowFactory.create(record.key(), record.value())), getSchema());
            // df.show();

            // Dataset<String> keyColumnData = df.select("key").as(Encoders.STRING());
            // keyColumnData.show();
            // Dataset<String> valueColumnData = df.select("value").as(Encoders.STRING());
            // valueColumnData.show();

            // // // Write DataFrame to Delta table
            // String deltaPath = "/tmp/delta-table";
            // // df.write().format("delta").mode("append").save(deltaPath);
        // });

        // Start the computation
        jssc.start();

        // Wait for the computation to terminate
        jssc.awaitTermination();
    }
    private static void processRecord(String key, String value, SparkSession spark) {
        logger.info("key: {}", key);
        logger.info("value: {}", value);
        logger.info("spark: {}", spark.conf().toString());
    }
    private static StructType getSchema() {
        return new StructType()
                .add("key", DataTypes.StringType)
                .add("value", DataTypes.StringType);
    }

    
}
