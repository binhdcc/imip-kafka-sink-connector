package com.imip.kafka.connect;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import io.delta.tables.*;

import com.google.gson.JsonObject;
import java.time.Duration;
import java.util.Properties;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import io.github.cdimascio.dotenv.Dotenv;
public class ImipKafkaOperator {
    
    private final static Dotenv dotenv = Dotenv.load();
    private final static Logger logger = LoggerFactory.getLogger(ImipKafkaOperator.class);
    // This method testing show data only
    public static void showTable(SparkSession spark, String fullPathTable) {
        Dataset<Row> df = spark.read().format("delta").load(fullPathTable);
        df.show();
    }

    public static Set<String> loadTopicsFromEnv(Dotenv dotenv) {
        String topicsString = dotenv.get("KAFKA_TOPICS");
        String[] topics = topicsString.split(",");
        Set<String> listTopics = new HashSet<>(Arrays.asList(topics));
        return listTopics;
    }

    public static void createRecordDelta(SparkSession spark, String fullPathTable, JsonObject data) {
        List<String> jsonData = Arrays.asList(data.get("after").toString());
        Dataset<String> tempDataSet = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> df = spark.read().json(tempDataSet);
        df.show();
        df.write()
                .format("delta")
                .mode("append")
                .save(fullPathTable);
    }

    public static void updateRecordDelta(SparkSession spark, String fullPathTable, JsonObject conditions,
            JsonObject updates) {
        try {
            logger.info("data in updateRecordDelta: '{}':'{}'", conditions.toString(), updates.toString());
            Dataset<Row> deltaTable = spark.read().format("delta").load(fullPathTable);
            Dataset<Row> filteredData = deltaTable;
            for (String column : conditions.keySet()) {
                String value = conditions.get(column).getAsString();
                logger.info("data-update: '{}':'{}'", column, value);
                filteredData = filteredData.filter(functions.col(column).equalTo(value));
            }
            Dataset<Row> updatedData = filteredData;
            for (String column : updates.keySet()) {
                if (conditions.keySet().contains(column))
                    continue;
                String value = updates.get(column).getAsString();
                logger.info("data-update: '{}':'{}'", column, value);
                updatedData = updatedData.withColumn(column, functions.lit(value));
            }
            updatedData.write().format("delta").mode("overwrite").save(fullPathTable);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public static void deleteRecordDelta(SparkSession spark, String fullPathTable, JsonObject conditions) {
        try {
            DeltaTable deltaTable = DeltaTable.forPath(fullPathTable);

            // Dataset<Row> deltaTable = spark.read().format("delta").load(fullPathTable);
            StringBuilder conditionsBuilder = new StringBuilder();
            int index = 0;
            for (String column : conditions.keySet()) {
                String value = conditions.get(column).getAsString();
                if (index > 0)
                    conditionsBuilder.append(" AND ");
                conditionsBuilder.append(column).append(" = '").append(value).append("'");
                index++;
            }
            deltaTable.delete(conditionsBuilder.toString());

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ImipKafkaOperator")
                .setMaster("local[*]")
                .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
                .set("spark.hadoop.fs.s3a.access.key", "9i0VOsPZnmXqQCGhsVYE")
                .set("spark.hadoop.fs.s3a.secret.key", "XDxIj7jJIokF7JNoEEtyX9kxMwvnjPh2ObrzBidL")
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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, dotenv.get("KAFKA_BROKER")); // Change to your Kafka broker address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, dotenv.get("KAFKA_GROUP_ID")); // Specify consumer group
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to Kafka topic
        consumer.subscribe(loadTopicsFromEnv(dotenv)); // Change to your topic name

        // Start consuming messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Adjust poll duration as needed
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                    // Process the message here
                    System.out.printf("Spark: %s", spark.sessionUUID());

                    JsonObject keyObj = JsonParser.parseString(record.key().toString()).getAsJsonObject();
                    logger.info("key json: {}", keyObj.toString());
                    JsonObject keyPayload = keyObj.getAsJsonObject("payload");
                    logger.info("key payload data: {}", keyPayload.toString());
                    String deltaPathFile = String.format("s3a://imip-delta-lake/%s", record.topic().replace(".", "_").toLowerCase());

                    if (record.value() != null) {
                        JsonObject valueObj = JsonParser.parseString(record.value().toString()).getAsJsonObject();
                        logger.info("value json: {}", valueObj.toString());
                        logger.info("payload: {}", valueObj.get("payload").toString());
                        // create or update
                        JsonObject valuePayload = valueObj.getAsJsonObject("payload");
                        JsonObject afterObj = valuePayload.getAsJsonObject("after");
                        String op = valuePayload.get("op").getAsString();
                        logger.info("after data: {}", valuePayload.get("after").toString());
                        switch(op) {
                            case "c":
                                logger.info("Process case CREATE");
                                try {
                                    logger.info("deltaPathFile: {}", deltaPathFile);
                                    createRecordDelta(spark, deltaPathFile, valuePayload); 
                                    showTable(spark, deltaPathFile);

                                } catch(Exception e) {
                                    e.printStackTrace();
                                }

                                break;
                            case "u":
                                logger.info("Process case UPDATE");
                                updateRecordDelta(spark, deltaPathFile, keyPayload, afterObj);
                                showTable(spark, deltaPathFile);
                                break;
                            default:
                                logger.error("Operator invalid");
                        }
                    }
                    else {
                        // delete 
                        logger.info("Process case DELETE");
                        deleteRecordDelta(spark, deltaPathFile, keyPayload);
                        showTable(spark, deltaPathFile);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
