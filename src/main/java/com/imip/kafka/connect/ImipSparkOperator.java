package com.imip.kafka.connect;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.apache.spark.sql.Encoders;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.List;

public class ImipSparkOperator {
  private final static Logger logger = LoggerFactory.getLogger("ImipSparkOperator");
  private SparkConf conf;
  private SparkSession spark;
  // private MinioClient minioClient;

  public ImipSparkOperator() {
    // Set up Spark configuration
    // this.sparkConf = new
    // SparkConf().setMaster("localhost").setAppName("MinIOReadWrite");
    // // Create Spark context
    // this.jsc = new JavaSparkContext(sparkConf);
    // // this.minioClient = MinioClient.builder()
    // // .endpoint("http://127.0.0.1:9000")
    // // .credentials("NF2uF9BAICYJydkwCn2X",
    // // "jbM1NJApsXGzTJKxCWPwgVIcW6Qiy2diYLzpFUE9")
    // // .build();

    // // Configure Hadoop with MinIO credentials
    // this.hadoopConf = this.jsc.hadoopConfiguration();
    // this.hadoopConf.set("fs.s3a.endpoint", "http://127.0.0.1:9000"); // MinIO
    // endpoint
    // this.hadoopConf.set("fs.s3a.access.key", "NF2uF9BAICYJydkwCn2X");
    // this.hadoopConf.set("fs.s3a.secret.key",
    // "jbM1NJApsXGzTJKxCWPwgVIcW6Qiy2diYLzpFUE9");
    // this.hadoopConf.set("spark.hadoop.fs.s3a.impl",
    // "org.apache.hadoop.fs.s3a.S3AFileSystem");
    // this.hadoopConf.set("spark.hadoop.fs.s3a.path.style.access", "true");
    // this.hadoopConf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
    // "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

    this.conf = new SparkConf()
    .setAppName("WriteJsonToDeltaLakeAndMinIO")
    .setMaster("local[*]") // Set master URL for local mode
    .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .set("spark.hadoop.fs.s3a.access.key", "NF2uF9BAICYJydkwCn2X")
    .set("spark.hadoop.fs.s3a.secret.key", "jbM1NJApsXGzTJKxCWPwgVIcW6Qiy2diYLzpFUE9")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

    // Create Spark session
    this.spark = SparkSession.builder()
        .config(this.conf)
        .getOrCreate();


    // // Create Spark session
    // SparkSession spark = SparkSession.builder()
    //     .config(conf)
    //     .getOrCreate();
  }

  public void loadJsonKeyValue(JsonObject jsonObject) {
    for (String key : jsonObject.keySet()) {
      System.out.println(key);
      System.out.println(jsonObject.get(key));
    }
  }

  public void updateRecord(String topic, JsonObject record, String key) {
    String parquetPath = String.format("s3a://imip-delta-lake/%s", topic);
    // Read file into DataFrame
    Dataset<Row> df = this.spark.read().format("parquet")
        .load(parquetPath);
    this.spark.read().json("ABC");

    // Filter data based on ID
    String keyRow = DigestUtils.sha256Hex(key);

    Dataset<Row> filteredDF = df.filter(df.col("keyrow").equalTo(keyRow));

    List<String> jsonData = Arrays.asList(record.toString());
    Dataset<String> dataString = this.spark.createDataset(jsonData, Encoders.STRING());
    Dataset<Row> dataRow = this.spark.read().json(dataString);

    filteredDF.union(dataRow);

    // Show filtered data
    filteredDF.show();

    // Write filtered data to Parquet
    filteredDF.write().format("parquet")
        .mode("overwrite")
        .save(parquetPath);
  }

  public void createRecord(String topic, String record) {
    String deltaPathFile = String.format("s3a://imip-delta-lake/%s", topic);
    List<String> jsonData = Arrays.asList(record);
    Dataset<String> tempDataSet = this.spark.createDataset(jsonData, Encoders.STRING());
    Dataset<Row> df = spark.read().json(tempDataSet);
    // Write DataFrame to Delta Lake
    df.write()
        .format("delta")
        .mode("append") // Overwrite existing data in the Delta table
        .save(deltaPathFile);
  }

  public void deleteRecord(String topic, JsonObject record) {

  }

  // public static void main(String[] args) {
    // ImipSparkOperator iso = new ImipSparkOperator();
    // logger.info("ABC");
  // }
}
