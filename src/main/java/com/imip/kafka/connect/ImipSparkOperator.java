package com.imip.kafka.connect;
// import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.SparkSession;
// import org.apache.hadoop.conf.Configuration;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

public class ImipSparkOperator {
  // private final Logger logger = LoggerFactory.getLogger(this.toString());
  // private SparkConf sparkConf;
  // private JavaSparkContext sc;
  // private Configuration hadoopConf;
  // private SparkSession sps;

  // ImipSparkOperator() {
  //   // Set up Spark configuration
  //   this.sparkConf = new SparkConf().setMaster("local[*]").setAppName("MinIOReadWrite");
  //   // Create Spark context
  //   this.sc = new JavaSparkContext(sparkConf);

  //   // Configure Hadoop with MinIO credentials
  //   this.hadoopConf = this.sc.hadoopConfiguration();
  //   this.hadoopConf.set("fs.s3a.endpoint", "http://127.0.0.1:9000"); // MinIO endpoint
  //   this.hadoopConf.set("fs.s3a.access.key", "NF2uF9BAICYJydkwCn2X");
  //   this.hadoopConf.set("fs.s3a.secret.key", "jbM1NJApsXGzTJKxCWPwgVIcW6Qiy2diYLzpFUE9");
  //   this.hadoopConf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
  //   this.hadoopConf.set("spark.hadoop.fs.s3a.path.style.access", "true");
  //   this.hadoopConf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
  //       "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

  //   // Create Spark session
  //   this.sps = SparkSession.builder()
  //       .config(this.sparkConf)
  //       .getOrCreate();
  // }

  // public void readData() {
  //   // Read file into DataFrame
  //   Dataset<Row> df = this.sps.read().format("csv")
  //       .option("header", "true") // Assuming the file has a header
  //       .load("s3a://test-spark/output");

  //   df.show();
  //   logger.info("Show content file csv");
  //   logger.info("ABC");

  //   // // Read data from MinIO
  //   // JavaRDD<String> dataRDD = sc.textFile("s3a://test-spark/airtravel.csv");

  //   // // Do some processing (if needed)
  //   // JavaRDD<String> processedRDD = dataRDD.map(line -> line.toUpperCase());

  //   // // Write the processed data back to MinIO
  //   // processedRDD.saveAsTextFile("s3a://test-spark/output");

  //   // Stop Spark context
  //   sc.stop();
  // }
  // // public static void main(String[] args) {
  // //   ImipSparkOperator iso = new ImipSparkOperator();
  // //   iso.readData();
  // // }
}
