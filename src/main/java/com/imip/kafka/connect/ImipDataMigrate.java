package com.imip.kafka.connect;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.tables.DeltaTable;
import io.github.cdimascio.dotenv.Dotenv;

public class ImipDataMigrate {
  private final static Dotenv dotenv = Dotenv.load();
  private final static Logger logger = LoggerFactory.getLogger(ImipKafkaOperator.class);

  private static void executeTrinoQL(String command) {
    // Trino connection properties
    String trinoUrl = "jdbc:trino://172.31.100.19:8080/delta/pps";
    String trinoUser = "admin";
    String trinoPassword = "";
    String tableLocation = "s3a://imip-delta-lake/message2";

    // SQL statement to create the table in Trino
    String createTableSQL = "CREATE TABLE IF NOT EXISTS messages2 ("
        + "id INT,"
        + "name VARCHAR(100)"
        + ") WITH (location = '" + tableLocation + "')";

    // Establish JDBC connection to Trino
    try (Connection connection = DriverManager.getConnection(trinoUrl, trinoUser,
        trinoPassword);
        Statement statement = connection.createStatement()) {

      // Execute SQL statement to create the table
      statement.execute(createTableSQL);

      System.out.println("Trino table created successfully.");

    } catch (SQLException e) {
      System.out.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public static String sparkTypeToTrinoType(DataType sparkType) {
    if (sparkType instanceof StringType) {
        return "VARCHAR";
    } else if (sparkType instanceof IntegerType) {
        return "INTEGER";
    } else if (sparkType instanceof LongType) {
        return "BIGINT";
    } else if (sparkType instanceof DoubleType) {
        return "DOUBLE";
    } else if (sparkType instanceof FloatType) {
        return "REAL";
    } else if (sparkType instanceof BooleanType) {
        return "BOOLEAN";
    } else if (sparkType instanceof ShortType) {
        return "SMALLINT";
    } else if (sparkType instanceof ByteType) {
        return "TINYINT";
    } else if (sparkType instanceof DecimalType) {
        return "DECIMAL";
    } else if (sparkType instanceof DateType) {
        return "DATE";
    } else if (sparkType instanceof TimestampType) {
        return "TIMESTAMP";
    } else if (sparkType instanceof BinaryType) {
        return "VARBINARY";
    } else if (sparkType instanceof ArrayType) {
        return "ARRAY<" + sparkTypeToTrinoType(((ArrayType) sparkType).elementType()) + ">";
    } else if (sparkType instanceof MapType) {
        MapType mapType = (MapType) sparkType;
        return "MAP<" + sparkTypeToTrinoType(mapType.keyType()) + "," + sparkTypeToTrinoType(mapType.valueType()) + ">";
    } else if (sparkType instanceof StructType) {
        StructType structType = (StructType) sparkType;
        StringBuilder sb = new StringBuilder("ROW<");
        for (int i = 0; i < structType.fields().length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            StructField field = structType.fields()[i];
            sb.append(field.name()).append(":").append(sparkTypeToTrinoType(field.dataType()));
        }
        sb.append(">");
        return sb.toString();
    } else {
        // Default case if the type is not recognized
        return "UNKNOWN";
    }
}

  private static String generateTrinoDDL(String tableName, String bucketPath, StructType sparkSchema) {
    StringBuilder ddlBuilder = new StringBuilder();

    String tableLocation = String.format("%s/%s", bucketPath, tableName);

    // Start with the CREATE TABLE statement
    ddlBuilder.append("CREATE TABLE IF NOT EXISTS ")
        .append(tableName)
        .append(" (");

    // Append column definitions
    for (StructField field : sparkSchema.fields()) {
      String columnName = field.name();
      DataType dataType = field.dataType();

      String trinoType = sparkTypeToTrinoType(dataType);
      ddlBuilder.append(columnName).append(" ").append(trinoType).append(", ");
    }

    // Remove the trailing comma and space
    if (ddlBuilder.length() > 2) {
      ddlBuilder.setLength(ddlBuilder.length() - 2);
    }

    // Close the table definition
    ddlBuilder.append(")");
    ddlBuilder.append(" WITH (location = '").append(tableLocation).append("')");
    return ddlBuilder.toString();
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("ImipDataMigrate")
        .setMaster("local[*]")
        .set("spark.hadoop.fs.s3a.endpoint", dotenv.get("MINIO_ENDPOINT"))
        .set("spark.hadoop.fs.s3a.access.key", dotenv.get("MINIO_ACCESS_KEY"))
        .set("spark.hadoop.fs.s3a.secret.key", dotenv.get("MINIO_SECRET_KEY"))
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog");

    // Create Spark session
    SparkSession spark = SparkSession.builder()
        .config(conf)
        .config("hive.metastore.uris", dotenv.get("HIVE_METASTORE_URIS"))
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .enableHiveSupport()
        .getOrCreate();

    // Properties props = new Properties();
    // props.put("user", "postgres");
    // props.put("password", "postgres");
    // // props.put("driver", "org.postgresql.Driver");
    // Dataset<Row> jdbcDF2 = spark.read()
    //     .jdbc("jdbc:postgresql://localhost:5432/tinode", "messages", props);
    // jdbcDF2.show();
    // jdbcDF2.printSchema();

    // Delta table path
    String deltaTablePath = "s3a://imip-delta-lake/messages2";
    // Check if Delta table exists
    boolean deltaTableExists = DeltaTable.isDeltaTable(spark, deltaTablePath);
    deltaTableExists = false;

    if (!deltaTableExists) {
      // Read data from JDBC source into DataFrame
      Dataset<Row> jdbcDF = spark.read()
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/tinode")
          .option("dbtable", "messages")
          .option("user", "postgres")
          .option("password", "postgres")
          .load();

      // Write DataFrame as Delta table
      StructType schema = jdbcDF.schema();
      // Map Spark SQL data types to Trino data types
      String trinoDDL = generateTrinoDDL("messages2", "s3a://imip-delta-lake", schema);

      logger.info("trinoDDL: {}", trinoDDL);
      
      // Stop SparkSession
      spark.stop();
    }
  }
}
