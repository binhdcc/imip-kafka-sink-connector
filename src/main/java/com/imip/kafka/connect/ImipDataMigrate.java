package com.imip.kafka.connect;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

  private static void executeTrinoQL(String tUrl, String tDbName, String tSchema, String tUser, String tPassword,
      String command) {
    // Establish JDBC connection to Trino
    String fullUrl = String.format("%s/%s/%s", tUrl, tDbName, tSchema);
    try (Connection connection = DriverManager.getConnection(fullUrl, tUser,
        tPassword);
        Statement statement = connection.createStatement()) {

      // Execute SQL statement to create the table
      statement.execute(command);

      logger.info("Trino table created successfully.");

    } catch (SQLException e) {
      logger.error("Error: " + e.getMessage());
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

  private static void doMigrate(SparkSession spark, String jdbcUrl, String dbName, String dbTable, String user,
      String password, String bucketPath, String trinoDbName, String trinoSchema) {
    String tableLocation = String.format("%s/%s", bucketPath, dbTable);
    boolean deltaTableExists = DeltaTable.isDeltaTable(spark, tableLocation);
    if (deltaTableExists) {
      logger.warn("Table {} is exists. Please check again.", dbTable);
      return;
    }

    String url = String.format("%s/%s", jdbcUrl, dbName);
    logger.info("url: {}", url);

    // Read data from JDBC source into DataFrame
    Dataset<Row> jdbcDF = spark.read()
        .format("jdbc")
        .option("url", url)
        .option("dbtable", dbTable)
        .option("user", user)
        .option("password", password)
        .load();

    // Write DataFrame as Delta table
    StructType schema = jdbcDF.schema();

    String ddl = generateTrinoDDL(spark, trinoDbName, trinoSchema, dbTable, bucketPath, schema);
    logger.info("DDL: {}", ddl);
    executeTrinoQL(dotenv.get("TRINO_URL"), dotenv.get("TRINO_DBNAME"), dotenv.get("TRINO_SCHEMA"),
        dotenv.get("TRINO_USER"), dotenv.get("TRINO_PASSWORD"), ddl);

  }

  private static String generateTrinoDDL(SparkSession spark, String trinoDbName, String trinoSchema, String tableName,
      String bucketPath,
      StructType sparkSchema) {

    StringBuilder ddlBuilder = new StringBuilder();

    String tableLocation = String.format("%s/%s", bucketPath, tableName);

    String fullTableName = String.format("%s.%s.%s", trinoDbName, trinoSchema, tableName);

    // Start with the CREATE TABLE statement
    ddlBuilder.append("CREATE TABLE IF NOT EXISTS ")
        .append(fullTableName)
        .append(" (");

    // Append column definitions
    for (StructField field : sparkSchema.fields()) {
      String columnName = String.format("\"%s\"", field.name());
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

  public static void doMigrageMultiple(SparkSession spark, String jdbcUrl, String dbName, String user,
      String password, String bucketPath, String trinoDbName, String trinoSchema) {
    String[] tables = loadJdbcTablesFromEnv(dotenv);
    for (String tableName : tables) {
      doMigrate(spark, jdbcUrl, dbName, tableName, user, password, bucketPath, trinoDbName, trinoSchema);
    }

  }

  public static String[] loadJdbcTablesFromEnv(Dotenv dotenv) {
    String data = dotenv.get("JDBC_TABLE_NAME");
    String[] tables = data.split(",");
    return tables;
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

        doMigrageMultiple(spark,
        dotenv.get("JDBC_URL"),
        dotenv.get("JDBC_DB_NAME"),
        dotenv.get("JDBC_USER"),
        dotenv.get("JDBC_PASSWORD"),
        dotenv.get("BUCKET_PATH"),
        dotenv.get("TRINO_DBNAME"),
        dotenv.get("TRINO_SCHEMA"));

    // Stop SparkSession
    spark.stop();
  }
}
