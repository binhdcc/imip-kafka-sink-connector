package com.imip.kafka.connect;
import org.junit.Test;

import io.github.cdimascio.dotenv.Dotenv;

public class DotenvTest {
  @Test
  public void testEnv() {
    Dotenv dotenv = Dotenv.load();

    // Access environment variables

    String topicsString = dotenv.get("KAFKA_TOPICS");
    String[] topics = topicsString.split(",");

    String minioAccesskey = dotenv.get("MINIO_ACCESS_KEY");
    System.out.println("minioAccessKey: " + minioAccesskey);

    // Use the environment variables in your application
    for (String t : topics) {
      System.out.println(t);
  }
  }
}
