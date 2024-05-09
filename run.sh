#!/bin/bash
java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -cp target/kafka-connect-imip-1.0.jar com.imip.kafka.connect.ImipDataMigrate
