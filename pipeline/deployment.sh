#!/bin/sh
jar_file="./target/json-to-kafka-pipeline-1.1-SNAPSHOT-shaded.jar"
kafka_broker="127.0.0.1:9092"
kafka_topic="user-location-data"
input_file_path="./source-data/pedestrian/pedestrian-counting-system-monthly-counts-per-hour_backup.json"
reference_file_path="./source-data/pedestrian/pedestrian-counting-system-sensor-locations.json"

mvn clean install

java -jar ${jar_file} \
--bootstrapServers=${kafka_broker} --topic=${kafka_topic} --fileFullPath=${input_file_path} --referenceFilePath=${reference_file_path}  \
--cassandraHost='localhost' --cassandraPort=7000 -runner=Direct



