## Solution

### Apache Beam pipeline to perform following functions

Due to time constraint and issue with local kafka instance, I directly tested from file to Cassandra but written pipelines interfacing kafka as well.

 - org.kafkatocas.JsonFileToCassandra 
   - Read user location data file from file system.
   - Split single line multi record json to multiple records.
   - Read location reference data from a file and load it as a side input. This reference data is loaded every hour (configurable).
   - Join user location data with reference data.
   - Add the merged data to denormalized table in cassandra.
 - org.kafkatocas.JsonFileToKafka
   - Read user location data file from file system.
   - Split single line multi record json to multiple records.
   - Write the records to kafka topic.
 - org.kafkatocas.KafkaToCassandra
   - Read the user location data from kafka topic.
   - Read location reference data from a file and load it as a side input. This reference data is loaded every hour (configurable).
   - Join user location data with reference data.
   - Add the merged data to denormalized table in cassandra.

./pipeline/deployment.sh contains steps to compile and deploy as direct runner.
The sample test file needs to be placed under ./pipeline/source-data/pedestrian and file names needs to be modified in the deployment.sh
