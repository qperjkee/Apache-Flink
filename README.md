#Stream Processing for Created Orders with Apache Flink
This repository contains a Java application for stream processing using Apache Flink. The application processes order events from a Kafka topic, detects anomalies in the processed events, and writes the processed events and anomalies back to Kafka topics.

##Usage
Setup Apache Flink: Ensure that Apache Flink is properly installed and configured on your system.

Setup Kafka: Make sure Kafka is set up and running, and you have a topic with order events.

Build the Application: Build the Java application using Maven or your preferred build tool.

Run the Application: Execute the StreamProcessing class with appropriate parameters.

##Components
The main components of the application are:

ParseOrder: This class implements the MapFunction interface to parse incoming JSON order events and extract relevant information such as orderId, userId, timestamp, storeName, deliveryAddress, userCart, and totalPrice.

GroupOrder: This class implements the KeySelector interface to extract the userID and orderId as the key for grouping.

DetectAnomaly: This class implements the FilterFunction interface to detect anomalies in the processed order events. It filters events based on a predefined anomaly threshold (ANOMALY_THRESHOLD) within a specific time window (DAY_IN_MILLISECONDS).

ExtractAnomaly: This class implements the MapFunction interface to extract anomaly information from the processed order events. It extracts the userId, orderId, and anomalyType.

processOrderEvents: This method processes the order events by mapping, flat-mapping, keying, counting, and updating with windowing.

detectAnomalies: This method detects anomalies in the processed order events by filtering and mapping.

writeProcessedEventsAndAnomalies: This method writes the processed events and anomalies back to Kafka topics using KafkaSink.

##Dependencies
Apache Flink
Apache Kafka
JSON library for Java (e.g., org.json)

##Note
Ensure that Kafka is properly configured and accessible from the Apache Flink application. Also, adjust the anomaly threshold and time window parameters according to your requirements in the DetectAnomaly class.
