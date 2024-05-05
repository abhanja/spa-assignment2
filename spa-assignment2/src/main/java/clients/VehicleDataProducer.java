package clients;

import java.lang.System;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.time.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.eclipse.paho.client.mqttv3.MqttException;

public class VehicleDataProducer {
  public static void main(String[] args) {
    try {
      String topic = "topic_0";
      final Properties config = readConfig("src/main/resources/application.properties");
      
      produce(topic, config);
      //consume(topic, config);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Properties readConfig(final String configFile) throws IOException {
    // reads the client configuration from client.properties
    // and returns it as a Properties object
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }

    final Properties config = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      config.load(inputStream);
    }

    return config;
  }

  public static void produce(String topic, Properties config) {
    // sets the message serializers
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // creates a new producer instance and sends a sample message to the topic
    String key = "key";
    String value = "value";
    KafkaProducer<String, String> producer = new KafkaProducer<>(config);
    MosquittoSubscriber MosquittoSubscriber = new MosquittoSubscriber(producer);
    try {
		MosquittoSubscriber.start();
	} catch (MqttException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
//    producer.send(new ProducerRecord<>(topic, key, value));
//    System.out.println(
//      String.format(
//        "Produced message to topic %s: key = %s value = %s", topic, key, value
//      )
//    );
//
//    // closes the producer connection
//    producer.close();
  }

  public static void consume (String topic, Properties config) {
    // sets the group ID, offset and message deserializers
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "java-group-1");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // creates a new consumer instance and subscribes to messages from the topic
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Arrays.asList(topic));
    
    // MongoDB connection string
    String connectionString = "mongodb+srv://tripathi1307shubh:y6D7IpstX6vkQ5hS@cluster0.xaomxoi.mongodb.net/";
    MongoCollection<Document> collection = null;
    // Create a MongoDB client
    try (MongoClient mongoClient = MongoClients.create(connectionString)) {
        // Connect to the database
        MongoDatabase database = mongoClient.getDatabase("truck");
        // Get the collection
        collection = database.getCollection("position_collection");
        // Create a document to insert
    } catch (Exception e) {
        e.printStackTrace();
    }
    
    while (true) {
      // polls the consumer for new messages and prints them
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(
          String.format(
            "Consumed message from topic %s: key = %s value = %s", topic, record.key(), record.value()
          )
        );
        Document document = Document.parse(record.value());
        // Insert the document into the collection
        collection.insertOne(document);
        System.out.println(document + "Document inserted successfully!");
      }
    }
  }
}