package com.example.example;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer =
                new KafkaProducer<>(props);

        String topic = "wordcount-topic";

        producer.send(new ProducerRecord<>(topic,
                "hello kafka kafka world"));

        producer.close();
        System.out.println("Message sent!");
    }
}
