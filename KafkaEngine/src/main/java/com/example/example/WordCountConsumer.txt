package com.example.example;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class WordCountConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "wordcount-group");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("wordcount-topic"));

        Map<String, Integer> wordCounts = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                String[] words =
                        record.value().toLowerCase().split("\\W+");

                for (String word : words) {
                    wordCounts.put(word,
                            wordCounts.getOrDefault(word, 0) + 1);
                }

                System.out.println("Current Word Count:");
                wordCounts.forEach((k, v) ->
                        System.out.println(k + " -> " + v));
                System.out.println("------------");
            }
        }
    }
}
