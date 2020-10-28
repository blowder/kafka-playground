package com.blowder.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RandNumberConsumer {
    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        conf.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        conf.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rand-num-consumers");
        conf.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(conf)) {
            consumer.subscribe(Collections.singletonList(Topics.randNumbers.name()));
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record ->
                        System.out.printf("Arrived number '%s' from partition '%s', offset is '%s'\n",
                                record.value(), record.partition(), record.offset())
                );
            }
        }
    }

}
