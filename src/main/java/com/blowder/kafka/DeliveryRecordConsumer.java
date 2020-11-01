package com.blowder.kafka;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DeliveryRecordConsumer {
    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.put("schema.registry.url", "http://localhost:8081");

        conf.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        conf.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class.getName());

        conf.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rand-user-consumers");
        conf.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<Integer, DefaultRecord> consumer = new KafkaConsumer<>(conf)) {
            consumer.subscribe(Collections.singletonList(Topics.deliveryRecords.name()));
            while (true) {
                consumer.poll(Duration.ofMillis(100)).forEach(record ->
                        System.out.printf("Arrived record '%s' from partition '%s', offset is '%s'\n",
                                record.value(), record.partition(), record.offset())
                );
            }
        }
    }
}
