package com.blowder.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

public class RandNumberProducer {
    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(conf)) {
            for (int i = 0; i < 100; i++) {
                int randomValue = (int) (Math.random() * 100);
                producer.send(
                        new ProducerRecord<>(Topics.randNumbers.name(), randomValue % 10, Objects.toString(randomValue)),
                        (recordMeta, e) -> {
                            if (e == null) {
                                System.out.printf("Value '%s' has offset '%s', partition '%s'\n",
                                        randomValue, recordMeta.offset(), recordMeta.partition());
                            } else {
                                throw new IllegalStateException(e);
                            }
                        }
                );
            }
        }
    }
}
