package com.blowder.kafka;

import com.blowder.avro.models.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.List;
import java.util.Properties;

public class UserProducer {

    private static final List<String> names = List.of("Jack", "John", "Joseph", "Phil", "Robert");
    private static final List<String> colors = List.of("red", "green", "blue", "white", "black", "gray");

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.put("schema.registry.url", "http://localhost:8081");

        conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        try (KafkaProducer<Integer, User> producer = new KafkaProducer<>(conf)) {
            for (int i = 0; i < 100; i++) {
                final User user = new User();
                user.setName(names.get((int) (Math.random() * 100) % names.size()));
                user.setFavoriteColor(colors.get((int) (Math.random() * 100) % colors.size()));
                user.setFavoriteNumber((int) (Math.random() * 100));

                producer.send(
                        new ProducerRecord<>(Topics.randUsers.name(), user.getFavoriteNumber(), user),
                        (recordMeta, e) -> {
                            if (e == null) {
                                System.out.printf("Value '%s' has offset '%s', partition '%s'\n",
                                        user, recordMeta.offset(), recordMeta.partition());
                            } else {
                                throw new IllegalStateException(e);
                            }
                        }
                );
            }
        }
    }
}
