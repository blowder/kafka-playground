package com.blowder.kafka;

import com.blowder.avro.models.DeliveryRecord;
import com.blowder.avro.models.User;
import com.blowder.avro.models.UserInfo;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class DeliveryRecordProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "delivery-record-producer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        final Serde<User> userSerde = new SpecificAvroSerde<>();
        userSerde.configure(serdeConfig, false);
        final Serde<UserInfo> userInfoSerde = new SpecificAvroSerde<>();
        userInfoSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, User> usersStream = builder.stream(Topics.partialUsers.name());
        KStream<Integer, UserInfo> usersInfoStream = builder.stream(Topics.partialUsersInfo.name());
        usersStream.join(usersInfoStream, (u, ui) ->
                        DeliveryRecord.newBuilder()
                                .setUsername(u.getName())
                                .setAddress(ui.getAddress())
                                .setPhone(ui.getPhoneNumber())
                                .build(),
                JoinWindows.of(Duration.ofMinutes(1)),
                StreamJoined.with(Serdes.Integer(), userSerde, userInfoSerde))
                .to(Topics.deliveryRecords.name());

        final Topology topology = builder.build();

        final CountDownLatch latch = new CountDownLatch(1);
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            Runtime.getRuntime().addShutdownHook(
                    new Thread("streams-shutdown-hook") {
                        @Override
                        public void run() {
                            streams.close();
                            latch.countDown();
                        }
                    });
            streams.start();
            latch.await();
        }
    }
}
