package com.blowder.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RandNumberCounterConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "number-counter-consumer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<Integer, String>stream(Topics.randNumbers.name())
                .mapValues((ValueMapper<String, Integer>) Integer::parseInt)
                .groupBy((k, v) -> v, Grouped.with(Serdes.Integer(), Serdes.Integer()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count(Materialized.as("count"))
                .toStream()
                .map((w, c) -> new KeyValue<>(w.key(), Objects.toString(c)))
                .to(Topics.randNumbersStats.name(), Produced.with(Serdes.Integer(), Serdes.String()));

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
