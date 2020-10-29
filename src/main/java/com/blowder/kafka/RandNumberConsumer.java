package com.blowder.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RandNumberConsumer {
    public static void main(String[] args) {
        String consumerId = args.length > 0 ? args[0] : "0";
        File logfile = new File(String.format("consumer-%s.log", consumerId));
        logfile.delete();

        Properties conf = new Properties();
        conf.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        conf.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        conf.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rand-num-consumers");
        conf.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(conf)) {
            consumer.subscribe(Collections.singletonList(Topics.randNumbers.name()));
            while (true) {
                consumer.poll(Duration.ofMillis(100)).forEach(record -> {

                    final String msg = String.format("Arrived number '%s' from partition '%s', offset is '%s'\n",
                            record.value(), record.partition(), record.offset());
                    System.out.print(msg);
                    log(logfile, msg);
                });
            }
        }
    }

    private static void log(File log, String msg) {
        try (FileWriter writer = new FileWriter(log,true)) {
            writer.append(msg);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
