package com.blowder.kafka;

import com.blowder.avro.models.UserInfo;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class PartialUserInfoProducer {
    private static final List<String> CITIES = List.of("New York City", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington", "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Mesa", "Sacramento", "Atlanta", "Kansas City", "Colorado Springs", "Omaha", "Raleigh", "Miami", "Long Beach", "Virginia Beach", "Oakland", "Minneapolis", "Tulsa", "Tampa", "Arlington", "New Orleans", "Wichita", "Bakersfield", "Cleveland", "Aurora", "Anaheim", "Honolulu", "Santa Ana", "Riverside", "Corpus Christi", "Lexington", "Henderson", "Stockton", "Saint Paul", "Cincinnati", "St. Louis", "Pittsburgh", "Greensboro", "Lincoln", "Anchorage", "Plano", "Orlando", "Irvine", "Newark", "Durham", "Chula Vista", "Toledo", "Fort Wayne", "St. Petersburg", "Laredo", "Jersey City", "Chandler", "Madison", "Lubbock", "Scottsdale", "Reno", "Buffalo", "Gilbert", "Glendale", "North Las Vegas", "Winston-Salem", "Chesapeake", "Norfolk", "Fremont", "Garland", "Irving", "Hialeah", "Richmond", "Boise", "Spokane", "Baton Rouge");
    private static final List<String> STREETS = List.of("Second", "Third", "First", "Fourth", "Park", "Fifth", "Main", "Sixth", "Oak", "Seventh", "Pine", "Maple", "Cedar", "Eighth", "Elm", "View", "Washington", "Ninth", "Lake", "Hill");

    private static String randAddress() {
        final String city = CITIES.get(randNum(2) % CITIES.size());
        final String street = STREETS.get(randNum(2) % STREETS.size());
        final int apartmentNum = randNum(2);
        return String.join(", ", city, street, Objects.toString(apartmentNum));
    }

    private static int randNum(int length) {
        if (length > 0) {
            int value = 0;
            for (int i = 1; i < length + 1; i++) {
                if (i != 1) value *= 10;
                value += (int) (Math.random() * 10);
            }
            return value;
        }
        return 0;
    }

    public static void main(String[] args) throws InterruptedException {
        Properties conf = new Properties();
        conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.put("schema.registry.url", "http://localhost:8081");
        conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());

        int userId = 0;
        try (KafkaProducer<Integer, UserInfo> producer = new KafkaProducer<>(conf)) {
            while (true) {
                UserInfo userInfo = new UserInfo();
                userInfo.setAddress(randAddress());
                userInfo.setPhoneNumber(randNum(8));
                producer.send(
                        new ProducerRecord<>(Topics.partialUsersInfo.name(), userId++, userInfo),
                        (recordMeta, e) -> {
                            if (e == null) {
                                System.out.printf("Value '%s' has offset '%s', partition '%s'\n",
                                        userInfo, recordMeta.offset(), recordMeta.partition());
                            } else {
                                throw new IllegalStateException(e);
                            }
                        }
                );
                Thread.sleep(100);
            }
        }
    }
}
