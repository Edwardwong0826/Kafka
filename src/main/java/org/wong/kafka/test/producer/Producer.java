package org.wong.kafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class Producer {
    public static void main(String[] args) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        // generate record
        ProducerRecord<String, String> record = new ProducerRecord("test","key", "value");

        // use producer object send data to kafka
        producer.send(record);
        // close producer object
        producer.close();
    }
}

