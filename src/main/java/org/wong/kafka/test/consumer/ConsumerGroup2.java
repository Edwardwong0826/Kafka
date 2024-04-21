package org.wong.kafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConsumerGroup2 {
    public static void main(String[] args) {

        // in Kafka, each topic will split to multiple partition, and each partition can only be consumed by one consumer group inside one consumer only
        // example there is 3 producer, one topic with 3 partition (0,1,2), and consumer group with 3 consumer (0,1,2)
        // multiple producer is allowed to produce to one topic multiple partition, but one consumer group inside example consumer no 1 and consumer no 2 cannot consume from same partition 0
        // is ok that consumer group to have more consumer than topic partition, if one of the consumer is down, the idle consumer can act as backup to consume
        // kafka server will store group consumer offset into internal topic called _consumer_offsets, _consumer_offsets inside also split into 50 partitions
        // so same group backup consumer can continue consumed back from last time same group down consumer offset
        // internally, kafka borker server use GroupCoordinatorAdapter exchange with kafka server ConsumerCoordinator to do store group consume offset into _consumer_offsets partitions
        // if consumer 1 and 2 is down, then consumer 0 will eventually consume from partition 0, 1, 2 simultaneously

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "wong");

        // create consumer object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);

        // subscribe kafka topic
        consumer.subscribe(Collections.singletonList("test"));

        // get data from kafka topic
        // consumer pull data from kafka topic, it depends on its capability
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                System.out.println(record.partition());
            }
        }

        // close consumer object
        // consumer.close();
    }
}
