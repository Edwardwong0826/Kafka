package org.wong.kafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Consumer {

    public static void main(String[] args) {

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
            ConsumerRecords<String, String> records = consumer.poll(100);

            for(ConsumerRecord<String,String> record : records){
                System.out.println(record);
            }
        }
        
        // close consumer object
        // consumer.close();
    }
}
