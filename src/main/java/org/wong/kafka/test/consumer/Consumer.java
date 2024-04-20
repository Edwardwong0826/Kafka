package org.wong.kafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class Consumer {

    public static void main(String[] args) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // kafka consumer when consumed, in order to prevent error or down, when restart don't know consume from which position
        // so kafka broker every 5 seconds will store current consume offset, is done by consumer
        // so next time when restart consumer, will consume based on the store offset value not config offset value
        // how it stored the current consume offset is related to this GROUP_ID_CONFIG parameter
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "wong1"); // wong + 1++,
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // latest, earliest, none, default is latest,
        //configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // earliest means fetch from LEO value

        // create consumer object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);

        // subscribe kafka topic
        consumer.subscribe(Collections.singletonList("test"));

        // get cluster meta data
        boolean flag = true;
        while (flag){
            consumer.poll(Duration.ofMillis(100));
            // get consumer topic partition meta data information
            Set<TopicPartition> assignment = consumer.assignment();

            if(assignment != null && !assignment.isEmpty()){
                for(TopicPartition topicPartition : assignment){
                    if("test".equals(topicPartition.topic())){
                        // set the topicPartition offset, so that we can choose from which offset to poll the data
                        consumer.seek(topicPartition, 2);
                        flag = false;
                    }
                }
            }
        }

        // get data from kafka topic
        // consumer pull data from kafka topic, it depends on its capability
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                System.out.println(record);
            }
        }

        // example test topic got 3 partition, each partition offset size is 0,1,2
        // LEO : Log End Offset = size + 1, offset start from 0
        // kafka will set the topic offset value same as LEO
        
        // close consumer object
        // consumer.close();
    }
}
