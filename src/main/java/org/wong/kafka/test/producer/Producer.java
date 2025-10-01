package org.wong.kafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class Producer {

    public static void main(String[] args) {

        // every kafka topic have 1 to many paritions, means data will split to different parition
        // every partition have 1 to many replicas
        // each kafka broker only can store one partition replica

        // kafka topic is a logical concepts, only partition is physical concepts and got it own physical folder
        // all partition managed by the Kafka Replica Manager component
        // in this directory C:\Kafka\cluster\broker-1\data we can see got test-1, test3-0 folder, which is represent topic-partition format
        // https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        // generate record
        //ProducerRecord<String, String> record = new ProducerRecord("test","key", "value");

        for(int i =1; i <10; i++){
            // other than these parameters, we can also put partition parameter
            // once the data is constructed and send, we can only use and cannot modify the value anymore
            // if we specify the partition, the producer will straight send without validate, so if we send to a non exist partition number it will be stuck
            ProducerRecord<String, String> record = new ProducerRecord<>("test","key"+i, "value"+i);
            //ProducerRecord<String, String> record = new ProducerRecord("test", 1,"key"+i, "value"+i); // specify send to which partition number
            //ProducerRecord<String, String> record = new ProducerRecord("test", i%3,"key"+i, "value"+i); // use mod to random assign partition

            // we can config or add interceptors before producer data send to validate or do transformation
            // we can add multiple interceptors, and they work in sequence, one if any of the interceptor error does not affect the data send
            // by default kafka do have its own interceptor
            producer.send(record);
        }

        // close producer object
        producer.close();
    }

}

