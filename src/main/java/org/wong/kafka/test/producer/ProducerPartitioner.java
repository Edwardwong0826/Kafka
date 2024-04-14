package org.wong.kafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class ProducerPartitioner {

    public static void main(String[] args) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // add custom partitioner class
        // we can specify how do we send data to partition according the method we define in our custom partitioner class
        configMap.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyKafkaPartitioner.class.getName());

        // create producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        for(int i =1; i <10; i++){

            // other than these parameters, we can also put partition parameter
            // once the data is constructed and send, we can only use and cannot modify the value anymore

            // the key in ProducerRecord is not the key during consume from consumer, its 核心作用就是用来做分区计算
            // if don't have custom partition class and didn't pass in any partition parameter, then it will use the producer record key with algorithm calculate the partition to send
            // kafka in record accumulator class no matter how it will append the partition number according current topic partitioner node load situation to dynamic get partition number if unknown partition detected
            ProducerRecord<String, String> record = new ProducerRecord("test","key"+i, "value"+i);

            producer.send(record);
        }

        // close producer object
        producer.close();

    }
}
