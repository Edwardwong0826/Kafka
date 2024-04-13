package org.wong.kafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class ProducerInterceptor {

    public static void main(String[] args) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // add custom interceptor class
        configMap.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ValueInterceptor.class.getName());

        // create producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        // generate record
        //ProducerRecord<String, String> record = new ProducerRecord("test","key", "value");

        for(int i =1; i <10; i++){
            // other than these parameters, we can also put partition parameter
            // once the data is constructed and send, we can only use and cannot modify the value anymore
            ProducerRecord<String, String> record = new ProducerRecord("test1","key"+i, "value"+i);

            // we can config or add interceptors before producer data send to validate or do transformation
            // we can add multiple interceptors, and they work in sequence, one if any of the interceptor error does not affect the data send
            // by default kafka do have its own interceptor
            producer.send(record);
        }
        // use producer object send data to kafka

        // close producer object
        producer.close();
    }

}
