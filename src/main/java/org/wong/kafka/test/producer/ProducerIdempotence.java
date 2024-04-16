package org.wong.kafka.test.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class ProducerIdempotence {

    public static void main(String[] args) throws Exception {

        // kafka according the producer situation split into 3 acknowledgement level call acks config to balance the performance and reliability
        // acks value got 0, 1, -1, default level is -1 which is also equal to all
        // acks 0 is first level : priority to ensure performance, and data not reliable, it will directly send to network client
        // acks 1 is second level : as long as broker leader sync and store data but not wait follower finish backup and replica sync data, then will respond and acknowledge
        // acks -1 or all is third level : priority to ensure reliable, but performance not high, broker leader and follower fully sync and replica store (not all partition follower replica, only ISR list replica) only will respond and acknowledge
        // for acks -1 or all, kafka producer actually will not wait all follower replica finish store because some of the replica maybe cannot perform action due to out of resource or network problem sync very long time or unable so just sync ISR list replica

        // kafka will retry to send data to leader if the leader not send back acks acknowledgement long time
        // it keep retry until successfully, we can set the retry times
        // retry may cause data duplicate issue and the data sequence disorder
        // if sequence important, we can enable idempotence config
        // if enable idempotence the acks need to be -1 and also have retries config less than or equal 5 because maxInFlightRequests per connection (default is 5) greater than 5 will have problem
        // idempotence operation only work in same topic partition, it does not support cross partition idempotence, cross session idempotence can be achieved by transaction
        // every partition producer state will be recorded, idempotence rely on partition producer state inside data status to known is it duplicate
        // producer id (random generated) + data sequence number, if the kafka producer restart means also will regenerate producer id again thus causing retry duplicate issue

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configMap.put(ProducerConfig.ACKS_CONFIG, "-1");
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);

        // create producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        for (int i = 1; i < 10; i++) {

            ProducerRecord<String, String> record = new ProducerRecord("test", "key" + i, "value" + i);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("Data send successfully: " + metadata);
                }
            });

            // sender thread get the data from record accumulator and send to kafka server
            System.out.println("Send data ");

        }

        // close producer object
        producer.close();
    }

}
