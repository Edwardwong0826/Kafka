package org.wong.kafka.test.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConsumerGroup1 {
    public static void main(String[] args) {

        // in Kafka, each topic will split to multiple partition, and each partition can only be consumed by one consumer group inside one consumer only
        // example there is 3 producer, one topic with 3 partition (0,1,2), and consumer group with 3 consumer (0,1,2)
        // multiple producer is allowed to produce to one topic multiple partition, but one consumer group inside example consumer no 1 and consumer no 2 cannot consume from same partition 0
        // is ok that consumer group to have more consumer than topic partition, if one of the consumer is down, the idle consumer can act as backup to consume
        // kafka server will store group consumer offset into internal topic called _consumer_offsets, _consumer_offsets inside also split into 50 partitions
        // so same group backup consumer can continue consumed back from last time same group down consumer offset
        // internally, kafka broker server use GroupCoordinatorAdapter exchange with kafka server ConsumerCoordinator to do store group consume offset into _consumer_offsets partitions
        // if consumer 1 and 2 is down, then consumer 0 will eventually consume from partition 0, 1, 2 simultaneously

        // consumer partition assign strategy
        // every consumer group got leader and follower, the leader is the first consume that join the group, and leader will decide consumer consume which from topic partition
        // same group consumer subscribe to one same topic, so consumer group consumer can concurrent consume all data from topic
        // in order to avoid data repeat consume, every topic each partition only will allow consumed by one of the consumer of the consumer group only
        // means no two consumer or more consumer simultaneously consume data from same one partition data, instead one consumer allow to consume data from different partition
        // consumer group consumer better do not more than topic partition quantity, because idle consumer cannot consume data, but can act as backup if wanted

        // when consumer join the group and there is leader exist, it will send the join group request, GroupCoordinatorAdapter will kick existing leader and all exist consumer out of group and elect the new leader,
        // consumers will simultaneously join the group and this time whoever consumer first join will become new leader (every consumer is possible to become leader)
        // and leader responsible assign its follower to consume which partition
        // above process will repeat when there is new consumer join the same consumer group

        // there is common of four Kafka provided partition assign strategy, kafka default choose to use RangeAssignor and CooperativeStickyAssignor
        // 1. RoundRobinAssignor strategy - Round robin topic partition to subscribe consumer, non subscribe consumer will be skipped
        // 2. RangeAssignor strategy - according each topic partition number to calculate and assign, try to allocate average the partition to consumer as possible,
        //    when cannot allocate average, then fill in the head sequentially
        // 3. StickyAssignor strategy - after first assign, every consumer keep the assign partition information, if there is consumer leave and join, then redistribution
        //    try to keep consumer original consume partition not change or less change
        // 4. CooperativeStickyAssignor strategy - optimized StickyAssignor process of redistribution  with using cooperative protocol, this is more high performance to StickyAssignor


        // Message Delivery Guarantees for Producer delivery and Consumer receipt
        // https://docs.confluent.io/kafka/design/delivery-semantics.html - check kafka confluent for semantic guarantees Apache Kafka® provides between the broker and producers and consumers

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "wong");
        configMap.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "aaa"); // consumer member id
        configMap.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()); // only the leader able to config, follow one will ignore

        // create consumer object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);

        // subscribe kafka topic
        consumer.subscribe(Collections.singletonList("test"));
        //consumer.subscribe(Arrays.asList("test1","test2"));

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
