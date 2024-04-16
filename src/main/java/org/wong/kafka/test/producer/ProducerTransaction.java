package org.wong.kafka.test.producer;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class ProducerTransaction {

    public static void main(String[] args) throws Exception {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configMap.put(ProducerConfig.ACKS_CONFIG, "-1");
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);

        // if sequence important, we can enable idempotence config
        // if enable idempotence the acks need to be -1 and also have retries config less than or equal 5 because maxInFlightRequests per connection (default is 5) greater than 5 will have problem
        // idempotence operation only work in same topic partition, it does not support cross partition idempotence, cross session idempotence can be achieved by transaction
        // every partition producer state will be recorded, idempotence rely on partition producer state inside data status to known is it duplicate
        // producer id (random generated) + data sequence number, if the kafka producer restart means also will regenerate producer id again thus causing retry duplicate issue

        // once click run this, when click in kafka tools retrieve data, remember stop loading messages it will be able to show
        // also _transaction_state will create by kafka, this is the internal topic managed kafka during transaction
        // inside got 50 partitions, speciality for store transaction state data, our transaction data will be store in one of the partition
        // this transactional id will be hash, and the hash code will be mod with kafka internal topic _transaction_state inside 49 partitions to get _transaction_state fixed partition number
        // this number can be look by topic partition to choose the _transaction_state partition, which is using the partition inside TransactionCoordinator to process transaction
        // refer to Test class to get the fixed partition number and check in kafka tools
        // there are still some missing flow, for complete flow please refer to Kafka production data - transactions flow - 1 diagram

        // data transfer semantics 数据传输语义
        // at most once - only send one, don't care is it receive - ACKS level 0
        // at least once - retry to send until receive, possible of duplicate data issue - ACKS level 1
        // exactly once - data only receive one time, will not lose and not repeat - idempotence + transcation + ACKS level -1
        configMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-tx-id");

        // create producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        // initialize transaction
        // once use transaction, producer id will not change and idempotence will guarantee
        // every time when Kafka broker server startup, TransactionCoordinator will be instantiated
        producer.initTransactions();

        try{

            // begin transaction
            producer.beginTransaction();

            for (int i = 1; i < 10; i++) {

                ProducerRecord<String, String> record = new ProducerRecord("test3", "key" + i, "value" + i);

                final Future<RecordMetadata> send = producer.send(record);

            }

            // commit transaction
            producer.commitTransaction();

        } catch (Exception exception){
            exception.printStackTrace();
            // stop transaction
            producer.abortTransaction();
        }
        finally {
            producer.close();
        }

    }

}
