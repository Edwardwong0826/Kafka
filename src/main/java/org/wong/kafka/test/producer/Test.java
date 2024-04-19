package org.wong.kafka.test.producer;

public class Test {

    public static void main(String[] args){

        // this id is refer to ProducerTransaction class TRANSACTIONAL_ID_CONFIG ID and will be hash, and the hash code will be mod with kafka internal topic _transaction_state inside 49 partitions to get _transaction_state fixed partition number
        // this number can be look by topic partition leader to choose the _transaction_state partition, which is using this partition TransactionCoordinator to process transaction
        String id = "my-tx-id";
        System.out.println(Math.abs(id.hashCode()) % 50);

        // Kafka data storage info
        // kafka data actually store data in .log file, and Log Manager is the one flush data from memory to storage
        // kafka will split data to log segment file according two condition, one is the size of file, if log segment file > 1gb, will create another log segment continue to write
        // second is every 7 days kafka will roll the log segment file as log segment file, only the last log segment file got read write permission, previous log segment only got read

        // example server config for kafka broker server to generate many log segment files
        // log.flush.interval.messages = 1
        // log.segment.bytes = 200
        // log.roll.ms=5

        // add in KafkaProducer config map
        // configMap.put(ProducerConfig.BATCH_SIZE_CONFIG,2);

        // kafka provide the tools to examine or read the log segment file (.index, .log, .timeindex)
        // need to use it inside source code kafka.tools package DumpLogSegments class function
        // we can run it via cmd.bat under this directory C:/Kafka/cluster/broker-1/bin/windows
        // command - kafka-run-class.bat kafka.tools.DumpLogSegments --files C:/Kafka/cluster/broker-1/data/test-1/00000000000000000000.log --print-data-log

        // the log segments file actually name by the offset value, means different log segment file start at different offset
        // log segments file actually consist of 3 file which .index file, .log file and .timeindex file as one complete log segment

        // for Kafka data storage data synchronize consistency problem
        // basically there is watermark level (consider as virtual offset) across all partitions, consumer can only pull data within watermark,
        // those above watermark consumer cannot see it , the watermark level only increases after the fhe follower completed replicas and store the data,
        // and if leader broker is down, it will be elected other broker with the data within watermark level only,
        // those above watermark data will be discarded to ensure synchronize is consistency

        // there will be LOE -Log End Offset , HW - High Water status in each broker, and leader broker will contain other follow broker LEO as well
        // please refer to kafka folder image for more flows info

        // for ISR status, if the broker out of connect like 15 seconds, the ISR list will remove that broker server
        // ISR status can use to elect the new broker leader if the leader out of connect
    }
}
