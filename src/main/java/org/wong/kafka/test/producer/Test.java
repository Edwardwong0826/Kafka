package org.wong.kafka.test.producer;

public class Test {

    public static void main(String[] args){

        // this id is refer to ProducerTransaction class TRANSACTIONAL_ID_CONFIG ID and will be hash, and the hash code will be mod with kafka internal topic _transaction_state inside 49 partitions to get _transaction_state fixed partition number
        // this number can be look by topic partition leader to choose the _transaction_state partition, which is using the partition TransactionCoordinator to process transaction
        String id = "my-tx-id";
        System.out.println(Math.abs(id.hashCode()) % 50);
    }
}
