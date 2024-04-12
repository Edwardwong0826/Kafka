package org.wong.kafka.test.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdminTopic {
    public static void main(String[] args){

        // only the controller broker can create topic etc
        // we cannot ensure which one is the controller broker, so admin object sent request to broker 9092 socket server -> kafka APIs and get cluster info from Metadata and return cluster info as response to admin
        // once know which broker is controller broker, send another request to that broker example 9091 socket server -> kafka APIs ->  ZKClient send request to ZooKeeper to create topics under /brokers/topics
        // finally there is partition state machine and replica state machine in controller broker will listen to Zookeeper /brokers/topics to create the necessary partitions and replicas and sent the result to other brokers
        // others broker will use replicaManager to create the replicas, why is so complicated is because kafka using producer and consumer model and there is many primary keys, so using this way to decouple
        Map<String,Object> configMap = new HashMap<>();
        configMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // admin object
        Admin admin = Admin.create(configMap);

        String topicName1 = "test1";
        int partitions1 = 1;
        short replications1 = 1;

        NewTopic topic1 = new NewTopic(topicName1, partitions1, replications1);

        String topicName2 = "test2";
        int partitions2 = 2;
        short replications2 = 2;

        NewTopic topic2 = new NewTopic(topicName2, partitions2, replications2);

        String topicName3 = "test3";
        Map<Integer, List<Integer>> replicaAssignmentMap = new HashMap<>();

        // when create topic with partition and replicas, we can choose to assign our self
        // this will require us to have good understanding about the cluster resource config, else when causing the performance not good, if not sure just use default strategy provider by kafka
        // first parameter is partition, second parameter is to put to which broker, and list first element is the leader, the rest of list element is the followers
        replicaAssignmentMap.put(0, Arrays.asList(3,1));
        replicaAssignmentMap.put(1, Arrays.asList(2,3));
        replicaAssignmentMap.put(2, Arrays.asList(1,2));

        NewTopic topic3 = new NewTopic(topicName3, replicaAssignmentMap);

        // create topic
        CreateTopicsResult result = admin.createTopics(
                Arrays.asList(topic1, topic2, topic3)
        );

        // In - Sync - Replicas : 同步副本列表 - ISR, important concepts in kafka

        // close admin object
        admin.close();
    }
}
