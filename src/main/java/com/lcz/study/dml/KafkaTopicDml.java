package com.lcz.study.dml;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaTopicDml {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 创建KafkaAdminClient
        Properties props = new Properties();
        // 必须在运行环境中将三个主机名和IP映射在操作系统中
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");

        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);

        // 创建Topic
        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic("topic01", 3, (short) 3));
//        newTopics.add(new NewTopic("topic03", 3, (short) 3));
        // 异步创建
//        adminClient.createTopics(newTopics);

        // 同步创建
        CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
        createTopicsResult.all().get();

        // 删除Topic
        // 异步删除
//        adminClient.deleteTopics(Arrays.asList("topic02", "topic03"));

        // 同步删除
//        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("topic01", "topic02"));
//        deleteTopicsResult.all().get();

        // 查看Topic列表
        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> names = topicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }

        // 查看Topic详细信息
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList("topic01"));
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.allTopicNames().get();
        topicDescriptionMap.forEach((name, topicDescription) -> System.out.println(topicDescription.toString()));

        // 关闭AdminClient
        adminClient.close();
    }

}
