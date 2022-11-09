package com.lcz.study.offsets;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerOffset04 {
    public static void main(String[] args) {
        // 创建KafkaConsumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g4");
        // default: latest  other: earliest  none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅Topics
        consumer.subscribe(Collections.singletonList("topic01"));
        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                // 偏移量元数据
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();

                    // 偏移量每次提交都要+1
                    offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(++offset));
                    // 异步提交
                    consumer.commitAsync(offsets, (map, e) -> System.out.println("commit success: " + map + ", error: " + e.getMessage()));

                    System.out.println(topic + "\t" + partition + "," + offset + "\t" + key + "," + value + "," + timestamp);
                }
            }
        }
    }
}
