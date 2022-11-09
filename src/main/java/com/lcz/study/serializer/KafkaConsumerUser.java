package com.lcz.study.serializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerUser {
    public static void main(String[] args) {
        // 创建KafkaConsumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDefineDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
        // 订阅Topics
        consumer.subscribe(Collections.singleton("topic02"));
        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, User> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                for (ConsumerRecord<String, User> record : consumerRecords) {
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    User value = record.value();
                    long timestamp = record.timestamp();

                    System.out.println(topic + "\t" + partition + "," + offset + "\t" + key + "," + value.toString() + "," + timestamp);
                }
            }
        }
    }
}
