package com.lcz.study.quickStart;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumerQuickStart_1 {
    public static void main(String[] args) {
        // 创建KafkaConsumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g2");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅Topics
        consumer.subscribe(Pattern.compile("^topic.*"));
        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();

                    System.out.println(topic + "\t" + partition + "," + offset + "\t" + key + "," + value + "," + timestamp);
                }
            }
        }
    }
}
