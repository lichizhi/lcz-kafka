package com.lcz.study.transactions;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class KafkaProducerTransProducerAndConsumer {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = buildKafkaProducer();
        KafkaConsumer<String, String> consumer = buildKafkaConsumer();

        // 初始化事务
        producer.initTransactions();
        // 订阅
        consumer.subscribe(Collections.singletonList("topic01"));
        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();

                // 开启事务
                producer.beginTransaction();
                try {
                    // 迭代数据，进行业务处理
                    while (recordIterator.hasNext()) {
                        ConsumerRecord<String, String> record = recordIterator.next();
                        // 存储元数据
                        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                        // ====业务代码====
                        // 加工topic01中的消息，发送到下游
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic02", record.topic(), record.value() + " lcz study");
                        producer.send(producerRecord);
                    }

                    // 事务提交
                    // 提交消费者的偏移量
                    producer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata("g1"));
                    producer.commitTransaction();
                } catch (Exception e) {
                    System.err.println("transaction error: " + e.getMessage());
                    // 终止事务
                    producer.abortTransaction();
                }
            }
        }


    }

    private static KafkaProducer<String, String> buildKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置事务ID，必须唯一
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id-" + UUID.randomUUID().toString());

        // 配置批处理大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        // 配置等待时间（如果batch中的数据不足1024，则等待n时间）
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // 配置重试机制
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 默认是30秒
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        // 配置幂等
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> buildKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        // 配置消费者的消费事务隔离级别，默认是read_uncommitted
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // 必须关闭消费者的offset自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(props);
    }

}
