package com.lcz.study.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerTransOnly {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = buildKafkaProducer();

        // 初始化事务
        producer.initTransactions();

        try {
            // 开启事务
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                // 模拟异常
                if (i == 8) {
                    i = 10 / 0;
                }
                ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "transaction_" + i, "error data_" + i);
                producer.send(record);
                producer.flush();
            }
            // 事务提交
            producer.commitTransaction();
        } catch (Exception e) {
            System.out.println("transaction error: " + e.getMessage());
            // 终止事务
            producer.abortTransaction();
        } finally {
            producer.close();
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

        // 配置重试
        props.put(ProducerConfig.RETRIES_CONFIG, "all");
        // 默认是30秒
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        // 配置幂等
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaProducer<>(props);
    }

}
