package com.lcz.study.idempotence;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerIdempotence {
    public static void main(String[] args) {
        // 创建KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 模拟超时重发，重发3次还是失败的话，就放弃发送
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 超时时间1ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
        // 开启幂等性
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 如果有1个发送不成功，就阻塞  默认是5  小于等于就行
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "idempotence", "test idempotence");
        producer.send(record);
        producer.flush();

        // 关闭生产者
        producer.close();
    }
}
