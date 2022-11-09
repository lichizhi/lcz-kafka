package com.lcz.study.interceptors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerInterceptors {
    public static void main(String[] args) {
        // 创建KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, DefineProducerInterceptor.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            // 不指定key，则采用轮询策略，否则采用hash策略
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic01", "key_" + i, "value_" + i);
            producer.send(record);
        }

        // 关闭生产者
        producer.close();
    }
}
