package com.lcz.study.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class KafkaProducerUser {
    public static void main(String[] args) {
        // 创建KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "lczA:9092,lczB:9092,lczC:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserDefineSerializer.class.getName());

        KafkaProducer<String, User> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, User> record =
                    new ProducerRecord<>("topic02", "key_" + i, new User(i, "name_" + i, new Date()));
            producer.send(record);
        }

        // 关闭生产者
        producer.close();
    }
}
