package com.lcz.study.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class UserDefineSerializer implements Serializer<Object> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        return SerializationUtils.serialize((Serializable) o);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return SerializationUtils.serialize((Serializable) data);
    }

    @Override
    public void close() {

    }
}
