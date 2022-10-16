package com.dong;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class KafkaEncoder implements SerializationSchema<JSONObject> {
    private final String encoding = "UTF8";

    @Override
    public byte[] serialize(JSONObject jsonObject) {
        return jsonObject.toJSONBBytes();
    }
}
