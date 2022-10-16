package com.dong;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.IOException;

public class KafkaDecoder implements DeserializationSchema<JSONObject> {
    private final String encoding = "UTF8";


    @Override
    public JSONObject deserialize(byte[] bytes) throws IOException {
        return JSON.parseObject(bytes);
    }

    @Override
    public boolean isEndOfStream(JSONObject jsonObject) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}
