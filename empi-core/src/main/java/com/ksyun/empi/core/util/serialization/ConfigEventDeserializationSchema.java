package com.ksyun.empi.core.util.serialization;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class ConfigEventDeserializationSchema implements KeyedDeserializationSchema<JSONObject> {

    public static Logger logger = LoggerFactory.getLogger(ConfigEventDeserializationSchema.class);

    @Override
    public JSONObject deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {

        JSONObject jsonObject;

        try {
            jsonObject = JSON.parseObject(new String(message), new TypeReference<JSONObject>() {});

        } catch (Exception e) {
            logger.error("Config {} did not have valid JSON content.", new String(message));
            return null;
        }

        return jsonObject;
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(new TypeHint<JSONObject>() {});
    }
}
